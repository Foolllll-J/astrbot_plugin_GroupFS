import os
import asyncio
import time
import subprocess
import aiohttp
import chardet
from typing import List, Dict, Optional
from astrbot.api import logger
from astrbot.api.star import StarTools
from aiocqhttp.exceptions import ActionFailed
from . import utils

async def get_all_files_with_path(group_id: int, bot) -> List[Dict]:
    """递归获取所有文件，并计算其在备份目录中的相对路径。"""
    all_files = []
    # 结构: (folder_id, folder_name, relative_path)
    folders_to_scan = [(None, "根目录", "")] 
    while folders_to_scan:
        current_folder_id, current_folder_name, current_relative_path = folders_to_scan.pop(0)
        
        try:
            if current_folder_id is None or current_folder_id == '/':
                result = await bot.api.call_action('get_group_root_files', group_id=group_id, file_count=2000)
            else:
                result = await bot.api.call_action('get_group_files_by_folder', group_id=group_id, folder_id=current_folder_id, file_count=2000)
            
            if not result: continue
            
            if result.get('files'):
                for file_info in result['files']:
                    file_info['relative_path'] = os.path.join(current_relative_path, file_info.get('file_name', ''))
                    file_info['size'] = file_info.get('size', 0) # 确保有 size 字段
                    all_files.append(file_info)
                    
            if result.get('folders'):
                for folder in result['folders']:
                    if folder_id := folder.get('folder_id'):
                        new_relative_path = os.path.join(current_relative_path, folder.get('folder_name', ''))
                        folders_to_scan.append((folder_id, folder.get('folder_name', ''), new_relative_path))
                        
        except Exception as e:
            logger.error(f"[{group_id}-群文件遍历] 递归获取文件夹 '{current_folder_name}' 内容时出错: {e}")
            continue
    return all_files

async def get_all_files_recursive_core(group_id: int, bot) -> List[Dict]:
    """
    递归获取所有文件，并补充父文件夹名称。
    兼容 /cdf, /cf, /sf, /df 等指令。
    """
    all_files_with_path = await get_all_files_with_path(group_id, bot)
    for file_info in all_files_with_path:
        path_parts = file_info.get('relative_path', '').split(os.path.sep)
        file_info['parent_folder_name'] = os.path.sep.join(path_parts[:-1]) if len(path_parts) > 1 else '根目录'
    return all_files_with_path

async def download_and_save_file(group_id: int, file_id: str, file_name: str, file_size: int, relative_path: str, root_dir: str, bot, semaphore: asyncio.Semaphore) -> bool:
    log_prefix = f"[群文件备份-{group_id}-下载]"
    target_path = os.path.join(root_dir, relative_path)
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    if os.path.exists(target_path):
        try:
            existing_size = os.path.getsize(target_path)
            if existing_size == file_size:
                logger.info(f"{log_prefix} 文件 '{file_name}' 已存在 ({utils.format_bytes(file_size)})，跳过下载。")
                return True
            else:
                logger.warning(f"{log_prefix} 文件 '{file_name}' 存在但大小不匹配 ({utils.format_bytes(existing_size)} != {utils.format_bytes(file_size)})，重新下载。")
                os.remove(target_path)
        except Exception as e:
            logger.warning(f"{log_prefix} 检查文件 '{file_name}' 大小失败 ({e})，尝试重新下载。")
            try:
                os.remove(target_path)
            except:
                pass

    try:
        url_result = await bot.api.call_action('get_group_file_url', group_id=group_id, file_id=file_id)
        if not (url_result and url_result.get('url')):
            logger.error(f"{log_prefix} 无法获取文件 '{file_name}' 的下载链接或文件已失效。")
            return False
        url = url_result['url']

        async with aiohttp.ClientSession() as session:
            async with semaphore:
                async with session.get(url, timeout=60) as resp:
                    if resp.status != 200:
                        logger.error(f"{log_prefix} 下载文件 '{file_name}' 失败 (HTTP: {resp.status})。")
                        return False
                    
                    with open(target_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            f.write(chunk)
        
        logger.info(f"{log_prefix} 成功下载文件 '{file_name}' ({utils.format_bytes(file_size)}) 到: {target_path}")
        return True
    except ActionFailed as e:
        logger.warning(f"{log_prefix} 下载文件 '{file_name}' 失败 (API错误): {e}")
        return False
    except OSError as e:
        logger.error(f"{log_prefix} 写入文件 '{file_name}' 时发生 OS 错误: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"{log_prefix} 下载文件 '{file_name}' 时发生未知异常: {e}", exc_info=True)
        return False

async def create_zip_archive(source_dir: str, target_zip_path: str, password: str) -> bool:
    """使用外部命令行工具 (7za) 压缩整个目录。"""
    VOLUME_SIZE = '512m' 
    try:
        dir_to_zip = os.path.basename(source_dir)
        parent_dir = os.path.dirname(source_dir)
        command = ['7za', 'a', '-tzip', target_zip_path, dir_to_zip, '-r', f'-v{VOLUME_SIZE}']
        
        if password:
            command.append(f"-p{password}")
        
        logger.info(f"[群文件备份-压缩] 正在执行压缩命令: {' '.join(command)}")
        
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=parent_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_message = stderr.decode('utf-8', errors='ignore')
            logger.error(f"[群文件备份-压缩] 打包失败，返回码 {process.returncode}: {error_message}")
            return False
        
        logger.info(f"[群文件备份-压缩] 打包成功: {target_zip_path}")
        return True
    except FileNotFoundError:
        logger.error("[群文件备份-压缩] 压缩失败：容器内未找到 7za 命令。")
        return False
    except Exception as e:
        logger.error(f"[群文件备份-压缩] 打包时发生未知错误: {e}", exc_info=True)
        return False

async def cleanup_folder(path: str):
    """异步清理文件夹及其内容。"""
    await asyncio.sleep(5)
    try:
        if os.path.exists(path):
            for dirpath, dirnames, filenames in os.walk(path, topdown=False):
                for filename in filenames:
                    os.remove(os.path.join(dirpath, filename))
                for dirname in dirnames:
                    os.rmdir(os.path.join(dirpath, dirname))
            os.rmdir(path)
            logger.info(f"已清理临时文件夹: {path}")
    except OSError as e:
        logger.warning(f"删除临时文件夹 {path} 失败: {e}")

async def cleanup_backup_temp(backup_dir: str, zip_path: Optional[str]):
    """异步清理备份目录和生成的 ZIP 文件。"""
    try:
        await asyncio.sleep(600)  # 等待10分钟后再清理
        if os.path.exists(backup_dir):
            for dirpath, dirnames, filenames in os.walk(backup_dir, topdown=False):
                for filename in filenames:
                    os.remove(os.path.join(dirpath, filename))
                for dirname in dirnames:
                    os.rmdir(os.path.join(dirpath, dirname))
            os.rmdir(backup_dir)
            logger.info(f"[群文件备份-清理] 已清理临时目录: {backup_dir}")
        
        await asyncio.sleep(5)

        if zip_path and os.path.exists(os.path.dirname(zip_path)):
            zip_base_name_no_ext = os.path.basename(zip_path).rsplit('.zip', 1)[0]
            temp_base_dir = os.path.dirname(zip_path)
            
            for f in os.listdir(temp_base_dir):
                if f.startswith(zip_base_name_no_ext):
                     file_to_delete = os.path.join(temp_base_dir, f)
                     os.remove(file_to_delete)
                     logger.info(f"[群文件备份-清理] 已清理生成的压缩包/分卷: {f}")
    except OSError as e:
        logger.warning(f"[群文件备份-清理] 删除临时文件或目录失败: {e}")
