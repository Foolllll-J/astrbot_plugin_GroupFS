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

async def get_preview_from_bytes(content_bytes: bytes) -> tuple[str, str]:
    """从字节内容中尝试获取文本预览和编码。"""
    try:
        detection = chardet.detect(content_bytes)
        encoding = detection.get('encoding', 'utf-8') or 'utf-8'
        if encoding and detection['confidence'] > 0.7:
            decoded_text = content_bytes.decode(encoding, errors='ignore').strip()
            return decoded_text, encoding
        return "", "未知"
    except Exception:
        return "", "未知"

async def get_preview_from_zip(file_path: str, default_zip_password: str, preview_length: int, cleanup_fn) -> tuple[str, str]:
    """从本地压缩文件中解压并预览第一个文本文件。返回 (预览内容, 错误信息)。"""
    temp_dir = os.path.join(StarTools.get_data_dir('astrbot_plugin_GroupFS'), 'temp_file_previews')
    os.makedirs(temp_dir, exist_ok=True)
    extract_path = os.path.join(temp_dir, f"extract_{int(time.time())}")
    os.makedirs(extract_path, exist_ok=True)
    
    preview_text = ""
    error_msg = None
    
    try:
        logger.info(f"正在尝试无密码解压文件 '{os.path.basename(file_path)}'...")
        command_no_pwd = ["7za", "x", file_path, f"-o{extract_path}", "-y"]
        process = await asyncio.create_subprocess_exec(
            *command_no_pwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            if default_zip_password:
                logger.info("无密码解压失败，正在尝试使用默认密码...")
                command_with_pwd = ["7za", "x", file_path, f"-o{extract_path}", f"-p{default_zip_password}", "-y"]
                process = await asyncio.create_subprocess_exec(
                    *command_with_pwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    error_msg = stderr.decode('utf-8').strip()
                    logger.error(f"使用默认密码解压失败: {error_msg}")
                    error_msg = "解压失败，可能密码不正确"
            else:
                error_msg = stderr.decode('utf-8').strip()
                logger.error(f"使用 7za 命令解压失败且未设置默认密码: {error_msg}")
                error_msg = "解压失败，可能文件已加密"
        
        if error_msg:
            return "", error_msg

        all_extracted_files = [os.path.join(dirpath, f) for dirpath, _, filenames in os.walk(extract_path) for f in filenames]
        preview_file_path = None
        
        for f_path in all_extracted_files:
            if f_path.lower().endswith('.txt'):
                preview_file_path = f_path
                break
        
        if not preview_file_path:
            if not all_extracted_files:
                return "", "压缩包为空或解压失败"
            
            file_structure = ["📦 压缩包内文件结构："]
            for f_path in sorted(all_extracted_files):
                relative_path = os.path.relpath(f_path, extract_path)
                file_size = os.path.getsize(f_path)
                size_str = utils.format_bytes(file_size)
                depth = relative_path.count(os.sep)
                indent = "  " * depth
                file_name = os.path.basename(relative_path)
                file_structure.append(f"{indent}├─ {file_name} ({size_str})")
            
            structure_text = "\n".join(file_structure)
            return structure_text, None
        
        with open(preview_file_path, 'rb') as f:
            content_bytes = f.read(preview_length * 4)
        
        preview_text_raw, encoding = await get_preview_from_bytes(content_bytes)
        
        inner_file_name = os.path.relpath(preview_file_path, extract_path)
        extra_info = f"已解压「{inner_file_name}」(格式 {encoding})"
        preview_text = f"{extra_info}\n{preview_text_raw}"
        
    except FileNotFoundError:
        logger.error("解压失败：容器内未找到 7za 命令。")
        error_msg = "解压失败：未安装 7za"
    except Exception as e:
        logger.error(f"处理ZIP文件时发生未知错误: {e}", exc_info=True)
        error_msg = "处理压缩文件时发生内部错误"
    finally:
        if os.path.exists(extract_path):
            asyncio.create_task(cleanup_fn(extract_path))
    
    return preview_text, error_msg

async def get_file_preview(group_id: int, file_info: dict, bot, enable_zip_preview: bool, default_zip_password: str, preview_length: int, semaphore: asyncio.Semaphore, cleanup_fn) -> tuple[str, str | None]:
    file_id = file_info.get("file_id")
    file_name = file_info.get("file_name", "")
    _, file_extension = os.path.splitext(file_name)
    
    is_txt = file_extension.lower() == '.txt'
    is_zip = enable_zip_preview and file_extension.lower() == '.zip'
    
    if not (is_txt or is_zip):
        return "", f"❌ 文件「{file_name}」不是支持的文本或压缩格式，无法预览。"
        
    logger.info(f"[{group_id}] 正在为文件 '{file_name}' (ID: {file_id}) 获取预览...")
    
    local_file_path = None
    
    try:
        url_result = await bot.api.call_action('get_group_file_url', group_id=group_id, file_id=file_id)
        if not (url_result and url_result.get('url')):
            return "", f"❌ 无法获取文件「{file_name}」的下载链接。"
        url = url_result['url']
    except ActionFailed as e:
        if e.result.get('retcode') == 1200:
            error_message = (
                f"❌ 预览文件「{file_name}」失败：\n"
                f"该文件可能已失效。\n"
                f"建议使用 /df {os.path.splitext(file_name)[0]} 将其删除。"
            )
            return "", error_message
        else:
            return "", f"❌ 预览失败，API返回错误：{e.result.get('wording', '未知错误')}"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with semaphore:
                range_header = None
                if is_txt:
                    read_bytes_limit = preview_length * 4
                    range_header = {'Range': f'bytes=0-{read_bytes_limit - 1}'}
                async with session.get(url, headers=range_header, timeout=30) as resp:
                    if resp.status != 200 and resp.status != 206:
                        return "", f"❌ 下载文件「{file_name}」失败 (HTTP: {resp.status})。"
                    
                    temp_dir = os.path.join(StarTools.get_data_dir('astrbot_plugin_GroupFS'), 'temp_file_previews')
                    os.makedirs(temp_dir, exist_ok=True)
                    local_file_path = os.path.join(temp_dir, f"{file_id}_{file_name}")
                    
                    content_bytes = await resp.read()
                    with open(local_file_path, 'wb') as f:
                        f.write(content_bytes)
        
        preview_content = ""
        error_msg = None
        if is_txt:
            decoded_text, _ = await get_preview_from_bytes(content_bytes)
            preview_content = decoded_text
        elif is_zip:
            preview_text, error_msg = await get_preview_from_zip(local_file_path, default_zip_password, preview_length, cleanup_fn)
            if error_msg:
                return "", error_msg
            preview_content = preview_text
        
        is_file_structure = preview_content.startswith("📦 压缩包内文件结构：")
        if not is_file_structure and len(preview_content) > preview_length:
            preview_content = preview_content[:preview_length] + "..."
        
        logger.info(f"[文件预览] 文件: {file_name}, 预览长度: {len(preview_content)}, 是否文件结构: {is_file_structure}")
        
        return preview_content, None
            
    except asyncio.TimeoutError:
        return "", f"❌ 预览文件「{file_name}」超时。"
    except Exception as e:
        logger.error(f"[{group_id}] 获取文件 '{file_name}' 预览时发生未知异常: {e}", exc_info=True)
        return "", f"❌ 预览文件「{file_name}」时发生内部错误。"
    finally:
        if local_file_path and os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                logger.info(f"已清理临时文件: {local_file_path}")
            except OSError as e:
                logger.warning(f"删除临时文件 {local_file_path} 失败: {e}")
