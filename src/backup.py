import os
import asyncio
import time
from datetime import datetime
from typing import Optional, List, Dict
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
import astrbot.api.message_components as Comp
from astrbot.api.star import StarTools
from aiocqhttp.exceptions import ActionFailed
from . import utils
from .file_ops import get_all_files_with_path, download_and_save_file, create_zip_archive, cleanup_backup_temp

async def upload_and_send_file_via_api(event: AstrMessageEvent, bot, file_path: str, file_name: str) -> bool:
    """上传并发送文件。"""
    log_prefix = f"[群文件备份-上传/发送]"
    client = bot or event.bot
    target_id = int(event.get_sender_id())
    group_id_str = event.get_group_id() 
    file_uri = f"file://{file_path}"
    
    upload_result = None # 初始化变量
    
    try:
        # 1. API Call
        if group_id_str:
            target_group_id = int(group_id_str)
            logger.info(f"{log_prefix} 调用 /upload_group_file 上传文件到群 {target_group_id}")
            upload_result = await client.api.call_action('upload_group_file', 
                                                         group_id=target_group_id,
                                                         file=file_uri,
                                                         name=file_name,
                                                         folder_id='/',
                                                         timeout=300)
            
        else:
            logger.info(f"{log_prefix} 调用 /upload_private_file 上传文件到私聊 {target_id}")
            upload_result = await client.api.call_action('upload_private_file', 
                                                         user_id=target_id,
                                                         file=file_uri,
                                                         name=file_name,
                                                         timeout=300)
        
        # 2. 检查 upload_result 是否为 None
        if upload_result is None:
             logger.warning(f"{log_prefix} 文件 {file_name} 上传时 API 调用返回 NONE。根据测试经验，文件可能已在后台提交。")
             return True # 视为成功并继续下一个分卷
        
        # 3. 检查 API 响应状态：status='ok' 且 retcode=0 (正常成功)
        if upload_result.get('status') == 'ok' and upload_result.get('retcode') == 0:
            logger.info(f"{log_prefix} 文件 {file_name} 上传调用成功。")
            return True
        
        # 4. 处理 API 明确返回失败状态
        else:
            error_msg = upload_result.get('wording', upload_result.get('errMsg', 'API返回失败'))
            retcode = upload_result.get('retcode')
            
            # 如果返回的错误是 NTQQ 的 "rich media transfer failed" (retcode=1200)
            if retcode == 1200:
                logger.error(f"{log_prefix} 文件 {file_name} 上传失败 (NTQQ内部拒绝: {error_msg})。视为致命失败，中断任务。")
                return False
            else:
                if retcode is None:
                    logger.info(f"{log_prefix} 文件 {file_name} 上传调用返回异常状态 (retcode=None)，但根据经验文件可能已提交成功，继续执行。")
                else:
                    logger.warning(f"{log_prefix} 文件 {file_name} 上传返回非标准状态码 (retcode={retcode})。详情: {error_msg}。容忍并继续。")
                return True

    except ActionFailed as e:
        logger.warning(f"{log_prefix} 文件 {file_name} 上传时发生 ActionFailed (网络中断/超时)。错误: {e}")
        return False
        
    except Exception as e:
        error_type = type(e).__name__
        logger.warning(f"{log_prefix} 上传文件 {file_name} 时发生 Python 致命错误 ({error_type})。根据测试经验，文件可能已提交。错误: {e}", exc_info=True)
        return False

async def perform_group_file_backup(
    event: AstrMessageEvent, 
    group_id: int, 
    bot, 
    download_semaphore: asyncio.Semaphore, 
    backup_file_size_limit_mb: int,
    backup_file_extensions: List[str],
    backup_zip_password: str,
    date_filter_timestamp: Optional[int] = None
):
    """执行群文件备份任务。"""
    log_prefix = f"[群文件备份-{group_id}]"
    backup_root_dir = None
    final_zip_path = None
    
    try:
        client = bot or event.bot
        
        # 1. 预通知：获取群文件系统信息
        logger.info(f"{log_prefix} 正在获取群文件系统原始信息...")
        system_info = await client.api.call_action('get_group_file_system_info', group_id=group_id)
        
        total_count = system_info.get('file_count', '未知')
        
        notification = (
            f"备份任务已启动，目标群ID: {group_id}。\n"
            f"该群文件总数: {total_count}。"
        )
        if date_filter_timestamp:
            date_str = datetime.fromtimestamp(date_filter_timestamp).strftime('%Y-%m-%d')
            notification += f"\n将仅备份 {date_str} 之后上传的文件。"
        notification += "\n备份操作将遍历所有文件，请耐心等待，这可能需要几分钟。"
        
        await event.send(MessageChain([Comp.Plain(notification)]))
        logger.info(f"{log_prefix} 预通知已发送。")

        # 2. 准备工作：获取群名、创建本地临时目录
        group_info = await client.api.call_action('get_group_info', group_id=group_id)
        group_name = group_info.get('group_name', str(group_id))
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        
        temp_plugin_dir = StarTools.get_data_dir('astrbot_plugin_GroupFS')
        temp_base_dir = os.path.join(temp_plugin_dir, 'temp_backup_cache') 
        
        # 实际存放文件和最终 zip 的目录
        backup_root_dir = os.path.join(temp_base_dir, f"{group_name}") 
        
        # 创建目录
        os.makedirs(backup_root_dir, exist_ok=True)
        
        # 3. 递归获取所有文件信息
        logger.info(f"{log_prefix} 正在获取全量文件列表...")
        all_files_info = await get_all_files_with_path(group_id, client)
        
        # 4. 过滤和下载文件
        downloaded_files_count = 0
        downloaded_files_size = 0
        failed_downloads = []
        
        size_limit_bytes = backup_file_size_limit_mb * 1024 * 1024
        
        for i, file_info in enumerate(all_files_info):
            file_name = file_info.get('file_name', '未知文件')
            file_id = file_info.get('file_id')
            file_size = file_info.get('size', 0)
            file_modify_time = file_info.get('modify_time', 0)
            relative_path = file_info.get('relative_path', '')
            
            # 4.1. 过滤：日期筛选
            if date_filter_timestamp and file_modify_time < date_filter_timestamp:
                continue

            # 4.2. 过滤：大小和后缀名
            if size_limit_bytes > 0 and file_size > size_limit_bytes:
                continue
            
            _, ext = os.path.splitext(file_name)
            ext = ext[1:].lower()
            if backup_file_extensions and ext not in backup_file_extensions:
                continue
            
            # 4.3. 下载
            download_success = await download_and_save_file(
                group_id, file_id, file_name, file_size, relative_path, backup_root_dir, client, download_semaphore
            )
            
            if download_success:
                downloaded_files_count += 1
                downloaded_files_size += file_size
            else:
                failed_downloads.append(file_name)

        # 5. 压缩整个目录
        final_zip_name = f"{group_name}_备份_{timestamp}.zip"
        final_zip_path = os.path.join(temp_base_dir, final_zip_name)
        
        logger.info(f"{log_prefix} 文件下载完成，共成功下载 {downloaded_files_count} 个文件，开始压缩...")

        zip_success = False

        if downloaded_files_count > 0:
             zip_success = await create_zip_archive(backup_root_dir, final_zip_path, backup_zip_password)
        else:
            logger.warning(f"{log_prefix} 没有符合条件的文件需要备份，跳过压缩。")
            
        # 6. 发送和清理
        if zip_success:
            # 基础名：不包含 .zip 部分
            zip_base_name_no_ext = os.path.basename(final_zip_path).rsplit('.zip', 1)[0]
            
            all_volumes = []
            
            # 查找所有分卷
            for f in os.listdir(temp_base_dir):
                f_path = os.path.join(temp_base_dir, f)
                if f.startswith(zip_base_name_no_ext) and (f.endswith('.zip') or f.split('.')[-1].isdigit()):
                     if f == os.path.basename(final_zip_path) or f.startswith(f"{zip_base_name_no_ext}.zip."):
                        all_volumes.append(f_path)

            all_volumes.sort() # 确保按顺序发送
            
            is_single_volume = len(all_volumes) == 1
            
            if is_single_volume:
                original_path = all_volumes[0]
                new_volume_name = f"{zip_base_name_no_ext}.zip"
                new_volume_path = os.path.join(temp_base_dir, new_volume_name)
                if original_path != new_volume_path:
                    os.rename(original_path, new_volume_path)
                    all_volumes = [new_volume_path]

            if not all_volumes:
                await event.send(MessageChain([Comp.Plain(f"❌ 备份压缩成功，但未在目录中找到任何生成的压缩文件！")]))
            else:
                reply_message = (
                    f"✅ 群文件备份完成！\n"
                    f"成功备份文件数: {downloaded_files_count} 个 (总大小: {utils.format_bytes(downloaded_files_size)})\n"
                    f"{'共' if len(all_volumes) > 1 else ''} {len(all_volumes)} 个文件即将发送，请注意接收！"
                )
                if failed_downloads:
                    reply_message += f"\n⚠️ 备份失败文件数: {len(failed_downloads)} 个"
                
                await event.send(MessageChain([Comp.Plain(reply_message)]))

                # 逐个发送分卷文件
                all_sent_success = True
                for volume_path in all_volumes:
                    volume_name = os.path.basename(volume_path)
                    if not await upload_and_send_file_via_api(event, client, volume_path, volume_name):
                        all_sent_success = False
                        await event.send(MessageChain([Comp.Plain(f"❌ 文件 {volume_name} 发送失败。")]))
                        break
                        
                if not all_sent_success:
                    await event.send(MessageChain([Comp.Plain(f"❌ 备份发送中断。")]))
                    
        elif downloaded_files_count == 0:
             await event.send(MessageChain([Comp.Plain(f"ℹ️ 备份任务完成。但没有找到符合大小或后缀名限制的任何文件。")]))
        else:
            await event.send(MessageChain([Comp.Plain(f"❌ 备份任务失败：压缩文件失败或找不到压缩包。")]))
            
    except Exception as e:
        logger.error(f"{log_prefix} 备份任务执行过程中发生未知异常: {e}", exc_info=True)
        await event.send(MessageChain([Comp.Plain(f"❌ 备份任务执行失败，发生内部错误。")]))
    finally:
        asyncio.create_task(cleanup_backup_temp(backup_root_dir, final_zip_path))
