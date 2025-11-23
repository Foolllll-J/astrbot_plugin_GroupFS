# astrbot_plugin_GroupFS/main.py

# 请确保已安装依赖: pip install croniter aiohttp chardet apscheduler
import asyncio
import os
import time
from datetime import datetime
from typing import List, Dict, Optional
import chardet
import subprocess
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import aiohttp
import croniter

from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger
import astrbot.api.message_components as Comp
from astrbot.api.message_components import Plain, Node, Nodes
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
from aiocqhttp.exceptions import ActionFailed 

from . import utils

@register(
    "astrbot_plugin_GroupFS",
    "Foolllll",
    "管理QQ群文件",
    "0.9",
    "https://github.com/Foolllll-J/astrbot_plugin_GroupFS"
)
class GroupFSPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config if config else {}
        self.group_whitelist: List[int] = [int(g) for g in self.config.get("group_whitelist", [])]
        self.admin_users: List[int] = [int(u) for u in self.config.get("admin_users", [])]
        self.preview_length: int = self.config.get("preview_length", 300)
        self.storage_limits: Dict[int, Dict] = {}
        self.cron_configs = []
        self.bot = None
        self.forward_threshold: int = self.config.get("forward_threshold", 0)
        self.scheduler: Optional[AsyncIOScheduler] = None
        
        self.active_tasks = [] 
        
        self.enable_zip_preview: bool = self.config.get("enable_zip_preview", False)
        self.default_zip_password: str = self.config.get("default_zip_password", "")
        self.download_semaphore = asyncio.Semaphore(5)
        
        self.scheduled_autodelete: bool = self.config.get("scheduled_autodelete", False)

        limit_configs = self.config.get("storage_limits", [])
        for item in limit_configs:
            try:
                group_id_str, count_limit_str, space_limit_str = item.split(':')
                group_id = int(group_id_str)
                self.storage_limits[group_id] = { "count_limit": int(count_limit_str), "space_limit_gb": float(space_limit_str) }
            except ValueError as e:
                logger.error(f"解析 storage_limits 配置 '{item}' 时出错: {e}，已跳过。")
        
        self.backup_zip_password: str = self.config.get("backup_zip_password", "")
        self.backup_file_size_limit_mb: int = self.config.get("backup_file_size_limit_mb", 0)
        ext_str: str = self.config.get("backup_file_extensions", "txt,zip")
        
        self.backup_file_extensions: List[str] = [
            ext.strip().lstrip('.').lower()
            for ext in ext_str.split(',') 
            if ext.strip()
        ]

        cron_configs = self.config.get("scheduled_check_tasks", [])
        seen_tasks = set()
        for item in cron_configs:
            try:
                group_id_str, cron_str = item.split(':', 1)
                group_id = int(group_id_str)
                if not croniter.croniter.is_valid(cron_str):
                    raise ValueError(f"无效的 cron 表达式: {cron_str}")
                
                task_identifier = (group_id, cron_str)
                if task_identifier in seen_tasks:
                    logger.warning(f"检测到重复的定时任务配置 '{item}'，已跳过。")
                    continue
                
                self.cron_configs.append({"group_id": group_id, "cron_str": cron_str})
                seen_tasks.add(task_identifier)
            except ValueError as e:
                logger.error(f"解析 scheduled_check_tasks 配置 '{item}' 时出错: {e}，已跳过。")
        
        logger.info("插件 [群文件系统GroupFS] 已加载。")

    async def initialize(self):
        if self.cron_configs:
            logger.info("[定时任务] 启动失效文件检查调度器...")
            self.scheduler = AsyncIOScheduler()
            self._register_jobs()
            self.scheduler.start()

    def _register_jobs(self):
        """根据配置注册定时任务"""
        for job_config in self.cron_configs:
            group_id = job_config["group_id"]
            cron_str = job_config["cron_str"]
            job_id = f"scheduled_check_{group_id}_{cron_str.replace(' ', '_')}"
            
            if self.scheduler.get_job(job_id):
                logger.warning(f"任务 {job_id} 已存在，跳过注册。")
                continue
            
            try:
                cron_parts = cron_str.split()
                minute, hour, day, month, day_of_week = cron_parts
                
                self.scheduler.add_job(
                    self._perform_scheduled_check,
                    "cron",
                    args=[group_id, self.scheduled_autodelete],
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                    id=job_id
                )
                logger.info(f"成功注册定时任务: group_id={group_id}, cron_str='{cron_str}'")
            except Exception as e:
                logger.error(f"注册定时任务 '{cron_str}' 失败: {e}", exc_info=True)

    def _split_text_by_length(self, text: str, max_length: int = 1000) -> List[str]:
            """
            将文本按指定长度分割成一个字符串列表。
            """
            return [text[i:i + max_length] for i in range(0, len(text), max_length)]

    async def _send_or_forward(self, event: AstrMessageEvent, text: str, name: str = "GroupFS"):
        total_length = len(text)
        group_id = event.get_group_id()

        if self.forward_threshold > 0 and total_length > self.forward_threshold:
            logger.info(f"[{group_id}] 检测到长消息 (长度: {total_length} > {self.forward_threshold})，准备自动合并转发。")
            try:
                split_texts = self._split_text_by_length(text, 4000)
                forward_nodes = []
                
                logger.info(f"[{group_id}] 将消息分割为 {len(split_texts)} 个节点。")
                for i, part_text in enumerate(split_texts):
                    node_name = f"{name} ({i+1})" if len(split_texts) > 1 else name
                    forward_nodes.append(Node(uin=event.get_self_id(), name=node_name, content=[Plain(part_text)]))

                merged_forward_message = Nodes(nodes=forward_nodes)
                await event.send(MessageChain([merged_forward_message]))
                logger.info(f"[{group_id}] 成功发送合并转发消息。")

            except Exception as e:
                logger.error(f"[{group_id}] 合并转发长消息时出错: {e}", exc_info=True)
                
                fallback_text = text[:self.forward_threshold] + "... (消息过长且合并转发失败)"
                await event.send(MessageChain([Comp.Plain(fallback_text)]))
                logger.info(f"[{group_id}] 合并转发失败，已回退为发送截断的普通消息。")
        else:
            logger.info(f"[{group_id}] 消息长度未达阈值 ({total_length} <= {self.forward_threshold})，直接发送普通消息。")
            try:
                await event.send(MessageChain([Comp.Plain(text)]))
                logger.info(f"[{group_id}] 成功发送普通消息。")
            except Exception as e:
                logger.error(f"[{group_id}] 发送普通消息时出错: {e}", exc_info=True)

    async def _perform_scheduled_check(self, group_id: int, auto_delete: bool):
        """统一的定时检查函数，根据auto_delete决定是否删除。"""
        log_prefix = "[清理任务]" if auto_delete else "[检查任务]"
        report_title = "清理报告" if auto_delete else "检查报告"
        
        try:
            if not self.bot:
                logger.warning(f"[{group_id}] {log_prefix} 无法执行，因为尚未捕获到 bot 实例。请先触发任意一次指令。")
                return
            bot = self.bot
            logger.info(f"[{group_id}] {log_prefix} 开始获取全量文件列表...")
            all_files = await self._get_all_files_recursive_core(group_id, bot)
            total_count = len(all_files)
            logger.info(f"[{group_id}] {log_prefix} 获取到 {total_count} 个文件，准备分批检查。")
            invalid_files_info = []
            deleted_files = []
            failed_deletions = []
            
            batch_size = 50
            for i in range(0, total_count, batch_size):
                batch = all_files[i:i + batch_size]
                for file_info in batch:
                    file_id = file_info.get("file_id")
                    file_name = file_info.get("file_name", "未知文件名")
                    if not file_id: continue
                    try:
                        await bot.api.call_action('get_group_file_url', group_id=group_id, file_id=file_id)
                    except ActionFailed as e:
                        if e.result.get('retcode') == 1200:
                            invalid_files_info.append(file_info)
                            if auto_delete:
                                logger.warning(f"[{group_id}] {log_prefix} 发现失效文件 '{file_name}'，尝试删除...")
                                try:
                                    delete_result = await bot.api.call_action('delete_group_file', group_id=group_id, file_id=file_id)
                                    is_success = False
                                    if delete_result and delete_result.get('transGroupFileResult', {}).get('result', {}).get('retCode') == 0:
                                        is_success = True
                                    if is_success:
                                        logger.info(f"[{group_id}] {log_prefix} 成功删除失效文件: '{file_name}'")
                                        deleted_files.append(file_name)
                                    else:
                                        logger.error(f"[{group_id}] {log_prefix} 删除失效文件 '{file_name}' 失败，API未返回成功。")
                                        failed_deletions.append(file_name)
                                except Exception as del_e:
                                    logger.error(f"[{group_id}] {log_prefix} 删除失效文件 '{file_name}' 时发生异常: {del_e}")
                                    failed_deletions.append(file_name)
                    await asyncio.sleep(0.2)
            
            if not invalid_files_info:
                logger.info(f"[{group_id}] {log_prefix} 检查完成，未发现失效文件。")
                return 

            report_message = f"🚨 {report_title}\n在 {total_count} 个群文件中，"
            report_message += f"共发现 {len(invalid_files_info)} 个失效文件。\n"
            
            if auto_delete:
                report_message += f"\n- 成功删除: {len(deleted_files)} 个"
                if failed_deletions:
                    report_message += f"\n- 删除失败: {len(failed_deletions)} 个"
                if not deleted_files and not failed_deletions:
                     report_message += f"但未成功删除任何文件。"
                
                report_message += "\n" + "-" * 20
                for info in invalid_files_info:
                    status = "已删除" if info.get('file_name') in deleted_files else "删除失败"
                    folder_name = info.get('parent_folder_name', '未知')
                    modify_time = utils.format_timestamp(info.get('modify_time'))
                    report_message += f"\n- {info.get('file_name')} ({status})"
                    report_message += f"\n  (文件夹: {folder_name} | 时间: {modify_time})"
            else:
                report_message += "\n" + "-" * 20
                for info in invalid_files_info:
                    folder_name = info.get('parent_folder_name', '未知')
                    modify_time = utils.format_timestamp(info.get('modify_time'))
                    report_message += f"\n- {info.get('file_name')}"
                    report_message += f"\n  (文件夹: {folder_name} | 时间: {modify_time})"
                report_message += "\n" + "-" * 20
                report_message += "\n建议管理员使用 /cdf 指令进行一键清理。"
            
            logger.info(f"[{group_id}] {log_prefix} 检查全部完成，准备发送报告。")
            if self.bot:
                await self.bot.api.call_action('send_group_msg', group_id=group_id, message=report_message)
        except Exception as e:
            logger.error(f"[{group_id}] {log_prefix} 执行过程中发生未知异常: {e}", exc_info=True)
            if self.bot:
                await self.bot.api.call_action('send_group_msg', group_id=group_id, message="❌ 定时任务执行过程中发生内部错误，请检查后台日志。")


    async def _get_all_files_with_path(self, group_id: int, bot) -> List[Dict]:
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
    
    async def _get_all_files_recursive_core(self, group_id: int, bot) -> List[Dict]:
        """
        兼容 /cdf, /cf, /sf, /df 等指令。
        """
        all_files_with_path = await self._get_all_files_with_path(group_id, bot)
        for file_info in all_files_with_path:
            path_parts = file_info.get('relative_path', '').split(os.path.sep)
            file_info['parent_folder_name'] = os.path.sep.join(path_parts[:-1]) if len(path_parts) > 1 else '根目录'
        return all_files_with_path
    
    async def _download_and_save_file(self, group_id: int, file_id: str, file_name: str, file_size: int, relative_path: str, root_dir: str, client) -> bool:
        log_prefix = f"[群文件备份-{group_id}-下载]"
        target_path = os.path.join(root_dir, relative_path)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        if os.path.exists(target_path):
            try:
                # 检查已存在文件的大小是否与目标文件大小匹配
                existing_size = os.path.getsize(target_path)
                if existing_size == file_size:
                    logger.info(f"{log_prefix} 文件 '{file_name}' 已存在 ({utils.format_bytes(file_size)})，跳过下载。")
                    return True
                else:
                    logger.warning(f"{log_prefix} 文件 '{file_name}' 存在但大小不匹配 ({utils.format_bytes(existing_size)} != {utils.format_bytes(file_size)})，重新下载。")
                    os.remove(target_path) # 大小不一致，先删除再下载
            except Exception as e:
                logger.warning(f"{log_prefix} 检查文件 '{file_name}' 大小失败 ({e})，尝试重新下载。")
                try:
                    os.remove(target_path)
                except:
                    pass

        try:
            # 1. 获取下载链接
            url_result = await client.api.call_action('get_group_file_url', group_id=group_id, file_id=file_id)
            if not (url_result and url_result.get('url')):
                logger.error(f"{log_prefix} 无法获取文件 '{file_name}' 的下载链接或文件已失效。")
                return False
            url = url_result['url']

            # 2. 下载文件，使用信号量控制并发
            async with aiohttp.ClientSession() as session:
                async with self.download_semaphore:
                    async with session.get(url, timeout=60) as resp:
                        if resp.status != 200:
                            logger.error(f"{log_prefix} 下载文件 '{file_name}' 失败 (HTTP: {resp.status})。")
                            return False
                        
                        # 3. 写入文件，注意捕获 OS 异常（如磁盘空间不足）
                        with open(target_path, 'wb') as f:
                            async for chunk in resp.content.iter_chunked(8192):
                                f.write(chunk)
            
            logger.info(f"{log_prefix} 成功下载文件 '{file_name}' ({utils.format_bytes(file_size)}) 到: {target_path}")
            return True
        except ActionFailed as e:
            # NapCat/NTQQ API 调用失败，如文件已失效(-134)等
            logger.warning(f"{log_prefix} 下载文件 '{file_name}' 失败 (API错误): {e.message if hasattr(e, 'message') else str(e)}")
            return False
        except FileNotFoundError:
            logger.error(f"{log_prefix} 创建目标文件路径失败 (FileNotFoundError)，可能目录创建失败。")
            return False
        except OSError as e:
            logger.error(f"{log_prefix} 写入文件 '{file_name}' 时发生 OS 错误 (可能空间不足): {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"{log_prefix} 下载文件 '{file_name}' 时发生未知异常: {e}", exc_info=True)
            return False

    async def _cleanup_backup_temp(self, backup_dir: str, zip_path: Optional[str]):
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

            # 删除生成的 ZIP 文件
            if zip_path and os.path.exists(os.path.dirname(zip_path)):
                zip_base_name_no_ext = os.path.basename(zip_path).rsplit('.zip', 1)[0]
                temp_base_dir = os.path.dirname(zip_path)
                
                # 遍历所有可能的分卷文件并删除
                for f in os.listdir(temp_base_dir):
                    if f.startswith(zip_base_name_no_ext):
                         file_to_delete = os.path.join(temp_base_dir, f)
                         os.remove(file_to_delete)
                         logger.info(f"[群文件备份-清理] 已清理生成的压缩包/分卷: {f}")

        except OSError as e:
            logger.warning(f"[群文件备份-清理] 删除临时文件或目录失败: {e}")

    async def _upload_and_send_file_via_api(self, event: AstrMessageEvent, file_path: str, file_name: str) -> bool:
        log_prefix = f"[群文件备份-上传/发送]"
        client = self.bot or event.client
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
                    # 客户端拒绝，返回 False
                    return False
                else:
                    # 其他非 1200 的失败码，或者 retcode 为 None
                    # 根据经验，retcode=None 通常表示请求已提交但响应异常，文件可能已在后台处理
                    if retcode is None:
                        logger.info(f"{log_prefix} 文件 {file_name} 上传调用返回异常状态 (retcode=None)，但根据经验文件可能已提交成功，继续执行。")
                    else:
                        logger.warning(f"{log_prefix} 文件 {file_name} 上传返回非标准状态码 (retcode={retcode})。详情: {error_msg}。容忍并继续。")
                    return True

        except ActionFailed as e:
            # 捕获 ActionFailed
            logger.warning(f"{log_prefix} 文件 {file_name} 上传时发生 ActionFailed (网络中断/超时)。错误: {e}")
            return False # 视为失败，中断任务
            
        except Exception as e:
            error_type = type(e).__name__
            logger.warning(f"{log_prefix} 上传文件 {file_name} 时发生 Python 致命错误 ({error_type})。根据测试经验，文件可能已提交。错误: {e}", exc_info=True)
            return False

    @filter.command("cdf")
    async def on_check_and_delete_command(self, event: AstrMessageEvent):
        if not self.bot: self.bot = event.bot
        group_id = int(event.get_group_id())
        user_id = int(event.get_sender_id())
        logger.info(f"[{group_id}] 用户 {user_id} 触发 /cdf 失效文件清理指令。")
        if user_id not in self.admin_users:
            await event.send(MessageChain([Comp.Plain("⚠️ 您没有执行此操作的权限。")]))
            return
        await event.send(MessageChain([Comp.Plain("⚠️ 警告：即将开始扫描并自动删除所有失效文件！\n此过程可能需要几分钟，请耐心等待，完成后将发送报告。")]))
        self.active_tasks.append(asyncio.create_task(self._perform_batch_check_and_delete(event)))
        event.stop_event()

    async def _perform_batch_check_and_delete(self, event: AstrMessageEvent):
        group_id = int(event.get_group_id())
        try:
            logger.info(f"[{group_id}] [批量清理] 开始获取全量文件列表...")
            all_files = await self._get_all_files_recursive_core(group_id, event.bot)
            total_count = len(all_files)
            logger.info(f"[{group_id}] [批量清理] 获取到 {total_count} 个文件，准备分批处理。")
            deleted_files = []
            failed_deletions = []
            checked_count = 0
            batch_size = 50
            for i in range(0, total_count, batch_size):
                batch = all_files[i:i + batch_size]
                logger.info(f"[{group_id}] [批量清理] 正在处理批次 {i//batch_size + 1}/{ -(-total_count // batch_size)}...")
                for file_info in batch:
                    file_id = file_info.get("file_id")
                    file_name = file_info.get("file_name", "未知文件名")
                    if not file_id: continue
                    is_invalid = False
                    try:
                        await event.bot.api.call_action('get_group_file_url', group_id=group_id, file_id=file_id)
                    except ActionFailed as e:
                        if e.result.get('retcode') == 1200:
                            is_invalid = True
                    if is_invalid:
                        logger.warning(f"[{group_id}] [批量清理] 发现失效文件 '{file_name}'，尝试删除...")
                        try:
                            delete_result = await event.bot.api.call_action('delete_group_file', group_id=group_id, file_id=file_id)
                            is_success = False
                            if delete_result:
                                trans_result = delete_result.get('transGroupFileResult', {})
                                result_obj = trans_result.get('result', {})
                                if result_obj.get('retCode') == 0:
                                    is_success = True
                            if is_success:
                                logger.info(f"[{group_id}] [批量清理] 成功删除失效文件: '{file_name}'")
                                deleted_files.append(file_name)
                            else:
                                logger.error(f"[{group_id}] [批量清理] 删除失效文件 '{file_name}' 失败，API未返回成功。")
                                failed_deletions.append(file_name)
                        except Exception as del_e:
                            logger.error(f"[{group_id}] [批量清理] 删除失效文件 '{file_name}' 时发生异常: {del_e}")
                            failed_deletions.append(file_name)
                    checked_count += 1
                    await asyncio.sleep(0.2)
                logger.info(f"[{group_id}] [批量清理] 批次处理完毕，已检查 {checked_count}/{total_count} 个文件。")
            report_message = f"✅ 清理完成！\n共扫描了 {total_count} 个文件。\n\n"
            if deleted_files:
                report_message += f"成功删除了 {len(deleted_files)} 个失效文件：\n"
                report_message += "\n".join(f"- {name}" for name in deleted_files)
            else:
                report_message += "未发现或未成功删除任何失效文件。"
            if failed_deletions:
                report_message += f"\n\n🚨 有 {len(failed_deletions)} 个失效文件删除失败，可能需要手动处理：\n"
                report_message += "\n".join(f"- {name}" for name in failed_deletions)
            logger.info(f"[{group_id}] [批量清理] 检查全部完成，准备发送报告。")
            await self._send_or_forward(event, report_message, name="失效文件清理报告")
        except Exception as e:
            logger.error(f"[{group_id}] [批量清理] 执行过程中发生未知异常: {e}", exc_info=True)
            await event.send(MessageChain([Comp.Plain("❌ 在执行批量清理时发生内部错误，请检查后台日志。")]))

    @filter.command("cf")
    async def on_check_files_command(self, event: AstrMessageEvent):
        if not self.bot: self.bot = event.bot
        group_id = int(event.get_group_id())
        user_id = int(event.get_sender_id())
        if user_id not in self.admin_users:
            await event.send(MessageChain([Comp.Plain("⚠️ 您没有执行此操作的权限。")]))
            return
        logger.info(f"[{group_id}] 用户 {user_id} 触发 /cf 失效文件检查指令。")
        await event.send(MessageChain([Comp.Plain("✅ 已开始扫描群内所有文件，查找失效文件...\n这可能需要几分钟，请耐心等待。\n如果未发现失效文件，将不会发送任何消息。")]))
        self.active_tasks.append(asyncio.create_task(self._perform_scheduled_check(group_id, False)))
        event.stop_event()
    
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=10)
    async def on_group_file_upload(self, event: AstrMessageEvent):
        if not self.bot: self.bot = event.bot
        has_file = any(isinstance(seg, Comp.File) for seg in event.get_messages())
        if has_file:
            group_id = int(event.get_group_id())
            logger.info(f"[{group_id}] 检测到文件上传事件，将在5秒后触发容量检查。")
            self.active_tasks.append(asyncio.create_task(self._check_storage_and_notify(event)))

    async def _check_storage_and_notify(self, event: AstrMessageEvent):
        group_id = int(event.get_group_id())
        if group_id not in self.storage_limits:
            return
        try:
            client = event.bot
            system_info = await client.api.call_action('get_group_file_system_info', group_id=group_id)
            if not system_info: return
            file_count = system_info.get('file_count', 0)
            used_space_bytes = system_info.get('used_space', 0)
            used_space_gb = float(utils.format_bytes(used_space_bytes, 'GB'))
            limits = self.storage_limits[group_id]
            count_limit = limits['count_limit']
            space_limit = limits['space_limit_gb']
            notifications = []
            if file_count >= count_limit:
                notifications.append(f"文件数量已达 {file_count}，接近或超过设定的 {count_limit} 上限！")
            if used_space_gb >= space_limit:
                notifications.append(f"已用空间已达 {used_space_gb:.2f}GB，接近或超过设定的 {space_limit:.2f}GB 上限！")
            if notifications:
                full_notification = "⚠️ 群文件容量警告 ⚠️\n" + "\n".join(notifications) + "\n请及时清理文件！"
                logger.warning(f"[{group_id}] 发送容量超限警告: {full_notification}")
                await event.send(MessageChain([Comp.Plain(full_notification)]))
        except ActionFailed as e:
            logger.error(f"[{group_id}] 调用 get_group_file_system_info 失败: {e}")
        except Exception as e:
            logger.error(f"[{group_id}] 处理容量检查时发生未知异常: {e}", exc_info=True)
    
    def _format_search_results(self, files: List[Dict], search_term: str, for_delete: bool = False) -> str:
        reply_text = f"🔍 找到了 {len(files)} 个与「{search_term}」相关的结果：\n"
        reply_text += "-" * 20
        for i, file_info in enumerate(files, 1):
            reply_text += (
                f"\n[{i}] {file_info.get('file_name')}"
                f"\n  上传者: {file_info.get('uploader_name', '未知')}"
                f"\n  大小: {utils.format_bytes(file_info.get('size'))}"
                f"\n  修改时间: {utils.format_timestamp(file_info.get('modify_time'))}"
            )
        reply_text += "\n" + "-" * 20
        if for_delete:
            reply_text += f"\n请使用 /df {search_term} [序号] 来删除指定文件。"
        else:
            reply_text += f"\n如需删除，请使用 /df {search_term} [序号]"
        return reply_text
    
    @filter.command("sf")
    async def on_search_file_command(self, event: AstrMessageEvent):
        if not self.bot: self.bot = event.bot
        group_id = int(event.get_group_id())
        user_id = int(event.get_sender_id())
        command_parts = event.message_str.split(maxsplit=2)
        if len(command_parts) < 2 or not command_parts[1]:
            await event.send(MessageChain([Comp.Plain("❓ 请提供要搜索的文件名。用法: /sf <文件名> [序号]")]))
            return
        filename_to_find = command_parts[1]
        index_str = command_parts[2] if len(command_parts) > 2 else None
        logger.info(f"[{group_id}] 用户 {user_id} 触发 /sf, 目标: '{filename_to_find}', 序号: {index_str}")
        
        all_files = await self._get_all_files_recursive_core(group_id, event.bot)
        found_files = []
        for file_info in all_files:
            current_filename = file_info.get('file_name', '')
            base_name, _ = os.path.splitext(current_filename)
            if filename_to_find in base_name or filename_to_find in current_filename:
                found_files.append(file_info)
        
        logger.info(f"[{group_id}] 在 {len(all_files)} 个文件中，找到 {len(found_files)} 个匹配项。")

        if not found_files:
            await event.send(MessageChain([Comp.Plain(f"❌ 未在群文件中找到与「{filename_to_find}」相关的任何文件。")]))
            return
        if not index_str:
            reply_text = self._format_search_results(found_files, filename_to_find)
            await self._send_or_forward(event, reply_text, name="文件搜索结果")
            return
        try:
            index = int(index_str)
            if not (1 <= index <= len(found_files)):
                await event.send(MessageChain([Comp.Plain(f"❌ 序号错误！找到了 {len(found_files)} 个文件，请输入 1 到 {len(found_files)} 之间的数字。")]))
                return
            file_to_preview = found_files[index - 1]
            preview_text, error_msg = await self._get_file_preview(event, file_to_preview)
            if error_msg:
                await event.send(MessageChain([Comp.Plain(error_msg)]))
                return
            
            reply_text = (
                f"📄 文件「{file_to_preview.get('file_name')}」内容预览：\n"
                + "-" * 20 + "\n"
                + preview_text
            )
            await self._send_or_forward(event, reply_text, name=f"文件预览：{file_to_preview.get('file_name')}")
        except ValueError:
            await event.send(MessageChain([Comp.Plain("❌ 序号必须是一个数字。")]))
        except Exception as e:
            logger.error(f"[{group_id}] 处理预览时发生未知异常: {e}", exc_info=True)
            await event.send(MessageChain([Comp.Plain("❌ 预览文件时发生内部错误，请检查后台日志。")]))
            
    @filter.command("df")
    async def on_delete_file_command(self, event: AstrMessageEvent):
        if not self.bot: self.bot = event.bot
        group_id = int(event.get_group_id())
        user_id = int(event.get_sender_id())
        command_parts = event.message_str.split(maxsplit=2)
        if len(command_parts) < 2 or not command_parts[1]:
            await event.send(MessageChain([Comp.Plain("❓ 请提供要删除的文件名。用法: /df <文件名> [序号]")]))
            return
        filename_to_find = command_parts[1]
        index_str = command_parts[2] if len(command_parts) > 2 else None
        logger.info(f"[{group_id}] 用户 {user_id} 触发删除指令 /df, 目标: '{filename_to_find}', 序号: {index_str}")
        if user_id not in self.admin_users:
            await event.send(MessageChain([Comp.Plain("⚠️ 您没有执行此操作的权限。")]))
            return

        all_files = await self._get_all_files_recursive_core(group_id, event.bot)
        found_files = []
        for file_info in all_files:
            current_filename = file_info.get('file_name', '')
            base_name, _ = os.path.splitext(current_filename)
            if filename_to_find in base_name or filename_to_find in current_filename:
                found_files.append(file_info)

        logger.info(f"[{group_id}] 在 {len(all_files)} 个文件中，找到 {len(found_files)} 个匹配项用于删除。")
            
        if not found_files:
            await event.send(MessageChain([Comp.Plain(f"❌ 未找到与「{filename_to_find}」相关的任何文件。")]))
            return
            
        if index_str == '0':
            self.active_tasks.append(asyncio.create_task(self._perform_batch_delete(event, found_files)))
            event.stop_event()
            return

        file_to_delete = None
        if len(found_files) == 1 and not index_str:
            file_to_delete = found_files[0]
        elif index_str:
            try:
                index = int(index_str)
                if 1 <= index <= len(found_files):
                    file_to_delete = found_files[index - 1]
                else:
                    await event.send(MessageChain([Comp.Plain(f"❌ 序号错误！找到了 {len(found_files)} 个文件，请输入 1 到 {len(found_files)} 之间的数字。")]))
                    return
            except ValueError:
                await event.send(MessageChain([Comp.Plain("❌ 序号必须是一个数字。")]))
                return
        else:
            reply_text = self._format_search_results(found_files, filename_to_find, for_delete=True)
            await self._send_or_forward(event, reply_text, name="文件搜索结果")
            return

        if not file_to_delete:
            await event.send(MessageChain([Comp.Plain("❌ 内部错误，未能确定要删除的文件。")]))
            return
        try:
            file_id_to_delete = file_to_delete.get("file_id")
            found_filename = file_to_delete.get("file_name")
            if not file_id_to_delete:
                await event.send(MessageChain([Comp.Plain(f"❌ 找到文件「{found_filename}」，但无法获取其ID，删除失败。")]))
                return
            logger.info(f"[{group_id}] 确认删除文件 '{found_filename}', File ID: {file_id_to_delete}...")
            client = event.bot
            delete_result = await client.api.call_action('delete_group_file', group_id=group_id, file_id=file_id_to_delete)
            is_success = False
            if delete_result:
                trans_result = delete_result.get('transGroupFileResult', {})
                result_obj = trans_result.get('result', {})
                if result_obj.get('retCode') == 0:
                    is_success = True
            if is_success:
                await event.send(MessageChain([Comp.Plain(f"✅ 文件「{found_filename}」已成功删除。")]))
                logger.info(f"[{group_id}] 文件 '{found_filename}' 已成功删除。")
            else:
                error_msg = delete_result.get('wording', 'API未返回成功状态')
                await event.send(MessageChain([Comp.Plain(f"❌ 删除文件「{found_filename}」失败: {error_msg}")]))
        except Exception as e:
            logger.error(f"[{group_id}] 处理删除流程时发生未知异常: {e}", exc_info=True)
            await event.send(MessageChain([Comp.Plain(f"❌ 处理删除时发生内部错误，请检查后台日志。")]))

    async def _perform_batch_delete(self, event: AstrMessageEvent, files_to_delete: List[Dict]):
        group_id = int(event.get_group_id())
        deleted_files = []
        failed_deletions = []
        total_count = len(files_to_delete)
        logger.info(f"[{group_id}] [批量删除] 开始处理 {total_count} 个文件的删除任务。")
        for i, file_info in enumerate(files_to_delete):
            file_id = file_info.get("file_id")
            file_name = file_info.get("file_name", "未知文件名")
            if not file_id:
                failed_deletions.append(f"{file_name} (缺少File ID)")
                continue
            try:
                logger.info(f"[{group_id}] [批量删除] ({i+1}/{total_count}) 正在删除 '{file_name}'...")
                delete_result = await event.bot.api.call_action('delete_group_file', group_id=group_id, file_id=file_id)
                is_success = False
                if delete_result:
                    trans_result = delete_result.get('transGroupFileResult', {})
                    result_obj = trans_result.get('result', {})
                    if result_obj.get('retCode') == 0:
                        is_success = True
                if is_success:
                    deleted_files.append(file_name)
                else:
                    failed_deletions.append(file_name)
            except Exception as e:
                logger.error(f"[{group_id}] [批量删除] 删除 '{file_name}' 时发生异常: {e}")
                failed_deletions.append(file_name)
            await asyncio.sleep(0.5)
        report_message = f"✅ 批量删除完成！\n共处理了 {total_count} 个文件。\n\n"
        if deleted_files:
            report_message += f"成功删除了 {len(deleted_files)} 个失效文件：\n"
            report_message += "\n".join(f"- {name}" for name in deleted_files)
        else:
            report_message += "未能成功删除任何文件。"
        if failed_deletions:
            report_message += f"\n\n🚨 有 {len(failed_deletions)} 个文件删除失败：\n"
            report_message += "\n".join(f"- {name}" for name in failed_deletions)
        logger.info(f"[{group_id}] [批量删除] 任务完成，准备发送报告。")
        await self._send_or_forward(event, report_message, name="批量删除报告")

    def _get_preview_from_bytes(self, content_bytes: bytes) -> tuple[str, str]:
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

    async def _get_preview_from_zip(self, file_path: str) -> tuple[str, str]:
        """从本地压缩文件中解压并预览第一个文本文件。返回 (预览内容, 错误信息)。
           使用 7za 命令来支持更多格式。
        """
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
                if self.default_zip_password:
                    logger.info("无密码解压失败，正在尝试使用默认密码...")
                    command_with_pwd = ["7za", "x", file_path, f"-o{extract_path}", f"-p{self.default_zip_password}", "-y"]
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
                # 如果没有找到txt文件，输出压缩包的文件结构
                if not all_extracted_files:
                    return "", "压缩包为空或解压失败"
                
                # 构建文件结构树
                file_structure = ["📦 压缩包内文件结构：\n"]
                for f_path in sorted(all_extracted_files):
                    relative_path = os.path.relpath(f_path, extract_path)
                    file_size = os.path.getsize(f_path)
                    size_str = utils.format_bytes(file_size)
                    # 计算缩进层级
                    depth = relative_path.count(os.sep)
                    indent = "  " * depth
                    file_name = os.path.basename(relative_path)
                    file_structure.append(f"{indent}├─ {file_name} ({size_str})")
                
                structure_text = "\n".join(file_structure)
                return structure_text, None
            
            with open(preview_file_path, 'rb') as f:
                content_bytes = f.read(self.preview_length * 4)
            
            preview_text_raw, encoding = self._get_preview_from_bytes(content_bytes)
            
            inner_file_name = os.path.relpath(preview_file_path, extract_path)
            extra_info = f"已解压「{inner_file_name}」(格式 {encoding})"
            preview_text = f"{extra_info}\n{preview_text_raw}"
            
        except FileNotFoundError:
            logger.error("解压失败：容器内未找到 7za 命令。请安装 p7zip-full。")
            error_msg = "解压失败：未安装 7za"
        except Exception as e:
            logger.error(f"处理ZIP文件时发生未知错误: {e}", exc_info=True)
            error_msg = "处理压缩文件时发生内部错误"
        finally:
            if os.path.exists(extract_path):
                asyncio.create_task(self._cleanup_folder(extract_path))
        
        return preview_text, error_msg

    async def _cleanup_folder(self, path: str):
        """异步清理文件夹及其内容。"""
        await asyncio.sleep(5)
        try:
            for dirpath, dirnames, filenames in os.walk(path, topdown=False):
                for filename in filenames:
                    os.remove(os.path.join(dirpath, filename))
                for dirname in dirnames:
                    os.rmdir(os.path.join(dirpath, dirname))
            os.rmdir(path)
            logger.info(f"已清理临时文件夹: {path}")
        except OSError as e:
            logger.warning(f"删除临时文件夹 {path} 失败: {e}")

    async def _get_file_preview(self, event: AstrMessageEvent, file_info: dict) -> tuple[str, str | None]:
        group_id = int(event.get_group_id())
        file_id = file_info.get("file_id")
        file_name = file_info.get("file_name", "")
        _, file_extension = os.path.splitext(file_name)
        
        is_txt = file_extension.lower() == '.txt'
        is_zip = self.enable_zip_preview and file_extension.lower() == '.zip'
        
        if not (is_txt or is_zip):
            return "", f"❌ 文件「{file_name}」不是支持的文本或压缩格式，无法预览。"
            
        logger.info(f"[{group_id}] 正在为文件 '{file_name}' (ID: {file_id}) 获取预览...")
        
        local_file_path = None
        
        try:
            client = event.bot
            url_result = await client.api.call_action('get_group_file_url', group_id=group_id, file_id=file_id)
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
                async with self.download_semaphore:
                    range_header = None
                    if is_txt:
                        read_bytes_limit = self.preview_length * 4
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
                decoded_text, _ = self._get_preview_from_bytes(content_bytes)
                preview_content = decoded_text
            elif is_zip:
                preview_text, error_msg = await self._get_preview_from_zip(local_file_path)
                if error_msg:
                    return "", error_msg
                preview_content = preview_text
            
            # 文件结构列表不截断，普通文本预览才截断
            is_file_structure = preview_content.startswith("📦 压缩包内文件结构：")
            if not is_file_structure and len(preview_content) > self.preview_length:
                preview_content = preview_content[:self.preview_length] + "..."
            
            # 发送前输出日志
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

    async def _create_zip_archive(self, source_dir: str, target_zip_path: str, password: str) -> bool:
        """使用外部命令行工具 (7za) 压缩整个目录。"""
        VOLUME_SIZE = '512m' 
        try:
            dir_to_zip = os.path.basename(source_dir)
            parent_dir = os.path.dirname(source_dir)

            # 7za a -tzip: 添加并创建 zip 格式归档
            # -r: 递归
            command = ['7za', 'a', '-tzip', target_zip_path, dir_to_zip, '-r', f'-v{VOLUME_SIZE}']
            
            if password:
                # 7za 使用 -p[密码] 格式
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
            logger.error("[群文件备份-压缩] 压缩失败：容器内未找到 7za 命令。请安装 p7zip-full 或 7-Zip。")
            return False
        except Exception as e:
            logger.error(f"[群文件备份-压缩] 打包时发生未知错误: {e}", exc_info=True)
            return False

    async def _perform_group_file_backup(self, event: AstrMessageEvent, group_id: int, date_filter_timestamp: Optional[int] = None):
        log_prefix = f"[群文件备份-{group_id}]"
        backup_root_dir = None
        final_zip_path = None
        
        try:
            client = self.bot or event.bot
            
            # 1. 预通知：获取群文件系统信息
            logger.info(f"{log_prefix} 正在获取群文件系统原始信息...")
            system_info = await client.api.call_action('get_group_file_system_info', group_id=group_id)
            
            # 记录原始的系统信息字典
            logger.info(f"{log_prefix} --- 群文件系统原始信息 START ---")
            for key, value in system_info.items():
                # 针对大整数进行格式化，便于阅读
                if isinstance(value, int) and ('space' in key or 'count' in key):
                    logger.info(f"{log_prefix} | {key}: {value} ({utils.format_bytes(value)})")
                else:
                    logger.info(f"{log_prefix} | {key}: {value}")
            logger.info(f"{log_prefix} --- 群文件系统原始信息 END ---")

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
            logger.info(f"{log_prefix} 本地备份目录: {backup_root_dir}")

            # 3. 递归获取所有文件信息
            all_files_info = await self._get_all_files_with_path(group_id, client)
            
            # 4. 过滤和下载文件
            total_file_count_all = len(all_files_info)
            downloaded_files_count = 0
            downloaded_files_size = 0
            failed_downloads = []
            
            size_limit_bytes = self.backup_file_size_limit_mb * 1024 * 1024
            
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
                if self.backup_file_extensions and ext not in self.backup_file_extensions:
                    continue
                
                # 4.3. 下载
                download_success = await self._download_and_save_file(
                    group_id, file_id, file_name, file_size, relative_path, backup_root_dir, client
                )
                
                if download_success:
                    downloaded_files_count += 1
                    downloaded_files_size += file_size
                else:
                    failed_downloads.append(file_name)

                #await asyncio.sleep(0.5) # 下载间隔

            # 5. 压缩整个目录
            final_zip_name = f"{group_name}_备份_{timestamp}.zip"
            final_zip_path = os.path.join(temp_base_dir, final_zip_name)
            
            logger.info(f"{log_prefix} 文件下载完成，共成功下载 {downloaded_files_count} 个文件，开始压缩...")

            zip_success = False

            if downloaded_files_count > 0:
                 zip_success = await self._create_zip_archive(backup_root_dir, final_zip_path, self.backup_zip_password)
            else:
                logger.warning(f"{log_prefix} 没有符合条件的文件需要备份，跳过压缩。")
                
            # 6. 发送和清理
            if zip_success:
                
                temp_base_dir = os.path.dirname(final_zip_path)
                # 基础名：不包含 .zip 部分 (如 'bot测试_备份_20251003_134542')
                zip_base_name_no_ext = os.path.basename(final_zip_path).rsplit('.zip', 1)[0]
                
                all_volumes = []
                
                logger.info(f"{log_prefix} 正在查找所有分卷文件，基础名: {zip_base_name_no_ext}，目录: {temp_base_dir}")
                
                # 查找所有分卷：匹配 '基础名' + '.zip' + '.' + 数字
                for f in os.listdir(temp_base_dir):
                    logger.debug(f"{log_prefix} 目录项: {f}")
                    f_path = os.path.join(temp_base_dir, f)
                    if f.startswith(zip_base_name_no_ext) and (f.endswith('.zip') or f.split('.')[-1].isdigit()):
                         # 确保我们只添加主文件和分卷文件，排除其他无关文件
                         if f == os.path.basename(final_zip_path) or f.startswith(f"{zip_base_name_no_ext}.zip."):
                            all_volumes.append(f_path)
                            try:
                                file_size = os.path.getsize(f_path)
                                logger.info(f"[{log_prefix}] 识别到分卷文件: {f} ({utils.format_bytes(file_size)})")
                            except Exception as e:
                                logger.warning(f"[{log_prefix}] 识别到分卷文件: {f} (获取大小失败: {e})")

                all_volumes.sort() # 确保按顺序发送
                
                is_single_volume = len(all_volumes) == 1
                
                logger.info(f"{log_prefix} 找到分卷文件数: {len(all_volumes)}")
                
                if is_single_volume:
                    original_path = all_volumes[0]
                    original_name = os.path.basename(original_path)
                    
                    new_volume_name = f"{zip_base_name_no_ext}.zip"
                    new_volume_path = os.path.join(temp_base_dir, new_volume_name)
                    
                    os.rename(original_path, new_volume_path) # 执行重命名
                    all_volumes = [new_volume_path] # 更新列表为新的路径
                    
                    logger.info(f"{log_prefix} [重命名] 单分卷重命名成功: '{original_name}' -> '{new_volume_name}'")

                if not all_volumes:
                    # 如果压缩成功，但一个文件都没找到，说明路径或匹配有问题
                    await event.send(MessageChain([Comp.Plain(f"❌ 备份压缩成功，但未在目录中找到任何生成的压缩文件！请检查日志。")]))
                    zip_success = False 
                    
                else:
                    # 构造回复消息
                    reply_message = (
                        f"✅ 群文件备份完成！\n"
                        f"成功备份文件数: {downloaded_files_count} 个 (总大小: {utils.format_bytes(downloaded_files_size)})\n"
                        f"{'共' if len(all_volumes) > 1 else ''} {len(all_volumes)} 个文件即将发送，请注意接收！"
                    )
                    if failed_downloads:
                        reply_message += f"\n⚠️ 备份失败文件数: {len(failed_downloads)} 个 (详见日志)"
                    
                    await event.send(MessageChain([Comp.Plain(reply_message)]))

                    # 逐个发送分卷文件
                    all_sent_success = True
                    for volume_path in all_volumes:
                        volume_name = os.path.basename(volume_path)
                        logger.info(f"{log_prefix} 正在发送分卷: {volume_name}...")
                        
                        if not await self._upload_and_send_file_via_api(event, volume_path, volume_name):
                            all_sent_success = False
                            # 发送失败通知给用户
                            await event.send(MessageChain([Comp.Plain(f"❌ 文件 {volume_name} 发送失败，请检查 Bot 配置。")]))
                            break
                            
                    if not all_sent_success:
                        await event.send(MessageChain([Comp.Plain(f"❌ 备份发送中断。请检查日志。")]))
                    
                        
            elif downloaded_files_count == 0:
                 await event.send(MessageChain([Comp.Plain(f"ℹ️ 备份任务完成。但没有找到符合大小或后缀名限制的任何文件。")]))
            else:
                await event.send(MessageChain([Comp.Plain(f"❌ 备份任务失败：压缩文件失败或找不到压缩包。请检查后台日志。")]))
            
        except Exception as e:
            logger.error(f"{log_prefix} 备份任务执行过程中发生未知异常: {e}", exc_info=True)
            await event.send(MessageChain([Comp.Plain(f"❌ 备份任务执行失败，发生内部错误。请检查后台日志。")]))
        finally:
             asyncio.create_task(self._cleanup_backup_temp(backup_root_dir, final_zip_path))

    @filter.command("ddf")
    async def on_detect_duplicates_command(self, event: AstrMessageEvent):
        """检测群文件中的重复文件（使用LLM分析）"""
        if not self.bot:
            self.bot = event.bot
        
        user_id = int(event.get_sender_id())
        group_id = event.get_group_id()
        
        if not group_id:
            yield event.plain_result("❌ 此指令仅可在群聊中使用。")
            return
        
        # 权限校验
        if user_id not in self.admin_users:
            yield event.plain_result("⚠️ 您没有执行重复文件检测的权限。")
            return
        
        # 白名单校验
        if self.group_whitelist and int(group_id) not in self.group_whitelist:
            yield event.plain_result("⚠️ 当前群聊不在插件配置的白名单中。")
            return
        
        try:
            # 获取所有文件信息
            all_files = await self._get_all_files_recursive_core(int(group_id), self.bot)
            
            if not all_files:
                yield event.plain_result("❌ 未找到任何文件。")
                return
            
            # 构建文件列表字符串
            file_list_parts = []
            for file_info in all_files:
                file_name = file_info.get('file_name', '未知')
                file_size = file_info.get('size', 0)
                file_time = file_info.get('modify_time', 0)
                
                # 格式化时间
                try:
                    time_str = datetime.fromtimestamp(file_time).strftime('%Y-%m-%d')
                except:
                    time_str = '未知'
                
                # 格式化文件大小
                size_str = utils.format_bytes(file_size)
                
                file_list_parts.append(f"- {file_name} | {time_str} | {size_str}")
            
            file_list_str = "\n".join(file_list_parts)
            
            # 构建提示词
            prompt = f"""请仔细分析下面的文件列表，找出所有疑似重复的小说文件（同一本书的不同版本），并以 Markdown 表格形式列出。

【重要规则】：
1. 只输出重复文件对，每对文件只出现一次！同一对文件不要重复列出（例如A和B重复，只能出现一次，不要既列"A→B"又列"B→A"）
2. 判断标准：
   - 文件名高度相似（去除版本号、日期、文件后缀等后主体名称相同）
   - 同一作者的同一作品
3. 状态列填写：
   - "可删除"：通过文件名差异可明确是旧版本，应该删除
   - "待定"：无法确定哪个是旧版本，需要人工判断

【输出格式】：
- 表格只有3列：状态 | 旧版本文件信息 | 新版本文件信息
- 文件信息必须在同一个单元格内，格式为：文件名 \ 修改时间 \ 大小，此三者缺一不可
- 注意：文件信息的各部分用"\"（反斜杠）连接，保持在同一个表格单元格中
- 按旧版本文件的修改时间从旧到新排序

【输出示例】：
| 状态 | 旧版本文件信息 | 新版本文件信息 |
|---|---|---|
| 可删除 | 《示例小说》01_10章.txt \ 2024-01-15 \ 1.2MB | 《示例小说》01_20章.txt \ 2024-02-20 \ 2.5MB |
| 待定 | 《另一本书》完整版.zip \ 2024-03-10 \ 3.5MB | 《另一本书》完整版.txt \ 2024-03-10 \ 8.2MB |

重要：表格只能有3列，不要把时间和大小拆分成独立的列！

【特别注意】：
- 如果文件列表中没有重复文件，直接输出：未检测到重复文件
- 不要输出任何其他解释或询问

以下是文件信息列表：

{file_list_str}"""
            
            # 获取当前聊天模型
            umo = event.unified_msg_origin
            provider_id = await self.context.get_current_chat_provider_id(umo=umo)
            
            if not provider_id:
                yield event.plain_result("❌ 未配置聊天模型，无法使用此功能。")
                return
            
            yield event.plain_result("🤖 正在调用AI检测重复文件，这可能需要一些时间...")
            
            # 调用LLM
            llm_resp = await self.context.llm_generate(
                chat_provider_id=provider_id,
                prompt=prompt,
            )
            
            if not llm_resp or not llm_resp.completion_text:
                yield event.plain_result("❌ AI分析失败，请稍后重试。")
                return
            
            # 检查是否检测到重复文件
            response_text = llm_resp.completion_text.strip()
            if "未检测到重复文件" in response_text:
                # 如果没有重复文件，直接发送文本消息
                yield event.plain_result("✅ 未检测到重复文件")
                return
            
            # 将文本转换为图片
            try:
                logger.info(f"[重复文件检测] AI响应结果：\n{response_text}")
                image_url = await self.text_to_image(response_text)
                yield event.image_result(image_url)
            except Exception as img_error:
                logger.warning(f"文本转图片失败: {img_error}，将发送纯文本结果")
                # 如果转图失败，降级为发送文本
                await self._send_or_forward(event, response_text, "重复文件检测")
            
        except Exception as e:
            logger.error(f"检测重复文件失败: {e}", exc_info=True)
            yield event.plain_result(f"❌ 检测过程中发生错误: {str(e)}")

    @filter.command("gfb")
    async def on_group_file_backup_command(self, event: AstrMessageEvent):
        if not self.bot: self.bot = event.bot
        
        # 1. 解析目标群ID和日期参数
        group_id_str = event.get_group_id()
        user_id = int(event.get_sender_id())
        
        command_parts = event.message_str.split()
        target_group_id: Optional[int] = None
        date_filter_timestamp: Optional[int] = None
        
        # 解析参数: /gfb [群号] [日期]
        if len(command_parts) > 1:
            try:
                target_group_id = int(command_parts[1])
                # 如果有第三个参数，尝试解析为日期
                if len(command_parts) > 2:
                    date_filter_timestamp = utils.parse_date_param(command_parts[2])
                    if date_filter_timestamp is None:
                        await event.send(MessageChain([Comp.Plain("❌ 日期格式错误。支持格式: YYYY-MM-DD, YYYYMMDD, YYYY/MM/DD\n示例: /gfb 123456 2024-01-01")]))
                        return
            except ValueError:
                # 可能第一个参数是日期而不是群号
                if group_id_str:
                    target_group_id = int(group_id_str)
                    date_filter_timestamp = utils.parse_date_param(command_parts[1])
                    if date_filter_timestamp is None:
                        await event.send(MessageChain([Comp.Plain("❌ 格式错误：请提供有效的群号或日期。\n用法: /gfb [群号] [日期]\n日期格式: YYYY-MM-DD, YYYYMMDD, YYYY/MM/DD")]))
                        return
                else:
                    await event.send(MessageChain([Comp.Plain("❌ 格式错误：请提供有效的群号。用法: /gfb <群号> [日期]")]))
                    return
        elif group_id_str:
            # 群聊中且没有参数，备份当前群
            target_group_id = int(group_id_str)
        else:
            # 私聊中且没有参数
            await event.send(MessageChain([Comp.Plain("❌ 格式错误：在私聊中请指定要备份的群号。\n用法: /gfb <群号> [日期]\n日期格式: YYYY-MM-DD, YYYYMMDD, YYYY/MM/DD")]))
            return

        logger.info(f"用户 {user_id} 触发 /gfb 备份指令，目标群ID: {target_group_id}, 日期筛选: {command_parts[2] if len(command_parts) > 2 else '无'}")

        # 2. 权限和白名单校验
        if user_id not in self.admin_users:
            await event.send(MessageChain([Comp.Plain("⚠️ 您没有执行群文件备份操作的权限。")]))
            return
        
        if self.group_whitelist and target_group_id not in self.group_whitelist:
            await event.send(MessageChain([Comp.Plain("⚠️ 目标群聊不在插件配置的白名单中，操作已拒绝。")]))
            return

        # 3. 启动异步备份任务
        self.active_tasks.append(asyncio.create_task(
            self._perform_group_file_backup(event, target_group_id, date_filter_timestamp)
        ))
        event.stop_event()

    async def terminate(self):
        logger.info("插件 [群文件系统GroupFS] 正在卸载，取消所有任务...")

        if self.scheduler and self.scheduler.running:
            try:
                self.scheduler.shutdown(wait=False) 
                logger.info("APScheduler 定时任务调度器已成功停止。")
            except Exception as e:
                logger.error(f"停止 APScheduler 时发生错误: {e}")

        for task in self.active_tasks:
            if not task.done():
                task.cancel()
        
        try:
            await asyncio.gather(*self.active_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        
        logger.info("插件 [群文件系统GroupFS] 已卸载。")