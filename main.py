# astrbot_plugin_GroupFS/main.py

# 请确保已安装依赖: pip install croniter aiohttp chardet apscheduler
import asyncio
import os
from typing import List, Dict, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import croniter

from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger
import astrbot.api.message_components as Comp
from aiocqhttp.exceptions import ActionFailed 

from .src import utils
from .src.file_ops import (
    get_all_files_with_path, 
    download_and_save_file, 
    create_zip_archive, 
    cleanup_folder, 
    cleanup_backup_temp,
    get_all_files_recursive_core
)
from .src.preview_utils import get_file_preview
from .src.utils import send_or_forward
from .src.actions import (
    perform_scheduled_check, 
    perform_batch_check_and_delete,
    perform_batch_delete,
    check_storage_and_notify
)
from .src.backup import perform_group_file_backup
from .src.duplicate_check import detect_duplicates
from .src.session_manager import SessionManager


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

        # 搜索会话管理
        self.search_cache_timeout = 600 # 10分钟
        self.search_results_per_page = 20
        self.session_mgr = SessionManager(timeout=self.search_cache_timeout)

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

    async def _send_or_forward(self, event: AstrMessageEvent, text: str, name: str = "GroupFS"):
        await send_or_forward(event, text, self.forward_threshold, name)

    async def _perform_scheduled_check(self, group_id: int, auto_delete: bool):
        await perform_scheduled_check(group_id, auto_delete, self.bot, self.storage_limits, self.scheduled_autodelete)


    async def _get_all_files_with_path(self, group_id: int, bot) -> List[Dict]:
        return await get_all_files_with_path(group_id, bot)
    
    async def _get_all_files_recursive_core(self, group_id: int, bot) -> List[Dict]:
        """
        兼容 /cdf, /cf, /sf, /df 等指令。
        """
        return await get_all_files_recursive_core(group_id, bot)
    
    async def _download_and_save_file(self, group_id: int, file_id: str, file_name: str, file_size: int, relative_path: str, root_dir: str, client) -> bool:
        return await download_and_save_file(group_id, file_id, file_name, file_size, relative_path, root_dir, client, self.download_semaphore)

    async def _cleanup_backup_temp(self, backup_dir: str, zip_path: Optional[str]):
        await cleanup_backup_temp(backup_dir, zip_path)

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
        await perform_batch_check_and_delete(event, self.forward_threshold)

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
        await check_storage_and_notify(event, self.storage_limits)
    
    def _format_search_results(self, files: List[Dict], search_term: str, for_delete: bool = False) -> str:
        return utils.format_search_results(files, search_term, for_delete)
    
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
        
        # 检查是否是翻页指令 (特殊处理：如果文件名是 'n' 且存在会话)
        if filename_to_find.lower() == 'n':
            session = self.session_mgr.get_session(group_id, user_id)
            if session:
                await self._show_search_page(event, session, session.current_page + 1)
                return

        logger.info(f"[{group_id}] 用户 {user_id} 触发 /sf, 目标: '{filename_to_find}', 序号: {index_str}")
        
        # 检查是否有现有会话且关键词匹配
        session = self.session_mgr.get_session(group_id, user_id)
        if session and session.keyword == filename_to_find and index_str:
            # 如果有序号且关键词匹配，直接从会话中取文件预览
            try:
                index = int(index_str)
                if 1 <= index <= session.total_count:
                    file_to_preview = session.results[index - 1]
                    await self._handle_preview(event, file_to_preview)
                    return
            except ValueError:
                pass

        # 重新搜索
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
            
        # 创建新会话
        session = self.session_mgr.create_session(group_id, user_id, filename_to_find, found_files, self.search_results_per_page)
        
        if index_str:
            try:
                index = int(index_str)
                if 1 <= index <= len(found_files):
                    file_to_preview = found_files[index - 1]
                    await self._handle_preview(event, file_to_preview)
                    return
                else:
                    await event.send(MessageChain([Comp.Plain(f"❌ 序号错误！找到了 {len(found_files)} 个文件，请输入 1 到 {len(found_files)} 之间的数字。")]))
                    return
            except ValueError:
                await event.send(MessageChain([Comp.Plain("❌ 序号必须是一个数字。")]))
                return
        
        # 显示第一页
        await self._show_search_page(event, session, 1)

    async def _show_search_page(self, event: AstrMessageEvent, session, page: int):
        total_pages = (session.total_count + session.page_size - 1) // session.page_size
        if page > total_pages:
            await event.send(MessageChain([Comp.Plain("⚠️ 已经是最后一页了。")]))
            return
        
        session.current_page = page
        results = session.get_page_results(page)
        
        reply_text = f"🔍 找到了 {session.total_count} 个与「{session.keyword}」相关的结果 (第 {page}/{total_pages} 页)：\n"
        reply_text += "-" * 20
        
        start_idx = (page - 1) * session.page_size + 1
        for i, file_info in enumerate(results, start_idx):
            reply_text += (
                f"\n[{i}] {file_info.get('file_name')}"
                f"\n  上传者: {file_info.get('uploader_name', '未知')}"
                f"\n  大小: {utils.format_bytes(file_info.get('size'))}"
                f"\n  修改时间: {utils.format_timestamp(file_info.get('modify_time'))}"
            )
        
        reply_text += "\n" + "-" * 20
        if page < total_pages:
            reply_text += f"\n输入 /sf n 查看下一页"
        reply_text += f"\n如需预览/删除，请使用 /sf {session.keyword} [序号] 或 /df {session.keyword} [序号]"
        
        await self._send_or_forward(event, reply_text, name="文件搜索结果")

    async def _handle_preview(self, event: AstrMessageEvent, file_to_preview: dict):
        group_id = int(event.get_group_id())
        try:
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

        # 尝试从会话中获取
        session = self.session_mgr.get_session(group_id, user_id)
        found_files = []
        if session and session.keyword == filename_to_find:
            found_files = session.results
        else:
            all_files = await self._get_all_files_recursive_core(group_id, event.bot)
            for file_info in all_files:
                current_filename = file_info.get('file_name', '')
                base_name, _ = os.path.splitext(current_filename)
                if filename_to_find in base_name or filename_to_find in current_filename:
                    found_files.append(file_info)

        logger.info(f"[{group_id}] 在搜索中找到 {len(found_files)} 个匹配项用于删除。")
            
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
            # 如果没提供序号且有多个结果，创建/更新会话并显示结果
            session = self.session_mgr.create_session(group_id, user_id, filename_to_find, found_files, self.search_results_per_page)
            await self._show_search_page(event, session, 1)
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
                # 删除成功后，如果会话存在，从会话中移除
                if session and session.keyword == filename_to_find:
                    session.results.remove(file_to_delete)
                    session.total_count = len(session.results)
                    if session.total_count == 0:
                        self.session_mgr.clear_session(group_id, user_id)
            else:
                error_msg = delete_result.get('wording', 'API未返回成功状态')
                await event.send(MessageChain([Comp.Plain(f"❌ 删除文件「{found_filename}」失败: {error_msg}")]))
        except Exception as e:
            logger.error(f"[{group_id}] 处理删除流程时发生未知异常: {e}", exc_info=True)
            await event.send(MessageChain([Comp.Plain(f"❌ 处理删除时发生内部错误，请检查后台日志。")]))

    async def _perform_batch_delete(self, event: AstrMessageEvent, files_to_delete: List[Dict]):
        await perform_batch_delete(event, files_to_delete, self.forward_threshold)

    async def _cleanup_folder(self, path: str):
        await cleanup_folder(path)

    async def _get_file_preview(self, event: AstrMessageEvent, file_info: dict) -> tuple[str, str | None]:
        return await get_file_preview(
            int(event.get_group_id()), 
            file_info, 
            event.bot, 
            self.default_zip_password, 
            self.preview_length, 
            self.download_semaphore,
            self._cleanup_folder
        )

    async def _create_zip_archive(self, source_dir: str, target_zip_path: str, password: str) -> bool:
        return await create_zip_archive(source_dir, target_zip_path, password)

    async def _perform_group_file_backup(self, event: AstrMessageEvent, group_id: int, date_filter_timestamp: Optional[int] = None):
        await perform_group_file_backup(
            event, 
            group_id, 
            self.bot, 
            self.download_semaphore, 
            self.backup_file_size_limit_mb,
            self.backup_file_extensions,
            self.backup_zip_password,
            date_filter_timestamp
        )

    @filter.command("ddf")
    async def on_detect_duplicates_command(self, event: AstrMessageEvent):
        """检测群文件中的重复文件（使用LLM分析）"""
        
        async for result in detect_duplicates(
            event, 
            self.bot, 
            self.context, 
            self.admin_users, 
            self.group_whitelist, 
            self._get_all_files_recursive_core, 
            self.text_to_image, 
            self._send_or_forward
        ):
            yield result

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