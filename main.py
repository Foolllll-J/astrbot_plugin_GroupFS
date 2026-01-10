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
    get_all_files_recursive_core,
    rename_group_file
)
from .src.preview_utils import get_file_preview
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
    "1.0",
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
        # 尝试从 platform_manager 自动获取 bot 实例
        if hasattr(self.context, "platform_manager"):
            try:
                platforms = self.context.platform_manager.get_insts()
                for platform in platforms:
                    bot_client = None
                    if hasattr(platform, "get_client"):
                        bot_client = platform.get_client()
                    elif hasattr(platform, "bot"):
                        bot_client = platform.bot
                    
                    if bot_client:
                        self.bot = bot_client
                        logger.info(f"[初始化] 成功从 platform_manager 获取 bot 实例")
                        break
            except Exception as e:
                logger.warning(f"[初始化] 从 platform_manager 获取 bot 实例失败: {e}")
        
        # 启动定时任务
        if self.cron_configs:
            if self.bot:
                # 如果已经获取到 bot，直接启动调度器，不需要等待 30 秒
                logger.info("[定时任务] 启动失效文件检查调度器...")
                self.scheduler = AsyncIOScheduler()
                self._register_jobs()
                self.scheduler.start()
            else:
                # 只有在没获取到 bot 的情况下，才进入延迟启动流程
                logger.warning("[初始化] 未能立即获取 bot 实例，将通过延迟任务捕获并启动调度器。")
                asyncio.create_task(self._delayed_start_scheduler())

    async def _delayed_start_scheduler(self):
        """延迟启动调度器，等待系统初始化并尝试再次获取 bot"""
        try:
            # 等待 30 秒让系统完全初始化
            await asyncio.sleep(30)
            
            # 再次尝试获取 bot
            if not self.bot and hasattr(self.context, "platform_manager"):
                platforms = self.context.platform_manager.get_insts()
                for platform in platforms:
                    bot_client = None
                    if hasattr(platform, "get_client"):
                        bot_client = platform.get_client()
                    elif hasattr(platform, "bot"):
                        bot_client = platform.bot
                    
                    if bot_client:
                        self.bot = bot_client
                        logger.info("[定时任务] 延迟启动过程中成功补获 bot 实例")
                        break
            
            # 启动调度器
            if not self.scheduler:
                logger.info("[定时任务] 延迟启动调度器...")
                self.scheduler = AsyncIOScheduler()
                self._register_jobs()
                self.scheduler.start()
            
            if not self.bot:
                logger.warning("[定时任务] 调度器已启动，但尚未获取到 bot 实例。")
                
        except Exception as e:
            logger.error(f"[定时任务] 延迟启动失败: {e}", exc_info=True)

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
                    self._apscheduler_check_task,
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

    async def _apscheduler_check_task(self, group_id: int, auto_delete: bool):
        """APScheduler 调用的包装函数，负责消费生成器并发送消息"""
        if not self.bot:
            logger.warning(f"[{group_id}] [定时任务] 无法执行，因为尚未捕获到 bot 实例。")
            return

        async for msg in perform_scheduled_check(group_id, auto_delete, self.bot, self.storage_limits, self.scheduled_autodelete):
            if self.bot:
                await self.bot.api.call_action('send_group_msg', group_id=group_id, message=msg)

    async def _perform_scheduled_check(self, group_id: int, auto_delete: bool):
        async for res in perform_scheduled_check(group_id, auto_delete, self.bot, self.storage_limits, self.scheduled_autodelete):
            yield res


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

    @filter.command("cdf", alias={"清理失效文件","清理失效群文件"})
    async def on_check_and_delete_command(self, event: AstrMessageEvent):
        """扫描并自动删除所有失效文件"""
        if not self.bot: self.bot = event.bot
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        group_id = int(group_id_str)
        user_id = int(event.get_sender_id())
        logger.info(f"[{group_id}] 用户 {user_id} 触发 /cdf 失效文件清理指令。")
        if user_id not in self.admin_users:
            yield event.plain_result("⚠️ 您没有执行此操作的权限。")
            return
        yield event.plain_result("⚠️ 警告：即将开始扫描并自动删除所有失效文件！\n此过程可能需要几分钟，请耐心等待，完成后将发送报告。")
        async for res in perform_batch_check_and_delete(event):
            yield res

    @filter.command("cf", alias={"检查群文件"})
    async def on_check_files_command(self, event: AstrMessageEvent):
        """扫描并查找群内的失效文件（仅检查不删除）"""
        if not self.bot: self.bot = event.bot
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        group_id = int(group_id_str)
        user_id = int(event.get_sender_id())
        if user_id not in self.admin_users:
            yield event.plain_result("⚠️ 您没有执行此操作的权限。")
            return
        logger.info(f"[{group_id}] 用户 {user_id} 触发 /cf 失效文件检查指令。")
        yield event.plain_result("✅ 已开始扫描群内所有文件，查找失效文件...\n这可能需要几分钟，请耐心等待。\n如果未发现失效文件，将不会发送任何消息。")
        async for res in self._perform_scheduled_check(group_id, False):
            if isinstance(res, str):
                yield event.plain_result(res)
            else:
                yield res
    
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=10)
    async def on_group_file_upload(self, event: AstrMessageEvent):
        """监控群文件上传事件"""
        if not self.bot: self.bot = event.bot
        file_component = next((seg for seg in event.get_messages() if isinstance(seg, Comp.File)), None)
        if file_component:
            group_id = int(event.get_group_id())
            
            # 仅在配置了容量检查的群组中触发
            if group_id not in self.storage_limits:
                return

            file_id = getattr(file_component, 'file_id', None)
            file_name = getattr(file_component, 'file_name', None) or getattr(file_component, 'name', None)
            
            logger.info(f"[{group_id}] 检测到文件上传事件: {file_name} ({file_id})，将在5秒后触发容量检查。")
            await asyncio.sleep(5)
            async for res in self._check_storage_and_notify(event):
                yield res

    async def _check_storage_and_notify(self, event: AstrMessageEvent):
        async for res in check_storage_and_notify(event, self.storage_limits):
            yield res
    
    def _format_search_results(self, files: List[Dict], search_term: str, for_delete: bool = False) -> str:
        return utils.format_search_results(files, search_term, for_delete)
    
    @filter.command("sf", alias={"搜索"})
    async def on_search_file_command(self, event: AstrMessageEvent):
        """搜索群文件"""
        if not self.bot: self.bot = event.bot
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        group_id = int(group_id_str)
        user_id = int(event.get_sender_id())
        command_parts = event.message_str.split()
        
        if len(command_parts) < 2:
            yield event.plain_result("❓ 请提供要搜索的文件名。用法: /搜索 <文件名> [序号]")
            return
            
        action = command_parts[1].lower()
        session = self.session_mgr.get_session(group_id, user_id)

        # 检查是否是翻页指令
        if action in ['n', 'next', '下一页']:
            if session:
                async for res in self._show_search_page(event, session, session.current_page + 1):
                    yield res
                return
            else:
                yield event.plain_result("❌ 请先执行搜索。")
                return
        elif action in ['p', 'prev', '上一页']:
            if session:
                async for res in self._show_search_page(event, session, max(1, session.current_page - 1)):
                    yield res
                return
            else:
                yield event.plain_result("❌ 请先执行搜索。")
                return
        elif action in ['page', '跳转'] and len(command_parts) > 2:
            if session:
                try:
                    target_page = int(command_parts[2])
                    async for res in self._show_search_page(event, session, target_page):
                        yield res
                    return
                except ValueError:
                    pass
            else:
                yield event.plain_result("❌ 请先执行搜索。")
                return
        elif action.isdigit() and not session:
             # 如果用户直接输入 /sf 1 但没有会话，提示搜索
             yield event.plain_result("❌ 请先输入文件名进行搜索。")
             return
        elif action.isdigit() and session:
            # 如果输入的是数字且有会话，执行跳转页面
            async for res in self._show_search_page(event, session, int(action)):
                yield res
            return

        filename_to_find = command_parts[1]
        index_str = command_parts[2] if len(command_parts) > 2 else None
        
        logger.info(f"[{group_id}] 用户 {user_id} 触发 /sf, 目标: '{filename_to_find}', 序号: {index_str}")
        
        # 检查是否有现有会话且关键词匹配
        session = self.session_mgr.get_session(group_id, user_id)
        if session and session.keyword == filename_to_find and index_str:
            # 如果有序号且关键词匹配，直接从会话中取文件预览
            try:
                index = int(index_str)
                if 1 <= index <= session.total_count:
                    file_to_preview = session.results[index - 1]
                    async for res in self._handle_preview(event, file_to_preview):
                        yield res
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
            yield event.plain_result(f"❌ 未在群文件中找到与「{filename_to_find}」相关的任何文件。")
            return
            
        # 创建新会话
        session = self.session_mgr.create_session(group_id, user_id, filename_to_find, found_files, self.search_results_per_page)
        
        if index_str:
            try:
                index = int(index_str)
                if 1 <= index <= len(found_files):
                    file_to_preview = found_files[index - 1]
                    async for res in self._handle_preview(event, file_to_preview):
                        yield res
                    return
                else:
                    yield event.plain_result(f"❌ 序号错误！找到了 {len(found_files)} 个文件，请输入 1 到 {len(found_files)} 之间的数字。")
                    return
            except ValueError:
                yield event.plain_result("❌ 序号必须是一个数字。")
                return
        
        # 显示第一页
        async for res in self._show_search_page(event, session, 1):
            yield res

    async def _show_search_page(self, event: AstrMessageEvent, session, page: int):
        total_pages = (session.total_count + session.page_size - 1) // session.page_size
        if page > total_pages:
            yield event.plain_result("⚠️ 已经是最后一页了。")
            return
        
        session.current_page = page
        results = session.get_page_results(page)
        
        reply_text = f"🔍 找到了 {session.total_count} 个与「{session.keyword}」相关的结果 (第 {page}/{total_pages} 页)：\n"
        reply_text += "-" * 20
        
        start_idx = (page - 1) * session.page_size + 1
        for i, file_info in enumerate(results, start_idx):
            parent_folder = file_info.get('parent_folder_name', '根目录')
            path_display = f"\n  路径: {parent_folder}/" if parent_folder != '根目录' else ""
            
            reply_text += (
                f"\n[{i}] {file_info.get('file_name')}"
                f"{path_display}"
                f"\n  上传者: {file_info.get('uploader_name', '未知')}"
                f"\n  大小: {utils.format_bytes(file_info.get('size'))}"
                f"\n  修改时间: {utils.format_timestamp(file_info.get('modify_time'))}"
            )
        
        reply_text += "\n" + "-" * 20
        if page < total_pages:
            reply_text += f"\n输入 /sf 下一页 查看下一页"
        if page > 1:
            reply_text += f"\n输入 /sf 上一页 查看上一页"
        reply_text += f"\n输入 /sf 跳转 <页码> 跳转"
        reply_text += f"\n\n💡 快速操作："
        reply_text += f"\n• /预览 <序号> - 预览文件"
        reply_text += f"\n• /删除 <序号> - 删除文件"
        
        yield event.plain_result(reply_text)

    async def _handle_preview(self, event: AstrMessageEvent, file_to_preview: dict, inner_path: str = None):
        group_id = int(event.get_group_id())
        try:
            preview_text, error_msg = await self._get_file_preview(event, file_to_preview, inner_path)
            if error_msg:
                yield event.plain_result(error_msg)
                return
            
            title = f"📄 文件「{file_to_preview.get('file_name')}」内容预览"
            if inner_path:
                title += f" (内部路径: {inner_path})"
            
            reply_text = (
                f"{title}：\n"
                + "-" * 20 + "\n"
                + preview_text
            )
            yield event.plain_result(reply_text)
        except Exception as e:
            logger.error(f"[{group_id}] 处理预览时发生未知异常: {e}", exc_info=True)
            yield event.plain_result("❌ 预览文件时发生内部错误，请检查后台日志。")

    @filter.command("preview", alias={"预览"})
    async def on_preview_command(self, event: AstrMessageEvent):
        """预览群文件内容"""
        if not self.bot: self.bot = event.bot
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        group_id = int(group_id_str)
        user_id = int(event.get_sender_id())
        command_parts = event.message_str.split()
        
        # 检查是否引用了文件
        quoted_file = await self._resolve_file_from_message(event)

        if len(command_parts) < 2:
            if quoted_file:
                # 引用预览：/预览
                async for res in self._handle_preview(event, quoted_file):
                    yield res
                return
            yield event.plain_result("❓ 用法: /预览 <序号> [内部序号] 或 /预览 <文件名> [序号] [内部序号] 或 引用文件消息后输入 /预览")
            return
            
        arg1 = command_parts[1]
        arg2 = command_parts[2] if len(command_parts) > 2 else None
        session = self.session_mgr.get_session(group_id, user_id)
        
        # 场景 0: 引用预览且带内部路径/序号：引用文件后输入 /预览 1
        if quoted_file and arg1:
            # 如果 arg1 是数字且不是文件名，尝试作为内部路径预览
            async for res in self._handle_preview(event, quoted_file, arg1):
                yield res
            return

        # 场景 1: /预览 <序号> [内部路径/序号]
        if arg1.isdigit():
            if not session:
                yield event.plain_result("❌ 请先执行 /搜索 搜索文件。")
                return
            index = int(arg1)
            if 1 <= index <= session.total_count:
                file_to_preview = session.results[index - 1]
                inner_path = arg2
                async for res in self._handle_preview(event, file_to_preview, inner_path):
                    yield res
            else:
                yield event.plain_result(f"❌ 序号错误！有效范围: 1-{session.total_count}")
            return
            
        # 场景 2: /preview <文件名> [序号] [内部路径]
        filename_to_find = arg1
        
        # 搜索文件
        all_files = await self._get_all_files_recursive_core(group_id, event.bot)
        found_files = [f for f in all_files if filename_to_find in f.get('file_name', '')]
        
        if not found_files:
            yield event.plain_result(f"❌ 未找到文件「{filename_to_find}」。")
            return
            
        file_to_preview = None
        inner_path = None

        if arg2 and arg2.isdigit():
            # /preview <文件名> <序号> [内部路径]
            idx = int(arg2)
            if 1 <= idx <= len(found_files):
                file_to_preview = found_files[idx-1]
                inner_path = command_parts[3] if len(command_parts) > 3 else None
            else:
                yield event.plain_result(f"❌ 序号错误！共找到 {len(found_files)} 个匹配文件。")
                return
        else:
            # /preview <文件名> [内部路径]
            if len(found_files) == 1:
                file_to_preview = found_files[0]
                inner_path = arg2
            else:
                # 多个结果，更新会话并显示
                session = self.session_mgr.create_session(group_id, user_id, filename_to_find, found_files, 20)
                async for res in self._show_search_page(event, session, 1):
                    yield res
                return

        if file_to_preview:
            async for res in self._handle_preview(event, file_to_preview, inner_path):
                yield res

    @filter.command("df", alias={"删除"})
    async def on_delete_file_command(self, event: AstrMessageEvent):
        """删除指定的群文件"""
        if not self.bot: self.bot = event.bot
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        group_id = int(group_id_str)
        user_id = int(event.get_sender_id())
        command_parts = event.message_str.split()
        
        if user_id not in self.admin_users:
            yield event.plain_result("⚠️ 您没有执行此操作的权限。")
            return

        # 检查是否引用了文件
        quoted_file = await self._resolve_file_from_message(event)
        
        if len(command_parts) < 2:
            if quoted_file:
                async for res in self._perform_single_delete(event, quoted_file):
                    yield res
                return
            yield event.plain_result("❓ 用法: /删除 <序号> 或 /删除 <1,2,3> 或 /删除 <文件名> [序号/0/1,2,3] 或 引用文件消息后输入 /删除")
            return
            
        target = command_parts[1]
        session = self.session_mgr.get_session(group_id, user_id)
        
        # 场景 1: /删除 <序号> 或 /删除 <1,2,3>
        if target.isdigit() or ',' in target:
            if not session:
                yield event.plain_result("❌ 请先执行 /搜索 搜索文件。")
                return
            
            # 处理多选删除 /df 1,2,3
            if ',' in target:
                try:
                    indices = [int(i.strip()) for i in target.split(',') if i.strip().isdigit()]
                    files_to_delete = []
                    for idx in indices:
                        if 1 <= idx <= session.total_count:
                            files_to_delete.append(session.results[idx-1])
                    if files_to_delete:
                        async for res in self._perform_batch_delete(event, files_to_delete):
                            yield res
                        return
                except ValueError:
                    pass
            
            # 处理单选或全选
            index = int(target) if target.isdigit() else -1
            if index == 0:
                # 特殊场景: /df 0 删除当前搜索结果中的所有文件
                async for res in self._perform_batch_delete(event, session.results):
                    yield res
                return
            if 1 <= index <= session.total_count:
                file_to_delete = session.results[index - 1]
                async for res in self._perform_single_delete(event, file_to_delete, session):
                    yield res
            elif index != -1:
                yield event.plain_result(f"❌ 序号错误！有效范围: 1-{session.total_count}")
            return
            
        # 场景 2: /df <文件名> [序号/0/批量序号]
        filename_to_find = target
        index_str = command_parts[2] if len(command_parts) > 2 else None
        
        # 优先使用会话缓存
        found_files = []
        if session and session.keyword == filename_to_find:
            found_files = session.results
        else:
            all_files = await self._get_all_files_recursive_core(group_id, event.bot)
            found_files = [f for f in all_files if filename_to_find in f.get('file_name', '')]

        if not found_files:
            yield event.plain_result(f"❌ 未找到文件「{filename_to_find}」。")
            return

        # 批量删除 (/df <文件名> 0) 或 (/df <文件名> 1,2,3)
        if index_str:
            if index_str == '0':
                async for res in self._perform_batch_delete(event, found_files):
                    yield res
                return
            elif ',' in index_str:
                try:
                    indices = [int(i.strip()) for i in index_str.split(',') if i.strip().isdigit()]
                    files_to_delete = []
                    for idx in indices:
                        if 1 <= idx <= len(found_files):
                            files_to_delete.append(found_files[idx-1])
                    if files_to_delete:
                        async for res in self._perform_batch_delete(event, files_to_delete):
                            yield res
                        return
                except ValueError:
                    pass

        if len(found_files) == 1 and not index_str:
            async for res in self._perform_single_delete(event, found_files[0], session):
                yield res
        elif index_str and index_str.isdigit():
            idx = int(index_str)
            if 1 <= idx <= len(found_files):
                async for res in self._perform_single_delete(event, found_files[idx-1], session):
                    yield res
            else:
                yield event.plain_result(f"❌ 序号错误！共找到 {len(found_files)} 个文件。")
        else:
            # 多个结果，更新会话并显示
            session = self.session_mgr.create_session(group_id, user_id, filename_to_find, found_files, self.search_results_per_page)
            async for res in self._show_search_page(event, session, 1):
                yield res

    @filter.command("rn", alias={"重命名"})
    async def on_rename_command(self, event: AstrMessageEvent):
        """重命名指定的群文件"""
        if not self.bot: self.bot = event.bot
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        group_id = int(group_id_str)
        user_id = int(event.get_sender_id())
        command_parts = event.message_str.split()
        
        # 权限校验
        if user_id not in self.admin_users:
            yield event.plain_result("⚠️ 您没有执行此操作的权限。")
            return

        # 解析引用的文件
        quoted_file = await self._resolve_file_from_message(event)

        file_to_rename = None
        session = None
        new_name = None

        if quoted_file:
            if len(command_parts) < 2:
                yield event.plain_result("❓ 用法: 引用文件后输入 /重命名 <新文件名>")
                return
            file_to_rename = quoted_file
            new_name = command_parts[1]
            logger.info(f"[/rn] 使用引用文件: {file_to_rename['file_name']} -> {new_name}")
        else:
            if len(command_parts) < 3:
                yield event.plain_result("❓ 用法: /rn <序号> <新文件名> 或 /rn <旧文件名> <新文件名>")
                return
            
            target = command_parts[1]
            new_name = command_parts[2]
            session = self.session_mgr.get_session(group_id, user_id)
            
            # 场景 1: /重命名 <序号> <新名称>
            if target.isdigit():
                if not session:
                    yield event.plain_result("❌ 请先执行 /搜索 搜索文件。")
                    return
                index = int(target)
                if 1 <= index <= session.total_count:
                    file_to_rename = session.results[index - 1]
                else:
                    yield event.plain_result(f"❌ 序号错误！有效范围: 1-{session.total_count}")
                    return
            else:
                # 场景 2: /rn <旧文件名> <新名称>
                all_files = await self._get_all_files_recursive_core(group_id, event.bot)
                found_files = [f for f in all_files if target == f.get('file_name', '')]
                
                if not found_files:
                    # 模糊匹配
                    found_files = [f for f in all_files if target in f.get('file_name', '')]
                
                if not found_files:
                    yield event.plain_result(f"❌ 未找到文件「{target}」。")
                    return
                
                if len(found_files) == 1:
                    file_to_rename = found_files[0]
                else:
                    # 多个结果，更新会话并提示
                    session = self.session_mgr.create_session(group_id, user_id, target, found_files, 20)
                    async for res in self._show_search_page(event, session, 1):
                        yield res
                    yield event.plain_result(f"💡 找到多个匹配文件，请使用 /重命名 <序号> {new_name} 来指定。")
                    return

        if file_to_rename:
            old_name = file_to_rename.get("file_name")
            file_id = file_to_rename.get("file_id")
            parent_id = file_to_rename.get("parent_id", "/") 
            
            success, message = await rename_group_file(
                self.bot, 
                group_id, 
                file_id, 
                parent_id, 
                new_name
            )
            
            if success:
                yield event.plain_result(f"✅ 文件重命名成功：\n「{old_name}」\n  ➡️ 「{new_name}」")
                # 更新会话中的缓存
                if session and file_to_rename in session.results:
                    file_to_rename["file_name"] = new_name
            else:
                yield event.plain_result(f"❌ 重命名失败：{message}")

    async def _resolve_file_from_message(self, event: AstrMessageEvent) -> Optional[Dict]:
        """
        参考 filechecker 逻辑：从消息（尤其是引用消息）中解析并获取云端真实有效的群文件信息。
        """
        group_id = int(event.get_group_id())
        messages = event.get_messages()
        
        file_name_in_msg = None
        msg_file_id = None
        
        for seg in messages:
            if isinstance(seg, Comp.Reply):
                reply_chain = getattr(seg, 'chain', None)
                if reply_chain and isinstance(reply_chain, list):
                    for reply_seg in reply_chain:
                        if isinstance(reply_seg, Comp.File):
                            file_name_in_msg = getattr(reply_seg, "file_name", None) or getattr(reply_seg, "name", None)
                            msg_file_id = getattr(reply_seg, "file_id", None)
                            break
                if file_name_in_msg: break
            elif isinstance(seg, Comp.File):
                # 直接消息中的文件
                file_name_in_msg = getattr(seg, "file_name", None) or getattr(seg, "name", None)
                msg_file_id = getattr(seg, "file_id", None)
                break
                
        if not file_name_in_msg:
            return None
            
        logger.info(f"尝试解析消息中的文件 '{file_name_in_msg}' (ID: {msg_file_id})...")
        all_files = await self._get_all_files_recursive_core(group_id, self.bot)
        
        # 1. 尝试通过 ID 匹配
        if msg_file_id:
            target = next((f for f in all_files if f.get('file_id') == msg_file_id), None)
            if target:
                return target
                
        # 2. 尝试通过文件名全名匹配
        target = next((f for f in all_files if f.get('file_name') == file_name_in_msg), None)
        if target:
            return target
            
        return None

    async def _perform_single_delete(self, event: AstrMessageEvent, file_info: dict, session=None):
        group_id = int(event.get_group_id())
        file_name = file_info.get("file_name")
        file_id = file_info.get("file_id")
        
        if not file_id:
            yield event.plain_result(f"❌ 无法获取文件「{file_name}」的ID。")
            return
            
        try:
            delete_result = await event.bot.api.call_action('delete_group_file', group_id=group_id, file_id=file_id)
            is_success = False
            if delete_result:
                trans_result = delete_result.get('transGroupFileResult', {})
                result_obj = trans_result.get('result', {})
                if result_obj.get('retCode') == 0:
                    is_success = True
            
            if is_success:
                yield event.plain_result(f"✅ 文件「{file_name}」已成功删除。")
                if session and file_info in session.results:
                    session.results.remove(file_info)
                    session.total_count = len(session.results)
            else:
                wording = delete_result.get('wording', 'API未返回成功状态')
                yield event.plain_result(f"❌ 删除文件「{file_name}」失败：{wording}")
        except Exception as e:
            logger.error(f"[{group_id}] 删除文件时出错: {e}", exc_info=True)
            yield event.plain_result(f"❌ 删除文件「{file_name}」时发生内部错误。")

    async def _perform_batch_delete(self, event: AstrMessageEvent, files_to_delete: List[Dict]):
        async for res in perform_batch_delete(event, files_to_delete):
            yield res

    async def _cleanup_folder(self, path: str):
        await cleanup_folder(path)

    async def _get_file_preview(self, event: AstrMessageEvent, file_info: dict, inner_path: str = None) -> tuple[str, str | None]:
        return await get_file_preview(
            int(event.get_group_id()), 
            file_info, 
            event.bot, 
            self.default_zip_password, 
            self.preview_length, 
            self.download_semaphore,
            self._cleanup_folder,
            inner_path
        )

    async def _create_zip_archive(self, source_dir: str, target_zip_path: str, password: str) -> bool:
        return await create_zip_archive(source_dir, target_zip_path, password)

    async def _perform_group_file_backup(self, event: AstrMessageEvent, group_id: int, date_filter_timestamp: Optional[int] = None):
        async for res in perform_group_file_backup(
            event, 
            group_id, 
            self.bot, 
            self.download_semaphore, 
            self.backup_file_size_limit_mb,
            self.backup_file_extensions,
            self.backup_zip_password,
            date_filter_timestamp
        ):
            yield res

    @filter.command("ddf", alias={"重复文件检测","重复群文件检测","群文件查重"})
    async def on_detect_duplicates_command(self, event: AstrMessageEvent):
        """检测群文件中的重复文件（使用LLM分析）"""
        if not self.bot: self.bot = event.bot
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        
        user_id = int(event.get_sender_id())
        if user_id not in self.admin_users:
            yield event.plain_result("⚠️ 您没有执行重复文件检测的权限。")
            return
        
        async for result in detect_duplicates(
            event, 
            self.bot, 
            self.context, 
            self.admin_users, 
            self.group_whitelist, 
            self._get_all_files_recursive_core, 
            self.text_to_image
        ):
            yield result

    @filter.command("gfb", alias={"备份群文件","群文件备份"})
    async def on_group_file_backup_command(self, event: AstrMessageEvent):
        """备份指定群聊或当前群聊的群文件"""
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
                        yield event.plain_result("❌ 日期格式错误。支持格式: YYYY-MM-DD, YYYYMMDD, YYYY/MM/DD\n示例: /gfb 123456 2024-01-01")
                        return
            except ValueError:
                # 可能第一个参数是日期而不是群号
                if group_id_str:
                    target_group_id = int(group_id_str)
                    date_filter_timestamp = utils.parse_date_param(command_parts[1])
                    if date_filter_timestamp is None:
                        yield event.plain_result("❌ 格式错误：请提供有效的群号或日期。\n用法: /gfb [群号] [日期]\n日期格式: YYYY-MM-DD, YYYYMMDD, YYYY/MM/DD")
                        return
                else:
                    yield event.plain_result("❌ 格式错误：请提供有效的群号。用法: /gfb <群号> [日期]")
                    return
        elif group_id_str:
            # 群聊中且没有参数，备份当前群
            target_group_id = int(group_id_str)
        else:
            # 私聊中且没有参数
            yield event.plain_result("❌ 格式错误：在私聊中请指定要备份的群号。\n用法: /gfb <群号> [日期]\n日期格式: YYYY-MM-DD, YYYYMMDD, YYYY/MM/DD")
            return

        logger.info(f"用户 {user_id} 触发 /gfb 备份指令，目标群ID: {target_group_id}, 日期筛选: {command_parts[2] if len(command_parts) > 2 else '无'}")

        # 2. 权限和白名单校验
        if user_id not in self.admin_users:
            yield event.plain_result("⚠️ 您没有执行群文件备份操作的权限。")
            return
        
        if self.group_whitelist and target_group_id not in self.group_whitelist:
            yield event.plain_result("⚠️ 目标群聊不在插件配置的白名单中，操作已拒绝。")
            return

        # 3. 执行备份任务
        async for res in self._perform_group_file_backup(event, target_group_id, date_filter_timestamp):
            yield res

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
