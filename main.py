import asyncio
import os
from typing import List, Dict, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import croniter

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.api import logger
import astrbot.api.message_components as Comp

from .src import utils
from .src.file_ops import (
    get_all_files_with_path,
    download_and_save_file,
    create_zip_archive,
    cleanup_folder,
    cleanup_backup_temp,
    get_all_files_recursive_core,
    rename_group_file,
)
from .src.preview_utils import get_file_preview
from .src.actions import (
    perform_scheduled_check,
    perform_batch_check_and_delete,
    perform_batch_delete,
    check_storage_and_notify
)
from .src.utils import send_report_message
from .src.backup import perform_group_file_backup
from .src.duplicate_check import detect_duplicates
from .src.session_manager import SessionManager
from .src.sync_service import perform_group_file_sync, format_sync_report


class GroupFSPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config if config else {}
        self.admin_users: List[int] = [int(u) for u in self.config.get("admin_users", [])]
        self.storage_limits: Dict[int, Dict] = {}
        self.cron_configs = []
        self.bot = None
        self._is_llbot = False
        self.scheduler: Optional[AsyncIOScheduler] = None

        self.active_tasks = []

        preview_cfg_list = self.config.get("preview_module", [])
        backup_cfg_list = self.config.get("backup_module", [])
        check_cfg_list = self.config.get("check_module", [])
        sync_cfg_list = []
        if isinstance(backup_cfg_list, list):
            for item in backup_cfg_list:
                if not isinstance(item, dict):
                    continue
                template_key = str(item.get("__template_key") or item.get("template") or "").strip()
                if template_key == "sync":
                    sync_cfg_list.append(item)
            
        self._backup_default = {
            "backup_zip_password": "",
            "backup_file_size_limit_mb": 100,
            "backup_file_extensions": "txt,zip",
        }
        self._sync_default = {
            "sync_file_extensions": "",
            "sync_file_size_limit_mb": 0,
            "purify_rules": [],
        }
        self._preview_default = {
            "preview_length": 1000,
            "default_zip_password": "",
            "pdf_preview_pages": 1,
        }
        self.preview_configs = self._build_group_configs(
            preview_cfg_list, self._preview_default
        )
        self.backup_configs = self._build_group_configs(
            backup_cfg_list, self._backup_default
        )

        self.download_semaphore = asyncio.Semaphore(5)

        self.sync_configs = []
        self.sync_cron_configs = []

        if not isinstance(check_cfg_list, list):
            check_cfg_list = []

        for item in check_cfg_list:
            if not isinstance(item, dict):
                continue
            group_id = str(item.get("group_id", "")).strip()
            if not group_id:
                continue
            try:
                group_id = int(group_id)
            except (TypeError, ValueError):
                logger.error(f"检查配置中群号无效: {group_id}，已跳过。")
                continue

            count_limit = item.get("count_limit", 0)
            space_limit_gb = item.get("space_limit_gb", 0)
            if count_limit > 0 or space_limit_gb > 0:
                self.storage_limits[group_id] = {
                    "count_limit": int(count_limit),
                    "space_limit_gb": float(space_limit_gb),
                }

            cron_list = item.get("scheduled_check_tasks") or []
            auto_delete = bool(item.get("scheduled_autodelete", False))
            if isinstance(cron_list, list):
                for cron_str in cron_list:
                    if not isinstance(cron_str, str) or not cron_str.strip():
                        continue
                    if not croniter.croniter.is_valid(cron_str):
                        logger.error(f"无效的 cron 表达式: {cron_str}，已跳过。")
                        continue
                    self.cron_configs.append(
                        {
                            "group_id": group_id,
                            "cron_str": cron_str,
                            "auto_delete": auto_delete,
                        }
                    )

        if not isinstance(sync_cfg_list, list):
            sync_cfg_list = []

        for item in sync_cfg_list:
            if not isinstance(item, dict):
                logger.warning(f"[群文件同步] 跳过无效 sync 项: item类型={type(item).__name__}")
                continue

            source_group_id = str(item.get("source_group_id", "")).strip()
            target_group_id = str(item.get("target_group_id", "")).strip()
            if not source_group_id or not target_group_id:
                logger.error(f"[群文件同步] 该 sync 配置缺少 source_group_id/target_group_id，已跳过。当前项: {item}")
                continue

            try:
                source_group_id_int = int(source_group_id)
                target_group_id_int = int(target_group_id)
            except (TypeError, ValueError):
                logger.error(f"[群文件同步] 同步配置群号无效: {source_group_id}->{target_group_id}，已跳过。")
                continue

            sync_cfg = self._sync_default.copy()
            for k, v in item.items():
                if k in ("source_group_id", "target_group_id"):
                    continue
                if v is not None:
                    sync_cfg[k] = v

            sync_cfg["source_group_id"] = source_group_id_int
            sync_cfg["target_group_id"] = target_group_id_int
            sync_cfg["purify_rules"] = [
                rule for rule in sync_cfg.get("purify_rules", []) if isinstance(rule, str)
            ] if isinstance(sync_cfg.get("purify_rules", []), list) else []

            self.sync_configs.append(sync_cfg)

            cron_list = item.get("scheduled_sync_tasks") or []
            if isinstance(cron_list, list):
                for cron_str in cron_list:
                    if not isinstance(cron_str, str) or not cron_str.strip():
                        continue
                    if not croniter.croniter.is_valid(cron_str):
                        logger.error(f"[群文件同步] 无效的 cron 表达式: {cron_str}，已跳过。")
                        continue
                    self.sync_cron_configs.append(
                        {
                            "source_group_id": source_group_id_int,
                            "target_group_id": target_group_id_int,
                            "cron_str": cron_str,
                        }
                    )

        # 搜索会话管理
        self.search_cache_timeout = 600 # 10分钟
        self.search_results_per_page = 20
        self.session_mgr = SessionManager(timeout=self.search_cache_timeout)

        if self.cron_configs:
            seen_tasks = set()
            unique_configs = []
            for cfg in self.cron_configs:
                task_identifier = (cfg.get("group_id"), cfg.get("cron_str"))
                if task_identifier in seen_tasks:
                    logger.warning(f"检测到重复的定时任务配置 '{task_identifier}'，已跳过。")
                    continue
                seen_tasks.add(task_identifier)
                unique_configs.append(cfg)
            self.cron_configs = unique_configs

        if self.sync_cron_configs:
            seen_sync_tasks = set()
            unique_sync_configs = []
            for cfg in self.sync_cron_configs:
                task_identifier = (cfg.get("source_group_id"), cfg.get("target_group_id"), cfg.get("cron_str"))
                if task_identifier in seen_sync_tasks:
                    logger.warning(f"检测到重复的同步定时任务配置 '{task_identifier}'，已跳过。")
                    continue
                seen_sync_tasks.add(task_identifier)
                unique_sync_configs.append(cfg)
            self.sync_cron_configs = unique_sync_configs

        logger.info("插件 [群文件系统GroupFS] 已加载。")


    def _build_group_configs(self, raw_list, defaults: Dict) -> List[Dict]:
        if not isinstance(raw_list, list):
            return []
        configs: List[Dict] = []
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            group_id = str(item.get("group_id", "")).strip()
            cfg = defaults.copy()
            for k, v in item.items():
                if k == "group_id":
                    continue
                if v is not None:
                    cfg[k] = v
            cfg["group_id"] = group_id
            configs.append(cfg)
        return configs

    def _get_group_config(self, group_id: int, configs: List[Dict], defaults: Dict):
        gid = str(group_id)
        for cfg in configs:
            if cfg.get("group_id") == gid:
                return cfg
        for cfg in configs:
            if cfg.get("group_id") == "":
                return cfg
        return defaults

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        if event.is_admin():
            return True
        user_id = int(event.get_sender_id())
        return user_id in self.admin_users

    def _is_supported_bot_client(self, client) -> bool:
        return bool(client and hasattr(client, "api") and hasattr(client.api, "call_action"))

    async def _try_bind_bot_from_platform_manager(self) -> bool:
        platform = self.context.get_platform(filter.PlatformAdapterType.AIOCQHTTP)
        if not platform or not hasattr(platform, "get_client"):
            return False
        bot_client = platform.get_client()
        if not self._is_supported_bot_client(bot_client):
            return False
        self.bot = bot_client
        await self._detect_llbot_backend()
        logger.info("[GroupFS] 已绑定 AIOCQHTTP OneBot 客户端")
        return True

    async def _ensure_event_bot_bound(self, event: AstrMessageEvent):
        if self._is_supported_bot_client(self.bot):
            return
        candidate = getattr(event, "bot", None)
        if self._is_supported_bot_client(candidate):
            self.bot = candidate
            await self._detect_llbot_backend()
            logger.info("[GroupFS] 已切换到当前事件来源的 OneBot 客户端")

    async def _detect_llbot_backend(self):
        if not self.bot or not hasattr(self.bot, "api"):
            return

        try:
            version_info = await self.bot.api.call_action("get_version_info")
            app_name = None
            if isinstance(version_info, dict):
                app_name = version_info.get("app_name")
            self._is_llbot = app_name == "LLOneBot"
            logger.info(
                f"[GroupFS] 协议端探测结果: app_name={app_name or 'unknown'}, "
                f"backend={'llbot' if self._is_llbot else 'napcat'}"
            )
        except Exception as e:
            self._is_llbot = False
            logger.warning(f"[GroupFS] 协议端探测失败，默认按 NapCat 处理: {e}")

    async def initialize(self):
        # 尝试从 platform_manager 自动获取 bot 实例
        try:
            await self._try_bind_bot_from_platform_manager()
        except Exception as e:
            logger.warning(f"[GroupFS] 从 platform_manager 绑定 OneBot 客户端失败: {e}")

        has_check_jobs = bool(self.cron_configs)
        has_sync_crons = bool(self.sync_cron_configs)

        if self.bot:
            if has_check_jobs or has_sync_crons:
                logger.info("[定时任务] 启动调度器（检查/同步）...")
                self._start_scheduler()
                self.scheduler.start()
            # 无论是否有定时任务，若有同步配置都进行一次启动同步
            if self.sync_configs:
                self._schedule_startup_sync()
            return

        if has_check_jobs or has_sync_crons or self.sync_configs:
            logger.warning("[初始化] 未能立即获取 bot 实例，将通过延迟流程补获 bot 并启动同步/检查任务。")
            asyncio.create_task(self._delayed_start_scheduler())

    async def _delayed_start_scheduler(self):
        """延迟启动调度器，等待系统初始化并尝试再次获取 bot"""
        try:
            # 等待 30 秒让系统完全初始化
            await asyncio.sleep(30)

            # 再次尝试获取 bot
            if not self.bot and hasattr(self.context, "platform_manager"):
                await self._try_bind_bot_from_platform_manager()

            if self.bot and (self.cron_configs or self.sync_cron_configs):
                if not self.scheduler:
                    logger.info("[定时任务] 延迟启动调度器...")
                    self._start_scheduler()
                    self.scheduler.start()
                elif not self.scheduler.running:
                    self.scheduler.start()

            if self.sync_configs:
                self._schedule_startup_sync()

            if not self.bot:
                logger.warning("[定时任务] 尚未获取到 bot 实例，同步/检查任务未启动。")

        except Exception as e:
            logger.error(f"[定时任务] 延迟启动失败: {e}", exc_info=True)

    def _start_scheduler(self):
        if self.scheduler:
            return

        self.scheduler = AsyncIOScheduler()
        self._register_jobs()

    def _schedule_startup_sync(self):
        if getattr(self, "_startup_sync_scheduled", False):
            return

        if not self.sync_configs:
            return

        if not self.bot:
            logger.warning("[群文件同步] 未获取到 bot 实例，延迟启动一次性同步。")
            return

        self._startup_sync_scheduled = True
        task = asyncio.create_task(self._run_startup_sync())
        self.active_tasks.append(task)

    async def _run_startup_sync(self):
        for sync_cfg in self.sync_configs:
            try:
                await self._run_group_sync_job(
                    sync_cfg["source_group_id"],
                    sync_cfg["target_group_id"],
                    sync_cfg,
                    is_startup=True,
                )
            except Exception as e:
                logger.error(
                    f"[群文件同步] 启动同步任务异常: source={sync_cfg.get('source_group_id')} -> target={sync_cfg.get('target_group_id')}, error={e}",
                    exc_info=True,
                )

    def _register_jobs(self):
        """根据配置注册定时任务"""
        for job_config in self.cron_configs:
            group_id = job_config["group_id"]
            cron_str = job_config["cron_str"]
            auto_delete = bool(job_config.get("auto_delete", False))
            job_id = f"gfs_check_{group_id}_{cron_str.replace(' ', '_')}"

            if self.scheduler.get_job(job_id):
                logger.warning(f"任务 {job_id} 已存在，跳过注册。")
                continue

            try:
                cron_parts = cron_str.split()
                minute, hour, day, month, day_of_week = cron_parts

                self.scheduler.add_job(
                    self._apscheduler_check_task,
                    "cron",
                    args=[group_id, auto_delete],
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                    id=job_id
                )
                logger.info(f"成功注册检查任务: group_id={group_id}, cron_str='{cron_str}'")
            except Exception as e:
                logger.error(f"注册检查任务 '{cron_str}' 失败: {e}", exc_info=True)

        for job_config in self.sync_cron_configs:
            source_group_id = int(job_config["source_group_id"])
            target_group_id = int(job_config["target_group_id"])
            cron_str = job_config["cron_str"]
            job_id = f"gfs_sync_{source_group_id}_{target_group_id}_{cron_str.replace(' ', '_')}"

            if self.scheduler.get_job(job_id):
                logger.warning(f"任务 {job_id} 已存在，跳过注册。")
                continue

            try:
                cron_parts = cron_str.split()
                minute, hour, day, month, day_of_week = cron_parts

                sync_cfg = self._find_sync_cfg(source_group_id, target_group_id)
                self.scheduler.add_job(
                    self._apscheduler_sync_task,
                    "cron",
                    args=[source_group_id, target_group_id, sync_cfg],
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                    id=job_id
                )
                logger.info(f"成功注册同步任务: source={source_group_id}, target={target_group_id}, cron_str='{cron_str}'")
            except Exception as e:
                logger.error(f"注册同步任务 '{cron_str}' 失败: {e}", exc_info=True)

    def _find_sync_cfg(self, source_group_id: int, target_group_id: int) -> Dict:
        for cfg in self.sync_configs:
            if cfg.get("source_group_id") == source_group_id and cfg.get("target_group_id") == target_group_id:
                return cfg
        return self._sync_default.copy()

    async def _apscheduler_check_task(self, group_id: int, auto_delete: bool):
        """APScheduler 调用的包装函数，负责消费生成器并发送消息"""
        if not self.bot:
            logger.warning(f"[{group_id}] [定时任务] 无法执行，因为尚未捕获到 bot 实例。")
            return

        async for msg in perform_scheduled_check(group_id, auto_delete, self.bot, self.storage_limits, is_llbot=self._is_llbot):
            if self.bot:
                node_name = "定时清理报告" if auto_delete else "定时检查报告"
                await send_report_message(
                    self.bot,
                    group_id,
                    msg,
                    threshold=100,
                    node_name=node_name,
                )

    async def _apscheduler_sync_task(self, source_group_id: int, target_group_id: int, sync_cfg: Dict):
        await self._run_group_sync_job(source_group_id, target_group_id, sync_cfg)

    async def _run_group_sync_job(self, source_group_id: int, target_group_id: int, sync_cfg: Dict, is_startup: bool = False):
        if not self.bot:
            logger.warning(
                f"[群文件同步] 无法执行同步任务: source={source_group_id}, target={target_group_id}，尚未捕获 bot 实例。"
            )
            return

        cfg = self._sync_default.copy()
        if isinstance(sync_cfg, dict):
            for k, v in sync_cfg.items():
                cfg[k] = v

        if cfg.get("source_group_id") is None:
            cfg["source_group_id"] = source_group_id
        if cfg.get("target_group_id") is None:
            cfg["target_group_id"] = target_group_id

        logger.info(f"[群文件同步] 开始执行同步任务: source={source_group_id}, target={target_group_id}")

        stats = await perform_group_file_sync(
            self.bot,
            source_group_id,
            target_group_id,
            cfg,
            self.download_semaphore,
            is_llbot=self._is_llbot,
        )
        report = format_sync_report(stats)

        logger.info(
            f"[群文件同步] 同步完成: source={source_group_id}, target={target_group_id}, "
            f"uploaded={stats.get('uploaded')}, renamed={stats.get('renamed')}, moved={stats.get('moved')}, updated={stats.get('updated')}, "
            f"skipped={stats.get('skipped')}, deleted={stats.get('deleted')}, failed={stats.get('failed')}"
        )

        has_changes = any(int(stats.get(field, 0) > 0) for field in ("uploaded", "renamed", "moved", "updated", "deleted"))
        if not has_changes and int(stats.get("failed", 0)) <= 0:
            return

        await send_report_message(
            self.bot,
            target_group_id,
            report,
            threshold=100,
            node_name="群文件同步启动" if is_startup else "群文件同步",
        )

    async def _perform_scheduled_check(self, group_id: int, auto_delete: bool):
        async for res in perform_scheduled_check(group_id, auto_delete, self.bot, self.storage_limits, is_llbot=self._is_llbot):
            yield res


    async def _get_all_files_with_path(self, group_id: int, bot) -> List[Dict]:
        return await get_all_files_with_path(group_id, bot, is_llbot=self._is_llbot)
    
    async def _get_all_files_recursive_core(self, group_id: int, bot) -> List[Dict]:
        """
        兼容 /cdf, /cf, /sf, /df 等指令。
        """
        return await get_all_files_recursive_core(group_id, bot, is_llbot=self._is_llbot)
    
    async def _download_and_save_file(self, group_id: int, file_id: str, file_name: str, file_size: int, relative_path: str, root_dir: str, client) -> bool:
        return await download_and_save_file(group_id, file_id, file_name, file_size, relative_path, root_dir, client, self.download_semaphore, is_llbot=self._is_llbot)

    async def _cleanup_backup_temp(self, backup_dir: str, zip_path: Optional[str]):
        await cleanup_backup_temp(backup_dir, zip_path)

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("cdf", alias={"清理失效文件","清理失效群文件"})
    async def on_check_and_delete_command(self, event: AstrMessageEvent):
        """扫描并自动删除所有失效文件"""
        await self._ensure_event_bot_bound(event)
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        group_id = int(group_id_str)
        user_id = int(event.get_sender_id())
        logger.info(f"[{group_id}] 用户 {user_id} 触发 /cdf 失效文件清理指令。")
        if not self._is_admin(event):
            yield event.plain_result("⚠️ 您没有执行此操作的权限。")
            return
        yield event.plain_result("⚠️ 警告：即将开始扫描并自动删除所有失效文件！\n此过程可能需要几分钟，请耐心等待，完成后将发送报告。")
        async for res in perform_batch_check_and_delete(event, is_llbot=self._is_llbot):
            yield res

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("cf", alias={"检查群文件"})
    async def on_check_files_command(self, event: AstrMessageEvent):
        """扫描并查找群内的失效文件（仅检查不删除）"""
        await self._ensure_event_bot_bound(event)
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        group_id = int(group_id_str)
        user_id = int(event.get_sender_id())
        if not self._is_admin(event):
            yield event.plain_result("⚠️ 您没有执行此操作的权限。")
            return
        logger.info(f"[{group_id}] 用户 {user_id} 触发 /cf 失效文件检查指令。")
        yield event.plain_result("已开始扫描群内所有文件，查找失效文件...\n这可能需要几分钟，请耐心等待。")
        has_report = False
        async for res in self._perform_scheduled_check(group_id, False):
            has_report = True
            if isinstance(res, str):
                yield event.plain_result(res)
            else:
                yield res
        if not has_report:
            yield event.plain_result("✅ 检查完成，未发现失效文件。")
    
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=10)
    async def on_group_file_upload(self, event: AstrMessageEvent):
        """监控群文件上传事件"""
        await self._ensure_event_bot_bound(event)
        file_component = next((seg for seg in event.get_messages() if isinstance(seg, Comp.File)), None)
        if file_component:
            group_id = int(event.get_group_id())
            
            # 仅在配置了容量检查的群组中触发
            if group_id not in self.storage_limits:
                return

            file_name = getattr(file_component, 'file_name', None) or getattr(file_component, 'name', None)
            
            logger.info(f"[{group_id}] 检测到文件上传事件: {file_name}，将触发容量检查。")
            await asyncio.sleep(1)
            async for res in self._check_storage_and_notify(event):
                yield res

    async def _check_storage_and_notify(self, event: AstrMessageEvent):
        async for res in check_storage_and_notify(event, self.storage_limits, is_llbot=self._is_llbot):
            yield res
    
    def _format_search_results(self, files: List[Dict], search_term: str, for_delete: bool = False) -> str:
        return utils.format_search_results(files, search_term, for_delete)
    
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("sf", alias={"搜索"})
    async def on_search_file_command(self, event: AstrMessageEvent):
        """搜索群文件"""
        await self._ensure_event_bot_bound(event)
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
        
        page_hint = f" (第 {page}/{total_pages} 页)" if total_pages > 1 else ""
        reply_text = f"🔍 找到了 {session.total_count} 个与「{session.keyword}」相关的结果{page_hint}：\n"
        reply_text += "-" * 20
        
        start_idx = (page - 1) * session.page_size + 1
        for i, file_info in enumerate(results, start_idx):
            file_name = file_info.get('file_name', '未知文件')
            emoji = utils.get_file_emoji(file_name)
            parent_folder = file_info.get('parent_folder_name', '根目录')
            path_display = f"\n  路径: /{parent_folder}" if parent_folder != '根目录' else ""
            
            reply_text += (
                f"\n[{i}] {emoji} {file_name}"
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
        if total_pages > 1:
            reply_text += f"\n输入 /sf 跳转 <页码> 跳转"
        reply_text += f"\n\n💡 快速操作："
        reply_text += f"\n• /预览 <序号> - 预览文件"
        reply_text += f"\n• /删除 <序号> - 删除文件"
        
        yield event.plain_result(reply_text)

    async def _handle_preview(self, event: AstrMessageEvent, file_to_preview: dict, inner_path: str = None):
        group_id = int(event.get_group_id())
        try:
            preview_res, error_msg = await self._get_file_preview(event, file_to_preview, inner_path)
            if error_msg:
                yield event.plain_result(error_msg)
                return
            
            title = f"📄 预览: {file_to_preview.get('file_name')}"
            if inner_path:
                title += f" ({inner_path})"
            
            if isinstance(preview_res, list):
                # PDF 预览，发送合并消息
                image_paths = preview_res
                sender_id = event.get_self_id()
                nodes = []
                
                # 第一条消息是标题
                nodes.append(Comp.Node(uin=sender_id, name="PDF 预览", content=[Comp.Plain(f"{title}")]))
                
                for img_path in image_paths:
                    nodes.append(Comp.Node(uin=sender_id, name="PDF 预览", content=[Comp.Image.fromFileSystem(img_path)]))
                
                yield event.chain_result([Comp.Nodes(nodes=nodes)])
                
                # 延迟清理图片
                async def cleanup_images(paths):
                    await asyncio.sleep(60)
                    for p in paths:
                        try:
                            if os.path.exists(p): os.remove(p)
                        except: pass
                asyncio.create_task(cleanup_images(image_paths))
                return

            reply_text = (
                f"{title}\n"
                + "-" * 20 + "\n"
                + preview_res
            )
            yield event.plain_result(reply_text)
        except Exception as e:
            logger.error(f"[{group_id}] 处理预览时发生未知异常: {e}", exc_info=True)
            yield event.plain_result("❌ 预览文件时发生内部错误，请检查后台日志。")

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("preview", alias={"预览"})
    async def on_preview_command(self, event: AstrMessageEvent):
        """预览群文件内容"""
        await self._ensure_event_bot_bound(event)
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

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("df", alias={"删除"})
    async def on_delete_file_command(self, event: AstrMessageEvent):
        """删除指定的群文件"""
        await self._ensure_event_bot_bound(event)
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        group_id = int(group_id_str)
        user_id = int(event.get_sender_id())
        command_parts = event.message_str.split()
        
        if not self._is_admin(event):
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

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("rn", alias={"重命名"})
    async def on_rename_command(self, event: AstrMessageEvent):
        """重命名指定的群文件"""
        await self._ensure_event_bot_bound(event)
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        group_id = int(group_id_str)
        user_id = int(event.get_sender_id())
        command_parts = event.message_str.split()
        
        # 权限校验
        if not self._is_admin(event):
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
                new_name,
                is_llbot=self._is_llbot,
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
            if self._is_llbot:
                if delete_result is None:
                    is_success = True
                elif isinstance(delete_result, dict) and delete_result.get("status") != "failed":
                    is_success = True
            elif delete_result:
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
        async for res in perform_batch_delete(event, files_to_delete, is_llbot=self._is_llbot):
            yield res

    async def _cleanup_folder(self, path: str):
        await cleanup_folder(path)

    async def _get_file_preview(self, event: AstrMessageEvent, file_info: dict, inner_path: str = None) -> tuple[str, str | None]:
        group_id = int(event.get_group_id())
        preview_cfg = self._get_group_config(
            group_id, self.preview_configs, self._preview_default
        )
        return await get_file_preview(
            group_id,
            file_info, 
            event.bot, 
            preview_cfg.get("default_zip_password", ""),
            int(preview_cfg.get("preview_length", 1000)),
            self.download_semaphore,
            self._cleanup_folder,
            inner_path,
            max(1, int(preview_cfg.get("pdf_preview_pages", 1)))
        )

    async def _create_zip_archive(self, source_dir: str, target_zip_path: str, password: str) -> bool:
        return await create_zip_archive(source_dir, target_zip_path, password)

    async def _perform_group_file_backup(self, event: AstrMessageEvent, group_id: int, date_filter_timestamp: Optional[int] = None):
        backup_cfg = self._get_group_config(
            group_id, self.backup_configs, self._backup_default
        )
        ext_str = str(backup_cfg.get("backup_file_extensions", "txt,zip"))
        backup_file_extensions = [
            ext.strip().lstrip('.').lower()
            for ext in ext_str.split(',')
            if ext.strip()
        ]
        async for res in perform_group_file_backup(
            event, 
            group_id, 
            self.bot, 
            self.download_semaphore, 
            int(backup_cfg.get("backup_file_size_limit_mb", 100)),
            backup_file_extensions,
            str(backup_cfg.get("backup_zip_password", "")),
            date_filter_timestamp,
            is_llbot=self._is_llbot,
        ):
            yield res

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("ddf", alias={"重复文件检测","重复群文件检测","群文件查重"})
    async def on_detect_duplicates_command(self, event: AstrMessageEvent):
        """检测群文件中的重复文件（使用LLM分析）"""
        await self._ensure_event_bot_bound(event)
        group_id_str = event.get_group_id()
        if not group_id_str:
            yield event.plain_result("❌ 此指令只能在群聊中使用。")
            return
        
        if not self._is_admin(event):
            yield event.plain_result("⚠️ 您没有执行重复文件检测的权限。")
            return
        
        async for result in detect_duplicates(
            event, 
            self.bot, 
            self.context, 
            self._get_all_files_recursive_core, 
            self.text_to_image
        ):
            yield result

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("gfb", alias={"备份群文件","群文件备份"})
    async def on_group_file_backup_command(self, event: AstrMessageEvent):
        """备份指定群聊或当前群聊的群文件"""
        await self._ensure_event_bot_bound(event)
        
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

        # 2. 权限校验
        if not self._is_admin(event):
            yield event.plain_result("⚠️ 您没有执行群文件备份操作的权限。")
            return

        # 3. 执行备份任务
        async for res in self._perform_group_file_backup(event, target_group_id, date_filter_timestamp):
            yield res

    async def terminate(self):
        logger.info("插件 [QQ群文件管家] 正在卸载，取消所有任务...")

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
        
        logger.info("插件 [QQ 群文件管家] 已卸载。")
