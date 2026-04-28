import asyncio
import os
import posixpath
import re
import shutil
import random
from typing import Dict, List, Optional, Set, Tuple

from astrbot.api import logger
from astrbot.api.star import StarTools
from aiocqhttp.exceptions import ActionFailed

from .file_ops import (
    download_and_save_file,
    get_all_files_with_path,
    rename_group_file,
)


SYNC_DEFAULT_SEPARATOR = "/"


def _normalize_action_payload(result):
    if isinstance(result, dict) and isinstance(result.get("data"), dict):
        return result.get("data", {})
    return result if isinstance(result, dict) else {}


def _is_ob11_action_success(result: Optional[dict]) -> bool:
    return isinstance(result, dict) and result.get("status") == "ok" and result.get("retcode") == 0


def _is_llbot_action_success(result: Optional[dict]) -> bool:
    if result is None:
        return True
    if not isinstance(result, dict):
        return False
    if _is_ob11_action_success(result):
        return True
    if result.get("status") == "failed":
        return False
    return True


def _normalize_relative_path(path: str) -> str:
    if not isinstance(path, str):
        return ""
    return path.replace("\\", SYNC_DEFAULT_SEPARATOR).strip("/ ")


def _parent_path(relative_path: str) -> str:
    return posixpath.dirname(relative_path).strip("/")


def _purify_file_name(file_name: str, rules: Optional[list]) -> str:
    if not file_name or not isinstance(file_name, str):
        return file_name or ""

    if not rules or not isinstance(rules, list):
        return file_name

    result = file_name
    for pattern in rules:
        if not pattern or not isinstance(pattern, str):
            continue
        try:
            result = re.sub(pattern, "", result)
        except re.error as e:
            logger.warning(f"[群文件同步] 无效的文件名净化规则: pattern={pattern}, error={e}")
    return result


def _is_move_success_response(result: Optional[dict]) -> bool:
    # move_group_file 成功响应样例：{'ok': True}
    if result is None:
        return True
    if not isinstance(result, dict):
        return False

    if _is_ob11_action_success(result):
        return True

    ok_flag = result.get("ok")
    if isinstance(ok_flag, bool):
        return ok_flag is True

    if ok_flag in (1, "1", "true", "True"):
        return True

    return False


def _is_upload_success_response(result: Optional[dict]) -> bool:
    # upload_group_file 成功响应样例：{'file_id': '/xxxx'}
    if not isinstance(result, dict):
        return False

    if _is_llbot_action_success(result):
        return True

    file_id = result.get("file_id")
    if isinstance(file_id, str) and file_id:
        return True

    # 兼容某些封装返回 file_id 嵌套的结构
    data = result.get("data")
    if isinstance(data, dict):
        file_id = data.get("file_id")
        if isinstance(file_id, str) and file_id:
            return True

    return False



def _is_delete_success_response(result: Optional[dict]) -> bool:
    # NapCat delete_group_file / delete_group_folder 成功响应兼容
    if result is None:
        return True
    if not isinstance(result, dict):
        return False

    if _is_llbot_action_success(result):
        return True

    # 文件夹/通用返回： {'retCode': 0, 'retMsg': 'ok', 'clientWording': ''}
    if result.get("retCode") is not None:
        if result.get("retCode") != 0:
            return False

        ret_msg = str(result.get("retMsg", "")).strip().lower()
        return ret_msg in ("", "ok")

    # 文件删除返回结构按外层判定：{'result': 0, 'errMsg': 'ok', ...}
    return result.get("result") == 0 and str(result.get("errMsg", "")).strip().lower() == "ok"





def _build_filtered_snapshot(items: List[Dict], cfg: Dict) -> List[Dict]:
    extension_str = str(cfg.get("sync_file_extensions", "")).strip().lower()
    ext_allow = {ext.strip().lstrip(".") for ext in extension_str.split(",") if ext.strip()}

    size_limit_mb = int(cfg.get("sync_file_size_limit_mb", 0) or 0)
    size_limit_bytes = max(0, size_limit_mb) * 1024 * 1024

    purify_rules = cfg.get("purify_rules", [])
    result: List[Dict] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        file_name = item.get("file_name") or ""
        file_size = int(item.get("size", 0) or 0)

        if size_limit_bytes > 0 and file_size > size_limit_bytes:
            continue

        ext = posixpath.splitext(file_name)[1].lower().lstrip(".")
        if ext_allow and ext not in ext_allow:
            continue

        raw_relative_path = _normalize_relative_path(item.get("relative_path") or "")
        parent_dir = _parent_path(raw_relative_path)
        sanitized_name = _purify_file_name(file_name, purify_rules)
        if not sanitized_name:
            sanitized_name = file_name

        effective_path = sanitized_name if not parent_dir else f"{parent_dir}/{sanitized_name}"

        result.append(
            {
                "file_id": item.get("file_id", ""),
                "file_name": file_name,
                "sanitized_name": sanitized_name,
                "size": file_size,
                "modify_time": int(item.get("modify_time", 0) or 0),
                "relative_path": raw_relative_path,
                "parent_path": parent_dir,
                "effective_path": effective_path,
                "parent_id": item.get("parent_id") or "/",
            }
        )

    return result


def _build_orphan_snapshot(items: List[Dict], cfg: Dict) -> List[Dict]:
    purify_rules = cfg.get("purify_rules", [])
    result: List[Dict] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        file_name = item.get("file_name") or ""
        raw_relative_path = _normalize_relative_path(item.get("relative_path") or "")
        parent_dir = _parent_path(raw_relative_path)
        sanitized_name = _purify_file_name(file_name, purify_rules)
        if not sanitized_name:
            sanitized_name = file_name

        effective_path = sanitized_name if not parent_dir else f"{parent_dir}/{sanitized_name}"
        result.append(
            {
                "file_id": item.get("file_id", ""),
                "file_name": file_name,
                "effective_path": effective_path,
            }
        )

    return result


def _build_indexes(records: List[Dict]) -> Tuple[Dict[str, List[Dict]], Dict[Tuple[str, int], List[Dict]], Dict[int, List[Dict]]]:
    path_index: Dict[str, List[Dict]] = {}
    parent_size_index: Dict[Tuple[str, int], List[Dict]] = {}
    size_index: Dict[int, List[Dict]] = {}

    for item in records:
        if not item:
            continue

        path_index.setdefault(item["effective_path"], []).append(item)
        parent_size_index.setdefault((item["parent_path"], item["size"]), []).append(item)
        size_index.setdefault(item["size"], []).append(item)

    for container in (path_index, parent_size_index, size_index):
        for group in container.values():
            group.sort(key=lambda x: (x.get("modify_time", 0), x.get("sanitized_name", "")), reverse=True)

    return path_index, parent_size_index, size_index


def _pop_unused(items: List[Dict], used_ids: Set[str]) -> Optional[Dict]:
    for item in items:
        file_id = item.get("file_id")
        if not file_id or file_id not in used_ids:
            return item
    return None


async def _list_group_folders(bot, group_id: int, parent_id: str, is_llbot: bool = False):
    try:
        if parent_id in ("", "/", None):
            if is_llbot:
                result = await bot.api.call_action("get_group_root_files", group_id=group_id)
            else:
                result = await bot.api.call_action("get_group_root_files", group_id=group_id, file_count=2000)
        else:
            if is_llbot:
                result = await bot.api.call_action(
                    "get_group_files_by_folder",
                    group_id=group_id,
                    folder_id=parent_id,
                )
            else:
                result = await bot.api.call_action(
                    "get_group_files_by_folder",
                    group_id=group_id,
                    folder_id=parent_id,
                    file_count=2000,
                )

        payload = _normalize_action_payload(result)
        if isinstance(payload, dict) and payload.get("folders"):
            return payload.get("folders", [])
    except Exception as e:
        logger.warning(f"[群文件同步] 获取目录失败: group_id={group_id}, parent_id={parent_id}, error={e}")
    return []



async def _list_group_files_by_folder(bot, group_id: int, parent_id: str, is_llbot: bool = False):
    try:
        if parent_id in ("", "/", None):
            if is_llbot:
                result = await bot.api.call_action("get_group_root_files", group_id=group_id)
            else:
                result = await bot.api.call_action("get_group_root_files", group_id=group_id, file_count=2000)
        else:
            if is_llbot:
                result = await bot.api.call_action(
                    "get_group_files_by_folder",
                    group_id=group_id,
                    folder_id=parent_id,
                )
            else:
                result = await bot.api.call_action(
                    "get_group_files_by_folder",
                    group_id=group_id,
                    folder_id=parent_id,
                    file_count=2000,
                )

        payload = _normalize_action_payload(result)
        if isinstance(payload, dict) and payload.get("files"):
            return payload.get("files", [])
    except Exception as e:
        logger.warning(f"[群文件同步] 获取目录文件失败: group_id={group_id}, parent_id={parent_id}, error={e}")
    return []


async def _create_group_folder(bot, group_id: int, folder_name: str, parent_id: Optional[str], is_llbot: bool = False) -> bool:
    """创建群文件夹。"""
    if not folder_name:
        return False

    try:
        if is_llbot:
            result = await bot.api.call_action(
                "create_group_file_folder",
                group_id=group_id,
                name=folder_name,
            )
        else:
            result = await bot.api.call_action(
                "create_group_file_folder",
                group_id=group_id,
                folder_name=folder_name,
            )

        if is_llbot:
            if result is None:
                await asyncio.sleep(1)
                return True
            success = _is_llbot_action_success(result)
            if success:
                await asyncio.sleep(1)
            return success

        if not isinstance(result, dict):
            return False

        inner = result.get("result")
        if not isinstance(inner, dict):
            return False

        success = inner.get("retCode") == 0 and inner.get("retMsg") == "ok"
        if success:
            await asyncio.sleep(1)
        return success
    except Exception as e:
        logger.warning(
            f"[群文件同步] 创建群文件夹失败: group_id={group_id}, folder_name={folder_name}, "
            f"parent_id={parent_id}, error={e}"
        )
        return False


async def _collect_group_folders(bot, group_id: int, is_llbot: bool = False) -> List[Dict]:
    return await _list_group_folders(bot, group_id, "/", is_llbot=is_llbot)


async def _delete_group_folder(bot, group_id: int, folder_id: str, is_llbot: bool = False) -> bool:
    try:
        if is_llbot:
            result = await bot.api.call_action("delete_group_folder", group_id=group_id, folder_id=folder_id)
            if _is_llbot_action_success(result):
                return True
        else:
            result = await bot.api.call_action("delete_group_folder", group_id=group_id, folder_id=folder_id, folder=folder_id)

        if _is_delete_success_response(result):
            return True

        logger.warning(
            f"[群文件同步] 删除文件夹响应判定为失败: group_id={group_id}, folder_id={folder_id}, result={result}"
        )
        return False
    except Exception as e:
        logger.warning(f"[群文件同步] 删除文件夹失败: group_id={group_id}, folder_id={folder_id}, error={e}")
        return False


async def _cleanup_orphan_folders(bot, target_group_id: int, source_folder_names: Set[str], target_folders: List[Dict], is_llbot: bool = False) -> Tuple[int, int]:
    if not target_folders:
        return 0, 0

    deleted = 0
    failed = 0

    source_folder_names = {name.strip() for name in source_folder_names if isinstance(name, str)}
    orphan_folders = [
        folder
        for folder in target_folders
        if isinstance(folder, dict)
        and folder.get("folder_id")
        and folder.get("folder_name") and folder.get("folder_name") not in source_folder_names
    ]

    for folder in orphan_folders:
        folder_id = folder.get("folder_id")
        folder_name = folder.get("folder_name")
        if await _list_group_files_by_folder(bot, target_group_id, folder_id, is_llbot=is_llbot):
            continue

        if await _delete_group_folder(bot, target_group_id, folder_id, is_llbot=is_llbot):
            logger.info(f"[群文件同步] 已清理目标端空目录: {folder_name}")
            deleted += 1
            continue

        failed += 1

    return deleted, failed


async def _resolve_folder_id(
    bot,
    group_id: int,
    parent_path: str,
    folder_cache: Dict[str, str],
    is_llbot: bool = False,
) -> Optional[str]:
    parent_path = _normalize_relative_path(parent_path)
    if not parent_path:
        return "/"

    current_parent_id = "/"
    path_parts: List[str] = []

    for segment in [part for part in parent_path.split("/") if part]:
        path_parts.append(segment)
        current_key = "/".join(path_parts)

        cached_id = folder_cache.get(current_key)
        if cached_id:
            current_parent_id = cached_id
            continue

        folder_list = await _list_group_folders(bot, group_id, current_parent_id, is_llbot=is_llbot)
        target_folder_id = None
        for folder in folder_list:
            if folder.get("folder_name") == segment:
                target_folder_id = folder.get("folder_id")
                break

        if target_folder_id is None:
            created = await _create_group_folder(bot, group_id, segment, current_parent_id, is_llbot=is_llbot)
            if not created:
                return None

            folder_list = await _list_group_folders(bot, group_id, current_parent_id, is_llbot=is_llbot)
            for folder in folder_list:
                if folder.get("folder_name") == segment:
                    target_folder_id = folder.get("folder_id")
                    break

        if not target_folder_id:
            logger.error(f"[群文件同步] 无法解析目录ID: group_id={group_id}, path={current_key}")
            return None

        folder_cache[current_key] = target_folder_id
        current_parent_id = target_folder_id

    return current_parent_id


async def _delete_group_file(bot, group_id: int, file_id: str, is_llbot: bool = False) -> bool:
    try:
        result = await bot.api.call_action("delete_group_file", group_id=group_id, file_id=file_id)
        if is_llbot:
            if _is_llbot_action_success(result):
                return True
        if _is_delete_success_response(result):
            return True

        logger.warning(
            f"[群文件同步] 删除响应判定为失败: group_id={group_id}, file_id={file_id}, result={result}"
        )
        return False
    except Exception as e:
        logger.warning(f"[群文件同步] 删除文件失败: group_id={group_id}, file_id={file_id}, error={e}")
        return False


async def _move_group_file(bot, group_id: int, file_id: str, source_parent: str, target_parent: str, is_llbot: bool = False) -> bool:
    try:
        if is_llbot:
            result = await bot.api.call_action(
                "move_group_file",
                group_id=group_id,
                file_id=file_id,
                parent_directory=source_parent,
                target_directory=target_parent,
            )
        else:
            result = await bot.api.call_action(
                "move_group_file",
                group_id=group_id,
                file_id=file_id,
                current_parent_directory=source_parent,
                target_parent_directory=target_parent,
            )
        if is_llbot and _is_llbot_action_success(result):
            return True
        if _is_move_success_response(result):
            return True

        logger.warning(
            f"[群文件同步] 移动响应判定为失败: group_id={group_id}, file_id={file_id}, result={result}"
        )
        return False
    except Exception as e:
        logger.warning(
            f"[群文件同步] 移动文件失败: group_id={group_id}, file_id={file_id}, "
            f"source_parent={source_parent}, target_parent={target_parent}, error={e}"
        )
        return False


async def _upload_group_file(bot, group_id: int, local_path: str, file_name: str, folder_id: Optional[str], is_llbot: bool = False) -> bool:
    try:
        if is_llbot:
            result = await bot.api.call_action(
                "upload_group_file",
                group_id=group_id,
                file=f"file://{local_path}",
                name=file_name,
                folder_id=folder_id,
            )
        else:
            result = await bot.api.call_action(
                "upload_group_file",
                group_id=group_id,
                file=f"file://{local_path}",
                name=file_name,
                folder=folder_id,
                folder_id=folder_id,
                timeout=300,
            )
        if is_llbot and result is None:
            return True
        if is_llbot and _is_llbot_action_success(result):
            return True
        if _is_upload_success_response(result):
            return True

        logger.warning(
            f"[群文件同步] 上传响应判定为失败: group_id={group_id}, file_name={file_name}, "
            f"is_success={_is_upload_success_response(result)}"
        )
        return False
    except ActionFailed as e:
        logger.warning(f"[群文件同步] 上传文件异常: group_id={group_id}, file_name={file_name}, error={e}")
        return False
    except Exception as e:
        logger.warning(f"[群文件同步] 上传文件失败: group_id={group_id}, file_name={file_name}, error={e}")
        return False


async def _download_to_temp(bot, source_group_id: int, source_item: Dict, temp_root: str, semaphore: asyncio.Semaphore, is_llbot: bool = False) -> Tuple[bool, Optional[str]]:
    file_id = source_item.get("file_id")
    file_name = source_item.get("file_name") or ""
    file_size = int(source_item.get("size", 0) or 0)
    relative_path = source_item.get("relative_path") or ""

    local_path = os.path.join(temp_root, relative_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    ok = await download_and_save_file(
        source_group_id,
        file_id,
        file_name,
        file_size,
        relative_path,
        temp_root,
        bot,
        semaphore,
        is_llbot=is_llbot,
    )
    return ok, local_path if ok else None


async def _cleanup_temp_path(temp_root: str) -> None:
    try:
        if os.path.exists(temp_root):
            shutil.rmtree(temp_root)
    except OSError as e:
        logger.warning(f"[群文件同步] 临时目录清理失败: {temp_root}, error={e}")


async def _validate_source_snapshot(bot, group_id: int, file_records: List[Dict], sample_size: int = 2, is_llbot: bool = False) -> bool:
    if not isinstance(file_records, list) or not file_records:
        return True

    sample_size = max(0, min(sample_size, len(file_records)))
    if sample_size <= 0:
        return True

    candidates = random.sample(file_records, sample_size)
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue

        file_id = candidate.get("file_id")
        file_name = candidate.get("file_name", "")
        if not file_id:
            continue

        try:
            url_result = await bot.api.call_action("get_group_file_url", group_id=group_id, file_id=file_id)
            payload = _normalize_action_payload(url_result)
            if not (isinstance(payload, dict) and payload.get("url")):
                logger.warning(
                    f"[群文件同步] 源群快照抽检失败（无法获取下载链接）: source_group={group_id}, file_id={file_id}, file_name={file_name}"
                )
                return False
        except Exception as e:
            logger.warning(
                f"[群文件同步] 源群快照抽检失败: source_group={group_id}, file_id={file_id}, file_name={file_name}, error={e}"
            )
            return False

    return True


async def _list_group_files_with_retry(bot, group_id: int, retry: int = 1, is_llbot: bool = False) -> Optional[List[Dict]]:
    for idx in range(retry + 1):
        try:
            return await get_all_files_with_path(group_id, bot, is_llbot=is_llbot)
        except ActionFailed as e:
            if idx >= retry:
                logger.error(f"[群文件同步] 获取群文件列表失败: group_id={group_id}, error={e}")
                return None
            logger.warning(f"[群文件同步] 获取群文件列表失败，将重试: source={group_id}, attempt={idx + 1}, error={e}")
            await asyncio.sleep(1)
        except Exception as e:
            if idx >= retry:
                logger.error(f"[群文件同步] 获取群文件列表失败: group_id={group_id}, error={e}")
                return None
            logger.warning(f"[群文件同步] 获取群文件列表失败，将重试: source={group_id}, attempt={idx + 1}, error={e}")
            await asyncio.sleep(1)

    return []


def format_sync_report(stats: Dict) -> str:
    duration_ms = stats.get('duration_ms', 0)
    duration_seconds = (duration_ms + 999) // 1000
    if duration_seconds >= 60:
        minutes, seconds = divmod(duration_seconds, 60)
        duration_text = f"{minutes}分{seconds}秒"
    else:
        duration_text = f"{duration_seconds}秒"

    action_items = []
    uploaded = stats.get('uploaded', 0)
    renamed = stats.get('renamed', 0)
    moved = stats.get('moved', 0)
    updated = stats.get('updated', 0)
    deleted = stats.get('deleted', 0)
    failed = stats.get('failed', 0)

    if uploaded > 0:
        action_items.append(f"新增: {uploaded}")
    if renamed > 0:
        action_items.append(f"重命名: {renamed}")
    if moved > 0:
        action_items.append(f"移动: {moved}")
    if updated > 0:
        action_items.append(f"更新: {updated}")
    if deleted > 0:
        action_items.append(f"删除: {deleted}")
    if failed > 0:
        action_items.append(f"失败: {failed}")

    if not action_items:
        action_text = "本次无操作变更"
    else:
        action_text = "，".join(action_items)

    return "\n".join(
        [
            "🔁 群文件同步完成",
            action_text,
            f"耗时: {duration_text}",
        ]
    )


async def perform_group_file_sync(
    bot,
    source_group_id: int,
    target_group_id: int,
    sync_cfg: Dict,
    download_semaphore: asyncio.Semaphore,
    is_llbot: bool = False,
) -> Dict:
    source_group_id = int(source_group_id)
    target_group_id = int(target_group_id)

    stats = {
        "source_total": 0,
        "source_filtered": 0,
        "uploaded": 0,
        "renamed": 0,
        "moved": 0,
        "updated": 0,
        "skipped": 0,
        "deleted": 0,
        "failed": 0,
        "duration_ms": 0,
    }

    start_ts = asyncio.get_event_loop().time()
    temp_root = os.path.join(
        StarTools.get_data_dir("astrbot_plugin_GroupFS"),
        "temp_sync_cache",
        f"sync_{source_group_id}_{target_group_id}_{int(start_ts * 1000)}",
    )
    os.makedirs(temp_root, exist_ok=True)

    folder_cache: Dict[str, str] = {}
    used_target_ids: Set[str] = set()

    try:
        source_files = await _list_group_files_with_retry(bot, source_group_id, is_llbot=is_llbot)
        target_files = await _list_group_files_with_retry(bot, target_group_id, is_llbot=is_llbot)
        if source_files is None or target_files is None:
            logger.error(
                f"[群文件同步] 获取源/目标文件列表失败，终止本次同步，避免基于不完整快照误删："
                f"source_files_present={source_files is not None}, target_files_present={target_files is not None}"
            )
            stats["failed"] += 1
            return stats

        # 只对源群快照做一次抽检：抽到不存在即认为本次快照不可信，重拉一次后继续
        if not await _validate_source_snapshot(bot, source_group_id, source_files, sample_size=2, is_llbot=is_llbot):
            logger.warning(
                f"[群文件同步] 源群快照抽检失败，重拉源群文件列表: source={source_group_id}, target={target_group_id}"
            )
            source_files = await _list_group_files_with_retry(bot, source_group_id, is_llbot=is_llbot)
            if source_files is None or not await _validate_source_snapshot(
                bot,
                source_group_id,
                source_files,
                sample_size=2,
                is_llbot=is_llbot,
            ):
                logger.error(
                    f"[群文件同步] 源群快照重试后仍异常，终止本次同步（跳过删除）: source={source_group_id}, target={target_group_id}"
                )
                stats["failed"] += 1
                return stats

        source_records = _build_filtered_snapshot(source_files, sync_cfg)
        target_records = _build_filtered_snapshot(target_files, sync_cfg)
        orphan_records = _build_orphan_snapshot(target_files, sync_cfg)

        source_folders = await _collect_group_folders(bot, source_group_id, is_llbot=is_llbot)
        target_folders = await _collect_group_folders(bot, target_group_id, is_llbot=is_llbot)
        source_folder_names = {
            folder.get("folder_name")
            for folder in source_folders
            if folder.get("folder_name")
        }
        if not source_folder_names:
            for item in source_files:
                if not isinstance(item, dict):
                    continue

                raw_relative_path = _normalize_relative_path(item.get("relative_path") or "")
                parent_dir = _parent_path(raw_relative_path)
                if parent_dir:
                    source_folder_names.add(parent_dir.split("/")[0])

        stats["source_total"] = len(source_files)
        stats["source_filtered"] = len(source_records)

        target_path_index, target_parent_size_index, target_size_index = _build_indexes(target_records)

        # 目标端全量快照：用于严格判断“本次源侧有效文件”之外的文件是否应删除
        source_sorted = sorted(source_records, key=lambda item: (item["modify_time"], item["effective_path"]))

        for source_item in source_sorted:
            source_effective_path = source_item["effective_path"]
            source_parent_path = source_item["parent_path"]
            source_size = source_item["size"]
            source_sanitized_name = source_item["sanitized_name"]
            source_file_name = source_item["file_name"]
            source_file_id = source_item.get("file_id", "")

            target_parent_id = await _resolve_folder_id(
                bot,
                target_group_id,
                source_parent_path,
                folder_cache,
                is_llbot=is_llbot,
            )
            if not target_parent_id:
                logger.warning(
                    f"[群文件同步] 无法解析目标目录，跳过源文件: source={source_file_id}, source_name={source_file_name}, source_parent_path={source_parent_path}, target_group={target_group_id}"
                )
                stats["failed"] += 1
                continue

            path_candidates = target_path_index.get(source_effective_path, [])
            target_hit = _pop_unused(path_candidates, used_target_ids)
            if target_hit:
                target_file_id = target_hit.get("file_id") or ""

                if target_file_id:
                    used_target_ids.add(target_file_id)

                if target_hit.get("size", 0) == source_size:
                    stats["skipped"] += 1
                    continue

                if target_file_id:
                    delete_ok = await _delete_group_file(bot, target_group_id, target_file_id, is_llbot=is_llbot)
                    if not delete_ok:
                        logger.warning(
                            f"[群文件同步] 同路径覆盖前删除旧文件失败: target_group={target_group_id}, file_id={target_file_id}"
                        )
                        stats["failed"] += 1
                        continue

                download_ok, local_path = await _download_to_temp(
                    bot,
                    source_group_id,
                    source_item,
                    temp_root,
                    download_semaphore,
                    is_llbot=is_llbot,
                )
                if not download_ok or not local_path:
                    logger.warning(
                        f"[群文件同步] 下载源文件到临时目录失败: source_id={source_file_id}, path={source_effective_path}"
                    )
                    stats["failed"] += 1
                    continue

                upload_ok = await _upload_group_file(
                    bot,
                    target_group_id,
                    local_path,
                    source_sanitized_name,
                    target_parent_id,
                    is_llbot=is_llbot,
                )
                if upload_ok:
                    stats["updated"] += 1
                    if (stats["updated"] + stats["uploaded"]) % 50 == 0:
                        logger.info(f"[群文件同步] 已上传 {stats['updated'] + stats['uploaded']} 个文件")
                else:
                    logger.warning(
                        f"[群文件同步] 覆盖上传失败: target_group={target_group_id}, source_id={source_file_id}, target_path={source_effective_path}"
                    )
                    stats["failed"] += 1
                continue

            # 2. 同目录同体积（重命名）
            parent_size_candidates = target_parent_size_index.get((source_parent_path, source_size), [])
            rename_target = _pop_unused(parent_size_candidates, used_target_ids)
            if rename_target:
                rename_target_id = rename_target.get("file_id")
                rename_parent_id = rename_target.get("parent_id") or "/"
                if rename_target_id:
                    used_target_ids.add(rename_target_id)

                if rename_target.get("sanitized_name") != source_sanitized_name:
                    rename_ok, message = await rename_group_file(
                        bot,
                        target_group_id,
                        rename_target_id,
                        rename_parent_id,
                        source_sanitized_name,
                        is_llbot=is_llbot,
                    )
                    if not rename_ok:
                        logger.warning(
                            f"[群文件同步] 同目录同体积重命名失败: source={source_group_id}:{source_file_name} -> {source_sanitized_name}, detail={message}"
                        )
                        stats["failed"] += 1
                        continue

                    stats["renamed"] += 1
                    continue

                stats["skipped"] += 1
                continue

            # 3. 同体积命中（跨目录移动/重命名）
            size_candidates = target_size_index.get(source_size, [])
            move_target = _pop_unused(size_candidates, used_target_ids)
            if move_target:
                move_target_id = move_target.get("file_id") or ""
                if move_target_id:
                    used_target_ids.add(move_target_id)

                source_parent_dir = move_target.get("parent_id") or "/"
                target_new_parent_id = await _resolve_folder_id(
                    bot,
                    target_group_id,
                    source_parent_path,
                    folder_cache,
                    is_llbot=is_llbot,
                )
                if not target_new_parent_id:
                    stats["failed"] += 1
                    continue

                move_ok = False
                if move_target_id:
                    move_ok = await _move_group_file(
                        bot,
                        target_group_id,
                        move_target_id,
                        source_parent_dir,
                        target_new_parent_id,
                        is_llbot=is_llbot,
                    )

                if move_ok:
                    stats["moved"] += 1
                    if move_target.get("sanitized_name") == source_sanitized_name:
                        stats["skipped"] += 1
                    

                if move_ok and move_target.get("sanitized_name") != source_sanitized_name:
                    rename_ok, message = await rename_group_file(
                        bot,
                        target_group_id,
                        move_target_id,
                        target_new_parent_id,
                        source_sanitized_name,
                        is_llbot=is_llbot,
                    )
                    if not rename_ok:
                        logger.warning(
                            f"[群文件同步] 跨目录移动后重命名失败: target_group={target_group_id}, file_id={move_target_id}, detail={message}"
                        )
                        stats["failed"] += 1
                        continue

                    stats["renamed"] += 1
                    continue

                if move_ok:
                    # move 成功且名称一致：已按 moved/skipped 处理
                    continue

                stats["failed"] += 1
                continue

            # 4. 新增文件
            download_ok, local_path = await _download_to_temp(
                bot,
                source_group_id,
                source_item,
                temp_root,
                download_semaphore,
                is_llbot=is_llbot,
            )
            if not download_ok or not local_path:
                stats["failed"] += 1
                continue

            upload_ok = await _upload_group_file(
                bot,
                target_group_id,
                local_path,
                source_sanitized_name,
                target_parent_id,
                is_llbot=is_llbot,
            )
            if upload_ok:
                stats["uploaded"] += 1
                if (stats["uploaded"] + stats["updated"]) % 50 == 0:
                    logger.info(f"[群文件同步] 已上传 {stats['uploaded'] + stats['updated']} 个文件")
            else:
                stats["failed"] += 1

        for target_item in orphan_records:
            target_effective_path = target_item.get("effective_path")
            file_id = target_item.get("file_id")
            target_file_name = target_item.get("file_name")
            if not file_id:
                logger.warning(
                    f"[群文件同步] 跳过无 file_id 的目标项: file_name={target_item.get('file_name')}, effective_path={target_effective_path}"
                )
                continue

            if file_id in used_target_ids:
                continue
            if await _delete_group_file(bot, target_group_id, file_id, is_llbot=is_llbot):
                logger.info(
                    f"[群文件同步] 已删除目标端文件: target_group={target_group_id}, "
                    f"file_id={file_id}, file_name={target_file_name}, effective_path={target_effective_path}"
                )
                stats["deleted"] += 1
            else:
                stats["failed"] += 1

        deleted_folders, failed_folders = await _cleanup_orphan_folders(
            bot,
            target_group_id,
            source_folder_names,
            target_folders,
            is_llbot=is_llbot,
        )
        stats["deleted"] += deleted_folders
        stats["failed"] += failed_folders

        stats["duration_ms"] = int((asyncio.get_event_loop().time() - start_ts) * 1000)
        return stats

    except Exception as e:
        logger.error(f"[群文件同步] 执行异常: source={source_group_id}, target={target_group_id}, error={e}", exc_info=True)
        stats["failed"] += 1
        return stats
    finally:
        asyncio.create_task(_cleanup_temp_path(temp_root))
