import asyncio
from typing import List, Dict
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent
from aiocqhttp.exceptions import ActionFailed
from . import utils
from .file_ops import get_all_files_recursive_core, is_group_file_invalid


def _is_ob11_action_success(result) -> bool:
    return isinstance(result, dict) and result.get("status") == "ok" and result.get("retcode") == 0


def _is_llbot_action_success(result) -> bool:
    if result is None:
        return True
    if not isinstance(result, dict):
        return False
    if _is_ob11_action_success(result):
        return True
    if result.get("status") == "failed":
        return False
    return True

async def perform_scheduled_check(group_id: int, auto_delete: bool, bot, storage_limits: Dict[int, Dict], is_llbot: bool = False):
    """统一的定时检查函数，根据auto_delete决定是否删除。"""
    log_prefix = "[清理任务]" if auto_delete else "[检查任务]"
    report_title = "清理报告" if auto_delete else "检查报告"
    
    try:
        if not bot:
            logger.warning(f"[{group_id}] {log_prefix} 无法执行，因为尚未捕获到 bot 实例。")
            return
        
        logger.info(f"[{group_id}] {log_prefix} 开始获取全量文件列表...")
        all_files = await get_all_files_recursive_core(group_id, bot, is_llbot=is_llbot)
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
                is_invalid = await is_group_file_invalid(bot, group_id, file_id, is_llbot=is_llbot)
                if is_invalid:
                    invalid_files_info.append(file_info)
                    if auto_delete:
                        logger.warning(f"[{group_id}] {log_prefix} 发现失效文件 '{file_name}'，尝试删除...")
                        try:
                            delete_result = await bot.api.call_action('delete_group_file', group_id=group_id, file_id=file_id)
                            is_success = False
                            if _is_llbot_action_success(delete_result):
                                is_success = True
                            elif delete_result and delete_result.get('transGroupFileResult', {}).get('result', {}).get('retCode') == 0:
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
        if bot:
            yield report_message
    except Exception as e:
        logger.error(f"[{group_id}] {log_prefix} 执行过程中发生未知异常: {e}", exc_info=True)
        if bot:
            yield "❌ 定时任务执行过程中发生内部错误，请检查后台日志。"

async def perform_batch_check_and_delete(event: AstrMessageEvent, is_llbot: bool = False):
    group_id = int(event.get_group_id())
    bot = event.bot
    try:
        logger.info(f"[{group_id}] [批量清理] 开始获取全量文件列表...")
        all_files = await get_all_files_recursive_core(group_id, bot, is_llbot=is_llbot)
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
                is_invalid = await is_group_file_invalid(bot, group_id, file_id, is_llbot=is_llbot)
                if is_invalid:
                    logger.warning(f"[{group_id}] [批量清理] 发现失效文件 '{file_name}'，尝试删除...")
                    try:
                        delete_result = await bot.api.call_action('delete_group_file', group_id=group_id, file_id=file_id)
                        is_success = False
                        if _is_llbot_action_success(delete_result):
                            is_success = True
                        elif delete_result:
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
        yield event.plain_result(report_message)
    except Exception as e:
        logger.error(f"[{group_id}] [批量清理] 执行过程中发生未知异常: {e}", exc_info=True)
        yield event.plain_result("❌ 在执行批量清理时发生内部错误，请检查后台日志。")

async def perform_batch_delete(event: AstrMessageEvent, files_to_delete: List[Dict], is_llbot: bool = False):
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
            if _is_llbot_action_success(delete_result):
                is_success = True
            elif delete_result:
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
    yield event.plain_result(report_message)

async def check_storage_and_notify(event: AstrMessageEvent, storage_limits: Dict[int, Dict], is_llbot: bool = False):
    group_id = int(event.get_group_id())
    if group_id not in storage_limits:
        return
    try:
        client = event.bot
        system_info = await client.api.call_action('get_group_file_system_info', group_id=group_id)
        if isinstance(system_info, dict) and isinstance(system_info.get("data"), dict):
            system_info = system_info["data"]
        if not system_info: return
        file_count = system_info.get('file_count', 0)
        used_space_bytes = system_info.get('used_space', 0)
        used_space_gb = float(utils.format_bytes(used_space_bytes, 'GB'))
        limits = storage_limits[group_id]
        count_limit = limits['count_limit']
        space_limit = limits['space_limit_gb']
        notifications = []
        if count_limit > 0 and file_count >= count_limit:
            notifications.append(f"文件数量已达 {file_count}，超过设定上限 {count_limit}！")
        if space_limit > 0 and used_space_gb >= space_limit:
            notifications.append(f"已用空间已达 {used_space_gb:.2f}GB，超过设定上限 {space_limit:.2f}GB！")
        if notifications:
            full_notification = "⚠️ 群文件容量警告\n" + "\n".join(notifications) + "\n请及时清理文件！"
            logger.warning(f"[{group_id}] 发送容量超限警告: {full_notification}")
            yield event.plain_result(full_notification)
    except ActionFailed as e:
        logger.error(f"[{group_id}] 调用 get_group_file_system_info 失败: {e}")
    except Exception as e:
        logger.error(f"[{group_id}] 处理容量检查时发生未知异常: {e}", exc_info=True)
