# astrbot_plugin_GroupFS/utils.py

import datetime
from datetime import datetime as dt
from typing import Optional, List
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api import logger
import astrbot.api.message_components as Comp
from astrbot.api.message_components import Plain, Node, Nodes

# --- 辅助函数：格式化文件大小 ---
def format_bytes(size: int, target_unit=None) -> str:
    if size is None: return "未知大小"
    power = 1024
    n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    if target_unit and target_unit.upper() in power_labels.values():
        target_n = list(power_labels.keys())[list(power_labels.values()).index(target_unit.upper())]
        while n < target_n:
            size /= power
            n += 1
        return f"{size:.2f}"
    while size > power and n < len(power_labels) -1 :
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}"

# --- 辅助函数：格式化时间戳 ---
def format_timestamp(ts: int) -> str:
    if ts is None or ts == 0: return "未知时间"
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')

# --- 辅助函数：解析日期参数 ---
def parse_date_param(date_str: str) -> Optional[int]:
    """
    解析日期参数，支持格式: YYYY-MM-DD, YYYYMMDD, YYYY/MM/DD
    返回时间戳，失败返回 None
    """
    # 尝试多种日期格式
    date_formats = [
        "%Y-%m-%d",
        "%Y%m%d",
        "%Y/%m/%d"
    ]
    
    for fmt in date_formats:
        try:
            parsed_dt = dt.strptime(date_str, fmt)
            return int(parsed_dt.timestamp())
        except ValueError:
            continue
    return None

# --- 常量：定义支持预览的文件扩展名列表 ---
SUPPORTED_TEXT_FORMATS = (
    '.txt', '.md', '.json', '.xml', '.html', '.css', 
    '.js', '.py', '.java', '.c', '.cpp', '.h', '.hpp', 
    '.go', '.rs', '.rb', '.php', '.log', '.ini', '.yml', '.yaml',
    '.toml', '.conf', '.cfg', '.sh', '.bat', '.ps1', '.sql',
    '.csv', '.tsv', '.env', '.dockerfile', '.gitignore'
)

SUPPORTED_ARCHIVE_FORMATS = (
    '.zip', '.7z', '.tar', '.gz', '.bz2', '.xz',
    '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz',
    '.iso', '.wim', '.rar'
)

# --- 辅助函数：将文本按指定长度分割 ---
def split_text_by_length(text: str, max_length: int = 1000) -> list[str]:
    """
    将文本按指定长度分割成一个字符串列表。
    """
    return [text[i:i + max_length] for i in range(0, len(text), max_length)]

# --- 辅助函数：格式化搜索结果 ---
def format_search_results(files: list[dict], search_term: str, for_delete: bool = False) -> str:
    reply_text = f"🔍 找到了 {len(files)} 个与「{search_term}」相关的结果：\n"
    reply_text += "-" * 20
    for i, file_info in enumerate(files, 1):
        reply_text += (
            f"\n[{i}] {file_info.get('file_name')}"
            f"\n  上传者: {file_info.get('uploader_name', '未知')}"
            f"\n  大小: {format_bytes(file_info.get('size'))}"
            f"\n  修改时间: {format_timestamp(file_info.get('modify_time'))}"
        )
    reply_text += "\n" + "-" * 20
    if for_delete:
        reply_text += f"\n请使用 /df {search_term} [序号] 来删除指定文件。"
    else:
        reply_text += f"\n如需删除，请使用 /df {search_term} [序号]"
    return reply_text

# --- 辅助函数：发送普通消息或合并转发消息 ---
async def send_or_forward(event: AstrMessageEvent, text: str, forward_threshold: int, name: str = "GroupFS"):
    total_length = len(text)
    group_id = event.get_group_id()

    if forward_threshold > 0 and total_length > forward_threshold:
        logger.info(f"[{group_id}] 检测到长消息 (长度: {total_length} > {forward_threshold})，准备自动合并转发。")
        try:
            split_texts = split_text_by_length(text, 4000)
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
            
            fallback_text = text[:forward_threshold] + "... (消息过长且合并转发失败)"
            await event.send(MessageChain([Comp.Plain(fallback_text)]))
            logger.info(f"[{group_id}] 合并转发失败，已回退为发送截断的普通消息。")
    else:
        logger.info(f"[{group_id}] 消息长度未达阈值 ({total_length} <= {forward_threshold})，直接发送普通消息。")
        try:
            await event.send(MessageChain([Comp.Plain(text)]))
            logger.info(f"[{group_id}] 成功发送普通消息。")
        except Exception as e:
            logger.error(f"[{group_id}] 发送普通消息时出错: {e}", exc_info=True)