# astrbot_plugin_GroupFS/utils.py

import datetime
from datetime import datetime as dt
from typing import Optional, List
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api import logger
import astrbot.api.message_components as Comp

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

SUPPORTED_PDF_FORMATS = (
    '.pdf',
)

# --- 辅助函数：根据文件名获取 Emoji ---
def get_file_emoji(file_name: str) -> str:
    ext = file_name.lower().split('.')[-1] if '.' in file_name else ''
    if ext in ['txt', 'md', 'log', 'ini', 'yml', 'yaml', 'toml', 'conf', 'cfg']:
        return "📄"
    if ext in ['zip', '7z', 'rar', 'tar', 'gz', 'bz2', 'xz', 'iso', 'wim']:
        return "📦"
    if ext == 'pdf':
        return "📕"
    if ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'svg']:
        return "🖼️"
    if ext in ['mp4', 'mkv', 'avi', 'mov', 'wmv', 'flv']:
        return "🎬"
    if ext in ['mp3', 'wav', 'flac', 'ogg', 'm4a']:
        return "🎵"
    if ext in ['exe', 'msi', 'apk', 'app', 'dmg']:
        return "⚙️"
    if ext in ['py', 'js', 'java', 'c', 'cpp', 'go', 'rs', 'php', 'sh', 'bat', 'ps1', 'sql', 'html', 'css']:
        return "💻"
    if ext in ['xls', 'xlsx', 'csv']:
        return "📊"
    if ext in ['doc', 'docx']:
        return "📝"
    if ext in ['ppt', 'pptx']:
        return "📽️"
    return "📁"

# --- 辅助函数：格式化搜索结果 ---
def format_search_results(files: list[dict], search_term: str, for_delete: bool = False) -> str:
    reply_text = f"🔍 找到了 {len(files)} 个与「{search_term}」相关的结果：\n"
    reply_text += "-" * 20
    for i, file_info in enumerate(files, 1):
        file_name = file_info.get('file_name', '未知文件')
        emoji = get_file_emoji(file_name)
        reply_text += (
            f"\n[{i}] {emoji} {file_name}"
            f"\n  上传者: {file_info.get('uploader_name', '未知')}"
            f"\n  大小: {format_bytes(file_info.get('size'))}"
            f"\n  修改时间: {format_timestamp(file_info.get('modify_time'))}"
        )
    reply_text += "\n" + "-" * 20
    if for_delete:
        reply_text += f"\n请使用 /删除 {search_term} [序号] 来删除指定文件。"
    else:
        reply_text += f"\n如需删除，请使用 /删除 {search_term} [序号]"
    return reply_text