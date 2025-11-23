# astrbot_plugin_GroupFS/utils.py

import datetime
from datetime import datetime as dt
from typing import Optional

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
SUPPORTED_PREVIEW_EXTENSIONS = (
    '.txt', '.md', '.json', '.xml', '.html', '.css', 
    '.js', '.py', '.java', '.c', '.cpp', '.h', '.hpp', 
    '.go', '.rs', '.rb', '.php', '.log', '.ini', '.yml', '.yaml'
)