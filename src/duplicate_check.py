import os
import asyncio
from datetime import datetime
from typing import List, Dict, Optional, Any
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent
from . import utils

async def detect_duplicates(
    event: AstrMessageEvent, 
    bot, 
    context: Any,
    admin_users: List[int],
    group_whitelist: List[int],
    get_all_files_fn, # 传入获取全量文件的函数
    text_to_image_fn # 传入文字转图片的函数
):
    """检测群文件中的重复文件（使用LLM分析）"""
    user_id = int(event.get_sender_id())
    group_id = event.get_group_id()
    
    if not group_id:
        yield event.plain_result("❌ 此指令仅可在群聊中使用。")
        return
    
    # 白名单校验
    if group_whitelist and int(group_id) not in group_whitelist:
        yield event.plain_result("⚠️ 当前群聊不在插件配置的白名单中。")
        return
    
    try:
        # 获取所有文件信息
        all_files = await get_all_files_fn(int(group_id), bot)
        
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
        provider_id = await context.get_current_chat_provider_id(umo=umo)
        
        if not provider_id:
            yield event.plain_result("❌ 未配置聊天模型，无法使用此功能。")
            return
        
        yield event.plain_result("🤖 正在调用AI检测重复文件，这可能需要一些时间...")
        
        # 调用LLM
        llm_resp = await context.llm_generate(
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
            image_url = await text_to_image_fn(response_text)
            yield event.image_result(image_url)
        except Exception as img_error:
            logger.warning(f"文本转图片失败: {img_error}，将发送纯文本结果")
            # 如果转图失败，降级为发送文本
            yield event.plain_result(response_text)
        
    except Exception as e:
        logger.error(f"检测重复文件失败: {e}", exc_info=True)
        yield event.plain_result(f"❌ 检测过程中发生错误: {str(e)}")
