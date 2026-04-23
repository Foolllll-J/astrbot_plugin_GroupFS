from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent

from . import utils


REPORT_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <style>
    :root {
      --bg: #eef3fb;
      --card: #ffffff;
      --line: #dbe5f2;
      --text: #1f2937;
      --muted: #6b7280;
      --head-a: #1d4ed8;
      --head-b: #0f766e;
      --ok-bg: #dcfce7;
      --ok-text: #166534;
      --pending-bg: #fef3c7;
      --pending-text: #92400e;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      padding: 28px;
      background:
        radial-gradient(circle at top left, rgba(59, 130, 246, 0.10), transparent 32%),
        linear-gradient(180deg, #f7fbff 0%, var(--bg) 100%);
      color: var(--text);
      font-family: "Microsoft YaHei", "PingFang SC", "Noto Sans CJK SC", sans-serif;
    }

    .panel {
      max-width: 1360px;
      margin: 0 auto;
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 22px;
      overflow: hidden;
      box-shadow: 0 22px 70px rgba(15, 23, 42, 0.12);
    }

    .header {
      padding: 26px 30px 22px;
      background: linear-gradient(135deg, var(--head-a), var(--head-b));
      color: #ffffff;
    }

    .header-title {
      font-size: 30px;
      font-weight: 800;
      letter-spacing: 0.02em;
      margin: 0;
    }

    .header-subtitle {
      margin-top: 8px;
      font-size: 15px;
      line-height: 1.6;
      opacity: 0.94;
    }

    .content {
      padding: 22px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      font-size: 18px;
    }

    thead th {
      text-align: left;
      padding: 16px 18px;
      background: #eef6ff;
      color: #18417d;
      border-bottom: 2px solid #c8daf5;
      font-size: 16px;
      font-weight: 800;
    }

    tbody td {
      vertical-align: top;
      padding: 18px;
      border-bottom: 1px solid var(--line);
    }

    tbody tr:nth-child(even) {
      background: #fbfdff;
    }

    .status-badge {
      display: inline-block;
      padding: 7px 13px;
      border-radius: 999px;
      font-size: 15px;
      font-weight: 800;
      line-height: 1;
      white-space: nowrap;
    }

    .status-ok {
      background: var(--ok-bg);
      color: var(--ok-text);
    }

    .status-pending {
      background: var(--pending-bg);
      color: var(--pending-text);
    }

    .file-name {
      font-size: 18px;
      font-weight: 700;
      line-height: 1.6;
      word-break: break-word;
    }

    .meta {
      margin-top: 10px;
      color: var(--muted);
      font-size: 15px;
      line-height: 1.8;
    }

    .meta-line {
      display: block;
    }

    .footer {
      padding: 14px 24px 22px;
      color: var(--muted);
      font-size: 13px;
    }
  </style>
</head>
<body>
  <div class="panel">
    <div class="header">
      <div class="header-title">重复文件检测报告</div>
      <div class="header-subtitle">
        下表按旧版本文件时间从早到晚展示候选结果，便于直接判断是否需要清理。
      </div>
    </div>
    <div class="content">
      <table>
        <thead>
          <tr>
            <th style="width: 16%;">状态</th>
            <th style="width: 42%;">旧版本文件</th>
            <th style="width: 42%;">新版本文件</th>
          </tr>
        </thead>
        <tbody>
          {% for row in rows %}
          <tr>
            <td>
              <span class="status-badge {% if row.status_class == 'ok' %}status-ok{% else %}status-pending{% endif %}">
                {{ row.status }}
              </span>
            </td>
            <td>
              <div class="file-name">{{ row.old.name }}</div>
              <div class="meta">
                <span class="meta-line">修改时间：{{ row.old.date }}</span>
                <span class="meta-line">文件大小：{{ row.old.size }}</span>
              </div>
            </td>
            <td>
              <div class="file-name">{{ row.new.name }}</div>
              <div class="meta">
                <span class="meta-line">修改时间：{{ row.new.date }}</span>
                <span class="meta-line">文件大小：{{ row.new.size }}</span>
              </div>
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
    <div class="footer">
      第 {{ page }} / {{ total_pages }} 页 · Rendered by GroupFS custom HTML template.
    </div>
  </div>
</body>
</html>
"""

COMPACT_REPORT_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <style>
    body {
      margin: 0;
      padding: 20px;
      background: #f8fafc;
      color: #172033;
      font-family: "Microsoft YaHei", "PingFang SC", "Noto Sans CJK SC", sans-serif;
    }

    .panel {
      max-width: 1600px;
      margin: 0 auto;
      background: #ffffff;
      border: 1px solid #d7e0ec;
      border-radius: 14px;
      overflow: hidden;
      box-shadow: 0 14px 42px rgba(15, 23, 42, 0.10);
    }

    .header {
      padding: 16px 20px;
      background: #172033;
      color: #ffffff;
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 16px;
    }

    .title {
      font-size: 22px;
      font-weight: 800;
    }

    .summary {
      color: #cbd5e1;
      font-size: 13px;
      white-space: nowrap;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      font-size: 13px;
    }

    th {
      padding: 9px 10px;
      background: #edf2f7;
      color: #24324a;
      border-bottom: 1px solid #cbd5e1;
      text-align: left;
      font-weight: 800;
    }

    td {
      padding: 8px 10px;
      border-bottom: 1px solid #e2e8f0;
      vertical-align: top;
      line-height: 1.45;
      word-break: break-word;
    }

    tr:nth-child(even) {
      background: #fbfdff;
    }

    .status {
      display: inline-block;
      padding: 3px 7px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 800;
      white-space: nowrap;
    }

    .status-ok {
      color: #166534;
      background: #dcfce7;
    }

    .status-pending {
      color: #92400e;
      background: #fef3c7;
    }

    .name {
      font-weight: 700;
    }

    .meta {
      margin-top: 3px;
      color: #64748b;
      font-size: 12px;
    }

    .footer {
      padding: 10px 16px 14px;
      color: #64748b;
      font-size: 12px;
      background: #f8fafc;
    }
  </style>
</head>
<body>
  <div class="panel">
    <div class="header">
      <div class="title">重复文件检测报告 · 紧凑视图</div>
      <div class="summary">共 {{ total_rows }} 条 · 第 {{ page }} / {{ total_pages }} 页</div>
    </div>
    <table>
      <thead>
        <tr>
          <th style="width: 5%;">#</th>
          <th style="width: 10%;">状态</th>
          <th style="width: 42.5%;">旧版本文件</th>
          <th style="width: 42.5%;">新版本文件</th>
        </tr>
      </thead>
      <tbody>
        {% for row in rows %}
        <tr>
          <td>{{ row.index }}</td>
          <td>
            <span class="status {% if row.status_class == 'ok' %}status-ok{% else %}status-pending{% endif %}">
              {{ row.status }}
            </span>
          </td>
          <td>
            <div class="name">{{ row.old.name }}</div>
            <div class="meta">{{ row.old.date }} · {{ row.old.size }}</div>
          </td>
          <td>
            <div class="name">{{ row.new.name }}</div>
            <div class="meta">{{ row.new.date }} · {{ row.new.size }}</div>
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    <div class="footer">结果较多时自动启用紧凑表格视图，按页展示全部候选项。</div>
  </div>
</body>
</html>
"""

COMFORT_ROWS_PER_PAGE = 12
COMPACT_MODE_THRESHOLD = 60
COMPACT_ROWS_PER_PAGE = 36


def _normalize_text(value: Any, default: str = "未知") -> str:
    text = str(value).strip() if value is not None else ""
    return text or default


def _normalize_file_info(data: Any) -> dict[str, str]:
    if not isinstance(data, dict):
        return {"name": "未知文件", "date": "未知", "size": "未知"}
    return {
        "name": _normalize_text(data.get("name"), "未知文件"),
        "date": _normalize_text(data.get("date"), "未知"),
        "size": _normalize_text(data.get("size"), "未知"),
    }


def _normalize_rows(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []

    normalized: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        status = _normalize_text(item.get("status"), "待定")
        normalized.append(
            {
                "status": status,
                "status_class": "ok" if "删除" in status else "pending",
                "old": _normalize_file_info(item.get("old")),
                "new": _normalize_file_info(item.get("new")),
            }
        )
    return normalized


def _strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped

    lines = stripped.splitlines()
    if not lines:
        return stripped
    if lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _extract_json_candidate(text: str) -> str | None:
    stripped = _strip_code_fence(text)
    if stripped.startswith("{") or stripped.startswith("["):
        return stripped

    match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
    if match:
        candidate = match.group(1).strip()
        if candidate.startswith("{") or candidate.startswith("["):
            return candidate

    start_positions = [pos for pos in (text.find("{"), text.find("[")) if pos != -1]
    if not start_positions:
        return None

    candidate = text[min(start_positions) :].strip()
    if candidate.startswith("{") or candidate.startswith("["):
        return candidate
    return None


def _parse_json_result(text: str) -> list[dict[str, Any]] | None:
    candidate = _extract_json_candidate(text)
    if not candidate:
        return None

    try:
        data = json.loads(candidate)
    except json.JSONDecodeError:
        return None

    if isinstance(data, dict):
        if data.get("result") == "none":
            return []
        rows = data.get("duplicates")
    elif isinstance(data, list):
        rows = data
    else:
        return None

    normalized = _normalize_rows(rows)
    return normalized if normalized else None


def _parse_duplicate_result(text: str) -> list[dict[str, Any]] | None:
    return _parse_json_result(text)


def _extract_html_render_fn(text_to_image_fn):
    plugin_instance = getattr(text_to_image_fn, "__self__", None)
    html_render_fn = getattr(plugin_instance, "html_render", None)
    return html_render_fn if callable(html_render_fn) else None


def _paginate_rows(
    rows: list[dict[str, Any]],
    rows_per_page: int,
) -> list[list[dict[str, Any]]]:
    if not rows:
        return []
    return [
        rows[index : index + rows_per_page]
        for index in range(0, len(rows), rows_per_page)
    ]


def _with_row_indexes(rows: list[dict[str, Any]], offset: int) -> list[dict[str, Any]]:
    indexed_rows = []
    for index, row in enumerate(rows, start=offset + 1):
        copied = dict(row)
        copied["index"] = index
        indexed_rows.append(copied)
    return indexed_rows


def _build_prompt(file_list_str: str) -> str:
    return f"""请仔细分析下面的群文件列表，找出所有疑似重复的小说文件（同一本书的不同版本）。

请优先根据以下标准判断是否重复：
1. 文件名高度相似，去掉版本号、日期、后缀后主体名称一致。
2. 同一作者的同一作品。
3. 如果可以明显判断旧版本和新版本，则状态填“可删除”。
4. 如果只能判断可能重复，但无法确认该删哪一个，则状态填“待定”。

输出要求：
1. 只输出 JSON，不要输出 Markdown，不要输出解释文字。
2. JSON 格式固定如下：
{{
  "result": "ok",
  "duplicates": [
    {{
      "status": "可删除",
      "old": {{
        "name": "旧版本文件名",
        "date": "2026-04-08",
        "size": "1.68 MB"
      }},
      "new": {{
        "name": "新版本文件名",
        "date": "2026-04-21",
        "size": "2.15 MB"
      }}
    }}
  ]
}}
3. 如果没有发现重复文件，输出：
{{"result":"none","duplicates":[]}}
4. duplicates 中每一项只保留 3 个字段：status、old、new。
5. old 和 new 中都必须包含 name、date、size 三个字段。
6. 按 old.date 从早到晚排序。

下面是文件列表：

{file_list_str}"""


async def detect_duplicates(
    event: AstrMessageEvent,
    bot,
    context: Any,
    get_all_files_fn,
    text_to_image_fn,
):
    """检测群文件中的重复文件，优先使用结构化 JSON 结果并自定义渲染图片。"""
    group_id = event.get_group_id()

    if not group_id:
        yield event.plain_result("❌ 此指令仅可在群聊中使用。")
        return

    try:
        all_files = await get_all_files_fn(int(group_id), bot)

        if not all_files:
            yield event.plain_result("❌ 未找到任何文件。")
            return

        file_list_parts = []
        for file_info in all_files:
            file_name = file_info.get("file_name", "未知")
            file_size = file_info.get("size", 0)
            file_time = file_info.get("modify_time", 0)

            try:
                time_str = datetime.fromtimestamp(file_time).strftime("%Y-%m-%d")
            except Exception:
                time_str = "未知"

            size_str = utils.format_bytes(file_size)
            file_list_parts.append(f"- {file_name} | {time_str} | {size_str}")

        prompt = _build_prompt("\n".join(file_list_parts))

        umo = event.unified_msg_origin
        provider_id = await context.get_current_chat_provider_id(umo=umo)

        if not provider_id:
            yield event.plain_result("❌ 未配置聊天模型，无法使用此功能。")
            return

        yield event.plain_result("🤖 正在调用 AI 检测重复文件，这可能需要一些时间...")

        llm_resp = await context.llm_generate(
            chat_provider_id=provider_id,
            prompt=prompt,
        )

        if not llm_resp or not llm_resp.completion_text:
            yield event.plain_result("❌ AI 分析失败，请稍后重试。")
            return

        response_text = llm_resp.completion_text.strip()
        logger.info(f"[重复文件检测] AI响应结果：\n{response_text}")

        parsed_rows = _parse_duplicate_result(response_text)
        if parsed_rows == []:
            yield event.plain_result("✅ 未检测到重复文件")
            return

        try:
            html_render_fn = _extract_html_render_fn(text_to_image_fn)
            if html_render_fn and parsed_rows:
                total_rows = len(parsed_rows)
                use_compact_mode = total_rows > COMPACT_MODE_THRESHOLD
                rows_per_page = (
                    COMPACT_ROWS_PER_PAGE if use_compact_mode else COMFORT_ROWS_PER_PAGE
                )
                total_rows = len(parsed_rows)
                template = COMPACT_REPORT_TEMPLATE if use_compact_mode else REPORT_TEMPLATE

                if use_compact_mode:
                    image_url = await html_render_fn(
                        template,
                        {
                            "rows": _with_row_indexes(parsed_rows, 0),
                            "page": 1,
                            "total_pages": 1,
                            "total_rows": total_rows,
                        },
                        options={"full_page": True, "type": "jpeg", "quality": 85},
                    )
                    yield event.image_result(image_url)
                    return

                paged_rows = _paginate_rows(parsed_rows, rows_per_page)
                for page_index, page_rows in enumerate(paged_rows, start=1):
                    image_url = await html_render_fn(
                        template,
                        {
                            "rows": _with_row_indexes(
                                page_rows,
                                (page_index - 1) * rows_per_page,
                            ),
                            "page": page_index,
                            "total_pages": len(paged_rows),
                            "total_rows": total_rows,
                        },
                        options={"full_page": True, "type": "jpeg", "quality": 85},
                    )
                    yield event.image_result(image_url)
                return

            if "未检测到重复文件" in response_text:
                yield event.plain_result("✅ 未检测到重复文件")
                return

            image_url = await text_to_image_fn(response_text)
            yield event.image_result(image_url)
        except Exception as img_error:
            logger.warning(f"文本转图片失败: {img_error}，将发送纯文本结果")
            if parsed_rows == [] or "未检测到重复文件" in response_text:
                yield event.plain_result("✅ 未检测到重复文件")
            else:
                yield event.plain_result(response_text)

    except Exception as e:
        logger.error(f"检测重复文件失败: {e}", exc_info=True)
        yield event.plain_result(f"❌ 检测过程中发生错误: {str(e)}")
