import os
import asyncio
import time
import subprocess
import aiohttp
import chardet
import zipfile
import xml.etree.ElementTree as ET
import re
import pypdfium2 as pdfium
from typing import List
from astrbot.api import logger
from astrbot.api.star import StarTools
from aiocqhttp.exceptions import ActionFailed
from . import utils

async def get_preview_from_bytes(content_bytes: bytes) -> tuple[str, str]:
    """从字节内容中尝试获取文本预览和编码。"""
    try:
        detection = chardet.detect(content_bytes)
        encoding = detection.get('encoding', 'utf-8') or 'utf-8'
        if encoding and detection['confidence'] > 0.7:
            decoded_text = content_bytes.decode(encoding, errors='ignore').strip()
            return decoded_text, encoding
        return "", "未知"
    except Exception:
        return "", "未知"

def extract_epub_text(epub_path: str, max_chars: int) -> str:
    if not zipfile.is_zipfile(epub_path):
        return "错误：不是有效的 EPUB 文件（无效的 ZIP 结构）。"
    try:
        with zipfile.ZipFile(epub_path, 'r') as z:
            try:
                container_content = z.read('META-INF/container.xml')
                root = ET.fromstring(container_content)
                ns = {'ns': 'urn:oasis:names:tc:opendocument:xmlns:container'}
                rootfile = root.find('.//ns:rootfile', ns)
                if rootfile is None:
                    return "错误：EPUB 结构异常，未找到 rootfile。"
                opf_path = rootfile.attrib.get('full-path')
            except Exception as e:
                return f"错误：解析 container.xml 失败: {e}"
            if not opf_path:
                return "错误：未找到 OPF 文件路径。"
            try:
                opf_content = z.read(opf_path)
                opf_dir = os.path.dirname(opf_path)
                root = ET.fromstring(opf_content)
                ns = {'opf': 'http://www.idpf.org/2007/opf'}
                manifest = {}
                for item in root.findall('.//opf:manifest/opf:item', ns):
                    item_id = item.attrib.get('id')
                    href = item.attrib.get('href')
                    if item_id and href:
                        manifest[item_id] = href
                spine_items = []
                for itemref in root.findall('.//opf:spine/opf:itemref', ns):
                    idref = itemref.attrib.get('idref')
                    if idref in manifest:
                        full_href = os.path.join(opf_dir, manifest[idref]).replace('\\', '/')
                        spine_items.append(full_href)
            except Exception as e:
                return f"错误：解析 OPF 文件失败: {e}"
            full_text = []
            current_len = 0
            re_scripts = re.compile(r'<(script|style).*?>.*?</\1>', re.DOTALL | re.IGNORECASE)
            re_block_tags = re.compile(r'<(p|div|br|li|h[1-6]|tr|blockquote|section|article).*?>', re.IGNORECASE)
            re_tags = re.compile(r'<[^>]+>')
            re_spaces = re.compile(r'[ \t\f\v]+')
            re_newlines = re.compile(r'\n{3,}')
            for item_path in spine_items:
                if current_len >= max_chars * 2:
                    break
                try:
                    html_content = z.read(item_path).decode('utf-8', errors='ignore')
                    text = re_scripts.sub('', html_content)
                    text = text.replace('\r', ' ').replace('\n', ' ')
                    text = re_block_tags.sub('\n', text)
                    text = re_tags.sub('', text)
                    text = text.replace('&nbsp;', ' ').replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&').replace('&quot;', '"')
                    text = re_spaces.sub(' ', text)
                    lines = []
                    for line in text.split('\n'):
                        stripped = line.strip()
                        if stripped:
                            lines.append(stripped)
                    text = '\n'.join(lines)
                    if text:
                        full_text.append(text)
                        current_len += len(text)
                except Exception:
                    continue
            result = "\n\n".join(full_text)
            result = re_newlines.sub('\n\n', result).strip()
            if not result:
                return "（未提取到有效文本内容）"
            if len(result) > max_chars:
                return result[:max_chars + 1]
            return result
    except Exception as e:
        logger.error(f"提取 EPUB 文本时出错: {e}", exc_info=True)
        return f"错误：提取失败: {e}"

async def get_preview_from_archive(file_path: str, default_zip_password: str, preview_length: int, cleanup_fn, inner_target: str = None) -> tuple[str, str]:
    """从本地压缩文件中解压并预览最合适的文本文件。支持多种格式。"""
    temp_dir = os.path.join(StarTools.get_data_dir('astrbot_plugin_GroupFS'), 'temp_file_previews')
    os.makedirs(temp_dir, exist_ok=True)
    extract_path = os.path.join(temp_dir, f"extract_{int(time.time())}")
    os.makedirs(extract_path, exist_ok=True)
    
    preview_text = ""
    error_msg = None
    
    try:
        # 1. 解压文件
        logger.info(f"正在尝试无密码解压文件 '{os.path.basename(file_path)}'...")
        command_no_pwd = ["7za", "x", file_path, f"-o{extract_path}", "-y"]
        # 如果有指定内部路径（非序号），只解压该文件以提高速度
        if inner_target and not inner_target.isdigit():
             command_no_pwd.append(inner_target)

        process = await asyncio.create_subprocess_exec(
            *command_no_pwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            if default_zip_password:
                logger.info("无密码解压失败，正在尝试使用默认密码...")
                command_with_pwd = ["7za", "x", file_path, f"-o{extract_path}", f"-p{default_zip_password}", "-y"]
                if inner_target and not inner_target.isdigit():
                    command_with_pwd.append(inner_target)
                process = await asyncio.create_subprocess_exec(
                    *command_with_pwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    error_msg = stderr.decode('utf-8', errors='ignore').strip()
                    logger.error(f"使用默认密码解压失败: {error_msg}")
                    error_msg = "解压失败，可能密码不正确"
            else:
                error_msg = stderr.decode('utf-8', errors='ignore').strip()
                logger.error(f"使用 7za 命令解压失败且未设置默认密码: {error_msg}")
                error_msg = "解压失败，可能文件已加密"
        
        if error_msg:
            return "", error_msg

        all_extracted_files = []
        for dirpath, _, filenames in os.walk(extract_path):
            for f in filenames:
                f_p = os.path.join(dirpath, f)
                all_extracted_files.append(f_p)
        
        # 排序以保证序号稳定
        all_extracted_files.sort()
        
        if not all_extracted_files:
            if inner_target:
                return "", f"压缩包内未找到文件: {inner_target}"
            return "", "压缩包为空或解压失败"

        # 2. 寻找预览文件
        preview_file_path = None
        
        if inner_target:
            if inner_target.isdigit():
                # 按序号预览
                idx = int(inner_target)
                if 1 <= idx <= len(all_extracted_files):
                    preview_file_path = all_extracted_files[idx-1]
                    if not preview_file_path.lower().endswith(utils.SUPPORTED_TEXT_FORMATS):
                        return "", f"❌ 内部文件 #{idx} 不支持预览格式。"
                else:
                    return "", f"❌ 内部序号错误！有效范围: 1-{len(all_extracted_files)}"
            else:
                # 寻找最接近的文件路径
                for f_p in all_extracted_files:
                    if f_p.replace(extract_path, "").strip(os.sep).replace(os.sep, "/") == inner_target.replace(os.sep, "/"):
                        preview_file_path = f_p
                        break
                if not preview_file_path:
                    # 模糊匹配
                    for f_p in all_extracted_files:
                        if inner_target.lower() in f_p.lower():
                            preview_file_path = f_p
                            break
                if preview_file_path and not preview_file_path.lower().endswith(utils.SUPPORTED_TEXT_FORMATS):
                    return "", f"❌ 指定的文件「{os.path.basename(preview_file_path)}」不支持预览格式。"
        
        # 如果没有明确指定，且只有一个文件，且该文件支持预览，则自动预览
        if not preview_file_path and len(all_extracted_files) == 1:
            first_file = all_extracted_files[0]
            if first_file.lower().endswith(utils.SUPPORTED_TEXT_FORMATS):
                preview_file_path = first_file

        if not preview_file_path:
            # 输出文件树
            file_structure = ["📦 压缩包内文件结构："]
            for i, f_path in enumerate(all_extracted_files, 1):
                relative_path = os.path.relpath(f_path, extract_path)
                file_size = os.path.getsize(f_path)
                size_str = utils.format_bytes(file_size)
                # 使用 / 作为分隔符统一显示
                display_path = relative_path.replace(os.sep, "/")
                file_structure.append(f"[{i}] {display_path} ({size_str})")
            
            file_structure.append("\n💡 提示：使用 /预览 <序号> <内部序号> 预览特定文件。")
            structure_text = "\n".join(file_structure)
            return structure_text, None
        
        # 3. 读取并解码内容
        with open(preview_file_path, 'rb') as f:
            content_bytes = f.read(preview_length * 4)
        
        preview_text_raw, encoding = await get_preview_from_bytes(content_bytes)
        
        inner_file_name = os.path.relpath(preview_file_path, extract_path).replace(os.sep, "/")
        preview_text = f"📂 内部文件: {inner_file_name}\n" + "-" * 20 + f"\n{preview_text_raw}"
        
    except FileNotFoundError:
        logger.error("解压失败：容器内未找到 7za 命令。")
        error_msg = "解压失败：未安装 7za"
    except Exception as e:
        logger.error(f"处理压缩文件时发生未知错误: {e}", exc_info=True)
        error_msg = "处理压缩文件时发生内部错误"
    finally:
        if extract_path and os.path.exists(extract_path):
            try:
                for root, dirs, files in os.walk(extract_path, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
                os.rmdir(extract_path)
            except Exception as e:
                logger.warning(f"删除临时文件夹 {extract_path} 失败: {e}")
    
    return preview_text, error_msg

async def get_pdf_preview(file_path: str, max_pages: int = 1) -> List[str]:
    """使用 pypdfium2 生成 PDF 预览图"""
    if max_pages <= 0:
        return []
        
    image_paths = []
    temp_dir = os.path.join(StarTools.get_data_dir('astrbot_plugin_GroupFS'), 'temp_file_previews')
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        pdf = pdfium.PdfDocument(file_path)
        num_pages = len(pdf)
        pages_to_render = min(num_pages, max_pages)
        
        for i in range(pages_to_render):
            page = pdf[i]
            # scale=2 对应约 144 DPI
            bitmap = page.render(scale=2)
            image_path = os.path.join(temp_dir, f"pdf_preview_{int(time.time())}_{i}.png")
            bitmap.to_pil().save(image_path)
            image_paths.append(image_path)
            page.close() # 释放资源
        pdf.close() # 释放资源
    except Exception as e:
        logger.error(f"生成 PDF 预览失败: {e}", exc_info=True)
    return image_paths

async def get_file_preview(group_id: int, file_info: dict, bot, default_zip_password: str, preview_length: int, semaphore: asyncio.Semaphore, cleanup_fn, inner_target: str = None, pdf_preview_pages: int = 1) -> tuple[str, str | None | List[str]]:
    file_id = file_info.get("file_id")
    file_name = file_info.get("file_name", "")
    _, file_extension = os.path.splitext(file_name)
    file_extension = file_extension.lower()
    
    is_txt = file_extension in utils.SUPPORTED_TEXT_FORMATS
    is_archive = file_extension in utils.SUPPORTED_ARCHIVE_FORMATS
    is_pdf = file_extension in utils.SUPPORTED_PDF_FORMATS
    is_epub = file_extension == ".epub"
    
    if not (is_txt or is_archive or is_pdf or is_epub):
        return "", f"❌ 文件「{file_name}」不是支持的文件格式，无法预览。"
        
    logger.info(f"[{group_id}] 正在为文件 '{file_name}' (ID: {file_id}) 获取预览 (目标: {inner_target})...")
    
    local_file_path = None
    
    try:
        url_result = await bot.api.call_action('get_group_file_url', group_id=group_id, file_id=file_id)
        if not (url_result and url_result.get('url')):
            return "", f"❌ 无法获取文件「{file_name}」的下载链接。"
        url = url_result['url']
    except ActionFailed as e:
        if e.result.get('retcode') == 1200:
            error_message = (
                f"❌ 预览文件「{file_name}」失败：\n"
                f"该文件可能已失效。\n"
                f"建议使用 /删除 {os.path.splitext(file_name)[0]} 将其删除。"
            )
            return "", error_message
        else:
            return "", f"❌ 预览失败，API返回错误：{e.result.get('wording', '未知错误')}"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with semaphore:
                range_header = None
                # 如果是压缩包或指定了内部路径，不能使用 Range 下载，因为需要完整文件进行解压
                if is_txt and not inner_target:
                    read_bytes_limit = preview_length * 4
                    range_header = {'Range': f'bytes=0-{read_bytes_limit - 1}'}
                async with session.get(url, headers=range_header, timeout=30) as resp:
                    if resp.status != 200 and resp.status != 206:
                        return "", f"❌ 下载文件「{file_name}」失败 (HTTP: {resp.status})。"
                    
                    temp_dir = os.path.join(StarTools.get_data_dir('astrbot_plugin_GroupFS'), 'temp_file_previews')
                    os.makedirs(temp_dir, exist_ok=True)
                    local_file_path = os.path.join(temp_dir, f"{file_id}_{file_name}")
                    
                    content_bytes = await resp.read()
                    with open(local_file_path, 'wb') as f:
                        f.write(content_bytes)
        
        preview_content = ""
        error_msg = None
        if is_epub:
            preview_text = extract_epub_text(local_file_path, preview_length)
            if preview_text:
                preview_content = "📖 EPUB 内容预览：\n" + preview_text
        elif is_txt and not inner_target:
            decoded_text, _ = await get_preview_from_bytes(content_bytes)
            if decoded_text:
                preview_content = decoded_text
        elif is_archive:
            preview_text, error_msg = await get_preview_from_archive(local_file_path, default_zip_password, preview_length, cleanup_fn, inner_target)
            if error_msg:
                return "", error_msg
            preview_content = preview_text
        elif is_pdf:
            image_paths = await get_pdf_preview(local_file_path, pdf_preview_pages)
            if not image_paths:
                return "", "❌ 生成 PDF 预览失败。"
            return image_paths, None
        elif is_txt and inner_target:
             return "", f"❌ 文本文件「{file_name}」不支持内部路径预览。"
        
        is_file_structure = preview_content.startswith("📦 压缩包内文件结构：")
        if not is_file_structure and len(preview_content) > preview_length:
            preview_content = preview_content[:preview_length] + "..."
        
        logger.info(f"[文件预览] 文件: {file_name}, 预览长度: {len(preview_content)}, 是否文件结构: {is_file_structure}")
        
        return preview_content, None
            
    except asyncio.TimeoutError:
        return "", f"❌ 预览文件「{file_name}」超时。"
    except Exception as e:
        logger.error(f"[{group_id}] 获取文件 '{file_name}' 预览时发生未知异常: {e}", exc_info=True)
        return "", f"❌ 预览文件「{file_name}」时发生内部错误。"
    finally:
        if local_file_path and os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                logger.info(f"已清理临时文件: {local_file_path}")
            except OSError as e:
                logger.warning(f"删除临时文件 {local_file_path} 失败: {e}")
