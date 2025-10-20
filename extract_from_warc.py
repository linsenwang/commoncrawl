# 2_extract_to_jsonl.py
import os
import re
import io
import gzip
import json  # 引入json模块
from tqdm import tqdm
from charset_normalizer import from_bytes
from bs4 import BeautifulSoup

# ========== 配置 ==========
# 输入：存放原始WARC片段的目录
INPUT_DIR = "guardian_world_warc_segments"
# 输出：将提取的HTML和纯文本内容存储为JSON Lines格式的大文件
OUTPUT_HTML_FILE = "guardian_world_html.jsonl"
OUTPUT_TEXT_FILE = "guardian_world_text.jsonl"
# 日志：记录解析或提取失败的文件
LOG_FILE = "extraction_failed.log"

# ========== 工具函数 ==========

def maybe_decompress(warc_bytes: bytes) -> bytes:
    """尝试解压 gzip 数据，如果失败则原样返回"""
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(warc_bytes)) as f:
            return f.read()
    except (OSError, gzip.BadGzipFile):
        return warc_bytes


def extract_html_from_warc(warc_bytes: bytes) -> str:
    """
    从WARC响应中提取HTML正文，并自动检测和转换编码。
    """
    raw_bytes = maybe_decompress(warc_bytes)
    header_end = raw_bytes.find(b'\r\n\r\n')
    if header_end == -1:
        return raw_bytes.decode('utf-8', errors='ignore')
    http_header_end = raw_bytes.find(b'\r\n\r\n', header_end + 4)
    if http_header_end == -1:
        return raw_bytes.decode('utf-8', errors='ignore')
    http_headers_bytes = raw_bytes[header_end+4:http_header_end]
    body_bytes = raw_bytes[http_header_end+4:]

    encoding = None
    match = re.search(br"charset=([\w\-]+)", http_headers_bytes, re.IGNORECASE)
    if match:
        try:
            encoding = match.group(1).decode('ascii')
            return body_bytes.decode(encoding, errors='ignore')
        except (LookupError, UnicodeDecodeError):
            pass
    result = from_bytes(body_bytes).best()
    if result:
        return str(result)
    return body_bytes.decode('utf-8', errors='ignore')

def extract_article_text(html: str) -> str:
    """
    使用BeautifulSoup从HTML中提取主要文章内容。
    """
    soup = BeautifulSoup(html, 'html.parser')
    article_body = soup.find('div', attrs={'itemprop': 'articleBody'})
    if not article_body:
        article_body = soup.find('div', class_=re.compile(r'content__article-body'))
    if not article_body:
        article_body = soup.find('article') or soup.find('main') or soup.body

    if article_body:
        for element in article_body(["script", "style"]):
            element.decompose()
        text = article_body.get_text(separator='\n', strip=True)
        return text
    return ""


def log_failure(filename, reason):
    """记录失败的文件和原因"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{filename}\t{reason}\n")

def load_processed_ids(filepath: str) -> set:
    """
    从JSONL文件中加载已经处理过的记录ID，以便断点续传。
    """
    processed_ids = set()
    if not os.path.exists(filepath):
        return processed_ids
    
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    record = json.loads(line)
                    if 'id' in record:
                        processed_ids.add(record['id'])
                except json.JSONDecodeError:
                    # 忽略损坏的行
                    print(f"警告: 在 {filepath} 中发现损坏的JSON行，已跳过。")
    return processed_ids


# ========== 主逻辑 ==========
def main():
    if not os.path.exists(INPUT_DIR):
        print(f"错误: 输入目录 '{INPUT_DIR}' 不存在。请先运行 `1_download_warc_segments.py`。")
        return
    
    # 加载已处理的ID集合，我们只需要检查一个输出文件即可，因为它们是同步生成的
    processed_ids = load_processed_ids(OUTPUT_TEXT_FILE)
    if processed_ids:
        print(f"已找到 {len(processed_ids)} 个已处理的记录，将跳过它们。")
        
    warc_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.warc.gz')]
    print(f"找到 {len(warc_files)} 个WARC文件。开始提取内容...")

    # 在循环外以追加模式打开文件，效率更高
    with open(OUTPUT_HTML_FILE, "a", encoding="utf-8") as html_f, \
         open(OUTPUT_TEXT_FILE, "a", encoding="utf-8") as text_f:

        for filename in tqdm(warc_files, desc="处理进度"):
            base_name = os.path.splitext(os.path.splitext(filename)[0])[0]

            # 如果ID已处理，则跳过
            if base_name in processed_ids:
                continue

            input_path = os.path.join(INPUT_DIR, filename)
            
            try:
                with open(input_path, "rb") as f:
                    warc_bytes = f.read()

                html_content = extract_html_from_warc(warc_bytes)
                if not html_content.strip():
                    raise ValueError("Extracted HTML is empty.")
                    
                article_text = extract_article_text(html_content)
                if not article_text.strip():
                    raise ValueError("Extracted article text is empty.")

                # 创建要写入的JSON记录
                # 使用 base_name 作为唯一ID
                html_record = {"id": base_name, "content": html_content}
                text_record = {"id": base_name, "content": article_text}

                # 将记录作为JSON字符串写入文件，并添加换行符
                # ensure_ascii=False 确保中文字符等直接以UTF-8写入，而不是\uXXXX格式
                html_f.write(json.dumps(html_record, ensure_ascii=False) + '\n')
                text_f.write(json.dumps(text_record, ensure_ascii=False) + '\n')

            except Exception as e:
                log_failure(filename, f"{type(e).__name__}: {str(e)}")

    print(f"\n✅ 内容提取完成！")
    print(f"HTML内容已追加到: '{OUTPUT_HTML_FILE}'")
    print(f"纯文本内容已追加到: '{OUTPUT_TEXT_FILE}'")
    print(f"失败记录已写入: '{LOG_FILE}'")

if __name__ == "__main__":
    main()