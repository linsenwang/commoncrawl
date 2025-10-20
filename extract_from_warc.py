# 3_extract_structured_data_chunked.py
import os
import re
import io
import gzip
import json
import glob
from tqdm import tqdm
from charset_normalizer import from_bytes
from bs4 import BeautifulSoup
from typing import Dict, Optional, Set

# ========== 配置 ==========
# 输入：存放原始WARC片段的目录
INPUT_DIR = "guardian_world_warc_segments"
# 输出：存放提取出的结构化数据文件的目录
OUTPUT_DATA_DIR = "guardian_world_structured_data"
# 日志：记录解析或提取失败的文件
LOG_FILE = "extraction_failed.log"
# 分块大小：每处理多少个文件，就创建一个新的输出文件
FILES_PER_CHUNK = 10000

# 创建输出目录
os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)


# ========== 工具函数 ==========

def maybe_decompress(warc_bytes: bytes) -> bytes:
    """尝试解压 gzip 数据，如果失败则原样返回"""
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(warc_bytes)) as f:
            return f.read()
    except (OSError, gzip.BadGzipFile):
        return warc_bytes


def extract_html_from_warc(warc_bytes: bytes) -> str:
    """从WARC响应中提取HTML正文，并自动检测和转换编码。"""
    raw_bytes = maybe_decompress(warc_bytes)
    header_end = raw_bytes.find(b'\r\n\r\n')
    if header_end == -1: return raw_bytes.decode('utf-8', errors='ignore')
    http_header_end = raw_bytes.find(b'\r\n\r\n', header_end + 4)
    if http_header_end == -1: return raw_bytes.decode('utf-8', errors='ignore')
    
    http_headers_bytes = raw_bytes[header_end+4:http_header_end]
    body_bytes = raw_bytes[http_header_end+4:]

    match = re.search(br"charset=([\w\-]+)", http_headers_bytes, re.IGNORECASE)
    if match:
        try:
            encoding = match.group(1).decode('ascii')
            return body_bytes.decode(encoding, errors='ignore')
        except (LookupError, UnicodeDecodeError): pass

    result = from_bytes(body_bytes).best()
    return str(result) if result else body_bytes.decode('utf-8', errors='ignore')

def extract_article_data(html: str) -> Dict[str, Optional[str]]:
    """
    使用BeautifulSoup从HTML中提取结构化数据（标题、发布时间、作者、正文）。
    这是针对 The Guardian 网站优化的选择器。
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    # 1. 提取标题
    title_tag = soup.find('h1', class_=re.compile(r'content__headline'))
    title = title_tag.get_text(strip=True) if title_tag else None

    # 2. 提取发布时间 (寻找<time>标签及其datetime属性)
    time_tag = soup.find('time', attrs={'itemprop': 'datePublished'})
    publish_time = time_tag['datetime'] if time_tag and 'datetime' in time_tag.attrs else None

    # 3. 提取作者
    author_tag = soup.find('a', attrs={'rel': 'author'})
    author = author_tag.get_text(strip=True) if author_tag else None

    # 4. 提取文章正文
    article_body_tag = soup.find('div', attrs={'itemprop': 'articleBody'})
    if not article_body_tag:
        article_body_tag = soup.find('div', class_=re.compile(r'content__article-body'))
    
    text = ""
    if article_body_tag:
        for element in article_body_tag(["script", "style", "aside"]):
            element.decompose()
        text = article_body_tag.get_text(separator='\n', strip=True)
    
    return {
        "title": title,
        "publish_time": publish_time,
        "author": author,
        "text": text,
    }

def log_failure(filename: str, reason: str):
    """记录失败的文件和原因"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{filename}\t{reason}\n")

def load_processed_ids(dir_path: str) -> Set[str]:
    """
    扫描输出目录中的所有.jsonl文件，加载已经处理过的记录ID。
    """
    processed_ids = set()
    # 使用glob查找所有jsonl文件
    for filepath in glob.glob(os.path.join(dir_path, "*.jsonl")):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        if 'id' in record:
                            processed_ids.add(record['id'])
        except (json.JSONDecodeError, IOError) as e:
            print(f"警告: 读取 {filepath} 时出错，已跳过: {e}")
    return processed_ids

# ========== 主逻辑 ==========
def main():
    if not os.path.exists(INPUT_DIR):
        print(f"错误: 输入目录 '{INPUT_DIR}' 不存在。")
        return

    processed_ids = load_processed_ids(OUTPUT_DATA_DIR)
    if processed_ids:
        print(f"已找到 {len(processed_ids)} 个已处理的记录，将跳过它们。")

    all_warc_files = sorted([f for f in os.listdir(INPUT_DIR) if f.endswith('.warc.gz')])
    
    # 过滤掉已处理的文件
    files_to_process = []
    for f in all_warc_files:
        base_name = os.path.splitext(os.path.splitext(f)[0])[0]
        if base_name not in processed_ids:
            files_to_process.append(f)
            
    if not files_to_process:
        print("所有文件均已处理。程序退出。")
        return

    print(f"总共找到 {len(all_warc_files)} 个WARC文件，需处理 {len(files_to_process)} 个。")

    output_file_handle = None
    chunk_index = 1
    # 确定起始的chunk index
    existing_chunks = glob.glob(os.path.join(OUTPUT_DATA_DIR, "data_*.jsonl"))
    if existing_chunks:
        chunk_index = len(existing_chunks)
        # 假设最后一个文件可能未写满，我们继续向其追加
        last_chunk_path = os.path.join(OUTPUT_DATA_DIR, f"data_{chunk_index:05d}.jsonl")
        if os.path.exists(last_chunk_path):
             with open(last_chunk_path, 'r') as f:
                 lines_in_last_chunk = sum(1 for line in f)
             if lines_in_last_chunk < FILES_PER_CHUNK:
                 print(f"继续向未写满的文件 {last_chunk_path} 追加...")
             else:
                 chunk_index += 1 # 最后一个文件满了，开一个新的
        else: # 如果文件序号不连续，就从最大的下一个开始
            chunk_index = max([int(re.search(r'data_(\d+).jsonl', p).group(1)) for p in existing_chunks]) + 1


    files_in_current_chunk = 0

    try:
        for filename in tqdm(files_to_process, desc="处理进度"):
            # --- 分块逻辑 ---
            if output_file_handle is None or files_in_current_chunk >= FILES_PER_CHUNK:
                if output_file_handle:
                    output_file_handle.close()
                
                output_path = os.path.join(OUTPUT_DATA_DIR, f"data_{chunk_index:05d}.jsonl")
                print(f"\n创建新的输出块: {output_path}")
                output_file_handle = open(output_path, "a", encoding="utf-8")
                chunk_index += 1
                files_in_current_chunk = 0
            
            # --- 提取逻辑 ---
            base_name = os.path.splitext(os.path.splitext(filename)[0])[0]
            input_path = os.path.join(INPUT_DIR, filename)
            
            try:
                with open(input_path, "rb") as f:
                    warc_bytes = f.read()

                html_content = extract_html_from_warc(warc_bytes)
                if not html_content or not html_content.strip():
                    raise ValueError("Extracted HTML is empty.")
                
                article_data = extract_article_data(html_content)
                if not article_data.get("text") or not article_data["text"].strip():
                    raise ValueError("Extracted article text is empty.")

                # 合并ID和提取的数据
                final_record = {"id": base_name, **article_data}
                
                # 写入JSONL文件
                output_file_handle.write(json.dumps(final_record, ensure_ascii=False) + '\n')
                files_in_current_chunk += 1

            except Exception as e:
                log_failure(filename, f"{type(e).__name__}: {str(e)}")
    finally:
        # 确保循环结束后，最后打开的文件被关闭
        if output_file_handle and not output_file_handle.closed:
            output_file_handle.close()

    print(f"\n✅ 结构化数据提取完成！")
    print(f"数据块已保存到目录: '{OUTPUT_DATA_DIR}'")
    print(f"失败记录已写入: '{LOG_FILE}'")


if __name__ == "__main__":
    main()