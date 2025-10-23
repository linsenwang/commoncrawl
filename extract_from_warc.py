# 4_extract_parallel.py
import os
import re
import io
import gzip
import json
import glob
from tqdm import tqdm
from charset_normalizer import from_bytes
from bs4 import BeautifulSoup
from typing import Dict, Optional, Tuple, List
from multiprocessing import Pool, cpu_count

# ========== 配置 ==========
# INPUT_DIR = "/Volumes/T7/cc/guardian_warc_segments/batch_0201"
# OUTPUT_DATA_DIR = "guardian_world_structured_data"
# LOG_FILE = "extraction_failed.log"

INPUT_DIR = "/Users/yangqian/Downloads/commoncrawl/sample_1000/batch_0000"
OUTPUT_DATA_DIR = "sample_1000_featured"  # 输出目录名更改，更符合内容
LOG_FILE = "extraction_failed.log"

FILES_PER_CHUNK = 10000
# 使用所有可用的CPU核心数减一，留一个给系统，或者直接用 cpu_count()
NUM_PROCESSES = max(1, cpu_count() - 1) 

# 创建输出目录
os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)


# ========== 解析函数 (将在子进程中运行) ==========

def extract_html_from_warc(warc_bytes: bytes) -> str:
    # ... (此函数无需更改)
    try:
        raw_bytes = gzip.decompress(warc_bytes)
    except (OSError, gzip.BadGzipFile):
        raw_bytes = warc_bytes
        
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
    soup = BeautifulSoup(html, 'lxml')

    # ===== 标题 =====
    title_tag = soup.find('h1', class_=re.compile(r'content__headline'))
    title = title_tag.get_text(strip=True) if title_tag else None

    # ===== 发布时间 =====
    time_tag = soup.find('time', attrs={'itemprop': 'datePublished'})
    publish_time = time_tag['datetime'] if time_tag and 'datetime' in time_tag.attrs else None

    # ===== 作者 =====
    author_tag = soup.find('a', attrs={'rel': 'author'})
    author = author_tag.get_text(strip=True) if author_tag else None

    # ===== 正文内容 =====
    article_body_tag = soup.find('div', attrs={'itemprop': 'articleBody'})
    if not article_body_tag:
        article_body_tag = soup.find('div', class_=re.compile(r'content__article-body'))
    
    text = ""
    if article_body_tag:
        for element in article_body_tag(["script", "style", "aside"]):
            element.decompose()
        text = article_body_tag.get_text(separator='\n', strip=True)

    # ===== (1) 顶部导航标签 signposting =====
    signposting_tags = []

    # 1 浅蓝色
    signposting_ul = soup.find('ul', class_=re.compile(r'signposting'))
    if signposting_ul:
        for li in signposting_ul.find_all('li', class_=re.compile(r'signposting__item')):
            a_tag = li.find('a')
            if a_tag:
                tag_text = a_tag.get_text(strip=True)
                if tag_text.lower() != 'home':  # 排除 'home'
                    signposting_tags.append(tag_text)

    # 2 深蓝色
    if not signposting_tags:
        subnav_ul = soup.find('ul', class_=re.compile(r'subnav__list'))
        if subnav_ul:
            for li in subnav_ul.find_all('li', class_=re.compile(r'subnav__item')):
                a_tag = li.find('a', class_=re.compile(r'subnav-link'))
                if a_tag:
                    tag_text = a_tag.get_text(strip=True)
                    if tag_text.lower() != 'home':
                        signposting_tags.append(tag_text)

    # 3 浅蓝色2
    if not signposting_tags:
        labels_div = soup.find('div', class_=re.compile(r'content__labels'))
        if labels_div:
            for a_tag in labels_div.find_all('a', href=True):
                tag_text = a_tag.get_text(strip=True)
                if tag_text.lower() != 'home':
                    signposting_tags.append(tag_text)

    # ===== (2) 内容标签 content__labels =====
    section_labels = []
    labels_div = soup.find('div', class_=re.compile(r'content__labels'))
    if labels_div:
        for a_tag in labels_div.find_all('a', class_=re.compile(r'content__section-label__link')):
            section_labels.append(a_tag.get_text(strip=True))

    # ===== (3) 关键词标签 submeta__keywords =====
    keyword_tags = []

    # 1 浅蓝色
    keywords_div = soup.find('div', class_=re.compile(r'submeta__keywords'))
    if keywords_div:
        for a_tag in keywords_div.find_all('a', class_=re.compile(r'submeta__link')):
            keyword_tags.append(a_tag.get_text(strip=True))

    # 2 深蓝色
    if not keyword_tags:  # 如果前者没找到，尝试新版结构
        keyword_list = soup.find('ul', class_=re.compile(r'keyword-list'))
        if keyword_list:
            for a_tag in keyword_list.find_all('a', itemprop='keywords'):
                keyword_tags.append(a_tag.get_text(strip=True))

    # 3 白色
    if not keyword_tags:
        submeta_links = soup.find('ul', class_=re.compile(r'submeta__links'))
        if submeta_links:
            for a_tag in submeta_links.find_all('a', class_=re.compile(r'submeta__link')):
                keyword_tags.append(a_tag.get_text(strip=True))

    # ===== 返回结构化结果 =====
    return {
        "title": title,
        "publish_time": publish_time,
        "author": author,
        "text": text,
        "signposting_tags": signposting_tags,  # 导航路径标签
        "section_labels": section_labels,      # 内容分类标签
        "keyword_tags": keyword_tags            # 关键词标签
    }

def process_single_file(filename: str) -> Optional[Dict]:
    """
    处理单个文件的完整流程：读取、解析、提取。
    这个函数会被分发到多个进程中并行执行。
    """
    try:
        base_name = os.path.splitext(os.path.splitext(filename)[0])[0]
        input_path = os.path.join(INPUT_DIR, filename)

        with open(input_path, "rb") as f:
            warc_bytes = f.read()

        html_content = extract_html_from_warc(warc_bytes)
        if not html_content or not html_content.strip():
            raise ValueError("Extracted HTML is empty.")
        
        article_data = extract_article_data(html_content)
        if not article_data.get("text") or not article_data["text"].strip():
            raise ValueError("Extracted article text is empty.")
        
        # 返回包含ID和提取数据的完整记录
        return {"id": base_name, **article_data}

    except Exception as e:
        # 在并行模式下，直接打印错误或返回特定标识符
        # 写入日志文件由主进程完成，避免竞争
        # print(f"Error processing {filename}: {e}")
        return {"_error": True, "filename": filename, "reason": f"{type(e).__name__}: {str(e)}"}


# ========== 主进程逻辑 ==========

def load_processed_ids(dir_path: str) -> set:
    processed_ids = set()
    for filepath in glob.glob(os.path.join(dir_path, "*.jsonl")):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        if 'id' in record:
                            processed_ids.add(record['id'])
        except (json.JSONDecodeError, IOError): pass
    return processed_ids

def main():
    if not os.path.exists(INPUT_DIR):
        print(f"错误: 输入目录 '{INPUT_DIR}' 不存在。")
        return

    processed_ids = load_processed_ids(OUTPUT_DATA_DIR)
    if processed_ids:
        print(f"已找到 {len(processed_ids)} 个已处理的记录，将跳过它们。")

    all_warc_files = sorted([f for f in os.listdir(INPUT_DIR) if f.endswith('.warc.gz') and not f.startswith('._')])
    files_to_process = [
        f for f in all_warc_files 
        if os.path.splitext(os.path.splitext(f)[0])[0] not in processed_ids
    ]
            
    if not files_to_process:
        print("所有文件均已处理。程序退出。")
        return

    print(f"总共找到 {len(all_warc_files)} 个WARC文件，需处理 {len(files_to_process)} 个。")
    print(f"启动 {NUM_PROCESSES} 个并行进程进行处理...")

    # 使用 multiprocessing.Pool 来并行处理文件
    with Pool(processes=NUM_PROCESSES) as pool:
        # 使用 imap_unordered 来获得最佳性能，它会按完成顺序返回结果
        results_iterator = pool.imap_unordered(process_single_file, files_to_process)
        
        # 使用 tqdm 显示进度
        pbar = tqdm(total=len(files_to_process), desc="处理进度")
        
        # --- 分块写入逻辑 ---
        output_file_handle = None
        records_in_current_chunk = 0
        chunk_index = len(glob.glob(os.path.join(OUTPUT_DATA_DIR, "data_*.jsonl"))) + 1
        log_f = open(LOG_FILE, "a", encoding="utf-8")

        try:
            for result in results_iterator:
                if result:
                    if result.get("_error"):
                        # 记录失败
                        log_f.write(f"{result['filename']}\t{result['reason']}\n")
                    else:
                        # 检查是否需要开启新文件块
                        if output_file_handle is None or records_in_current_chunk >= FILES_PER_CHUNK:
                            if output_file_handle:
                                output_file_handle.close()
                            
                            output_path = os.path.join(OUTPUT_DATA_DIR, f"data_{chunk_index:05d}.jsonl")
                            output_file_handle = open(output_path, "a", encoding="utf-8")
                            chunk_index += 1
                            records_in_current_chunk = 0
                        
                        # 写入成功的结果
                        output_file_handle.write(json.dumps(result, ensure_ascii=False) + '\n')
                        records_in_current_chunk += 1
                
                # 更新进度条
                pbar.update(1)
        finally:
            pbar.close()
            if output_file_handle:
                output_file_handle.close()
            log_f.close()

    print(f"\n✅ 结构化数据提取完成！")
    print(f"数据块已保存到目录: '{OUTPUT_DATA_DIR}'")
    print(f"失败记录已写入: '{LOG_FILE}'")

if __name__ == "__main__":
    main()