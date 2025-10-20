import os
import json
import hashlib
import requests
import time
import threading
import concurrent.futures
from tqdm import tqdm
from requests.exceptions import RequestException, ChunkedEncodingError, ConnectionError, ReadTimeout

# ========== 配置 ==========
# 输入：CDX索引文件
INPUT_JSONL = "guardian_index_world_merged.jsonl"
# 输出：存放原始WARC片段的目录
OUTPUT_DIR = "guardian_world_warc_segments"
# 日志：记录下载失败的URL
LOG_FILE = "download_failed.log"
# Common Crawl 基础URL
BASE_URL = "https://data.commoncrawl.org/"
# 并发下载的线程数 (根据你的网络和CPU调整)
MAX_WORKERS = 16

# 创建输出目录
os.makedirs(OUTPUT_DIR, exist_ok=True)
# 创建线程锁，用于安全地写入日志文件
log_lock = threading.Lock()


# ========== 工具函数 ==========
def safe_filename(url: str) -> str:
    """将URL转换为安全的、带.warc.gz后缀的文件名"""
    h = hashlib.md5(url.encode()).hexdigest()
    return f"{h}.warc.gz"


def fetch_segment(record, retries=3, backoff=2):
    """
    根据CDX记录下载对应的网页WARC片段（原始压缩格式）。
    """
    warc_path = record["filename"]
    offset = int(record["offset"])
    length = int(record["length"])
    end = offset + length - 1

    warc_url = BASE_URL + warc_path
    headers = {"Range": f"bytes={offset}-{end}"}

    for attempt in range(retries):
        try:
            # 使用流式传输和较长的超时时间，以应对大文件和慢网络
            with requests.get(warc_url, headers=headers, timeout=(10, 60), stream=True) as resp:
                resp.raise_for_status()
                # 检查响应大小是否与预期一致
                content_length = int(resp.headers.get('Content-Length', 0))
                if content_length != length:
                     raise RequestException(f"Incomplete download. Expected {length} bytes, got {content_length}")
                return resp.content
        except (ChunkedEncodingError, ConnectionError, ReadTimeout, RequestException) as e:
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt)) # 指数退避
            else:
                raise e


def log_failure(url, reason):
    """记录失败的URL和原因（线程安全）"""
    with log_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{url}\t{reason}\n")


# ========== 主逻辑 ==========
def process_record(record):
    """
    处理单个记录：下载、保存、处理异常。
    这是并发执行的工作单元。
    这个版本假设'skipped'检查已在主线程完成。
    """
    url = record["url"]
    filename = safe_filename(url)
    output_path = os.path.join(OUTPUT_DIR, filename)

    try:
        # 下载原始的、可能被压缩的WARC字节数据
        warc_bytes = fetch_segment(record)

        # 以二进制模式写入文件，保留原始压缩格式
        with open(output_path, "wb") as f:
            f.write(warc_bytes)
        
        return ("success", url, None)
        
    except Exception as e:
        # 捕获所有下载和写入过程中可能出现的错误
        error_message = f"{type(e).__name__}: {str(e)}"
        log_failure(url, error_message)
        return ("failed", url, error_message)


def main():
    try:
        with open(INPUT_JSONL, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        print(f"错误: 输入文件 '{INPUT_JSONL}' 未找到。")
        return
        
    print(f"共找到 {len(lines)} 条索引记录。")

    # --- 优化点：预先扫描已存在的文件 ---
    print("正在扫描输出目录以跳过已下载的文件...")
    try:
        # 使用集合(set)以获得 O(1) 的快速查找性能
        existing_files = set(os.listdir(OUTPUT_DIR))
        print(f"发现 {len(existing_files)} 个已存在的文件。")
    except FileNotFoundError:
        existing_files = set() # 目录可能在首次运行时不存在

    # --- 在主线程中快速过滤记录 ---
    records_to_process = []
    skipped_exists_count = 0
    skipped_status_count = 0
    
    for record in lines:
        status = record.get("status")
        url = record.get("url")

        if status != "200" or not url:
            skipped_status_count += 1
            continue
        
        filename = safe_filename(url)
        if filename in existing_files:
            skipped_exists_count += 1
            continue
            
        records_to_process.append(record)

    total_to_download = len(records_to_process)
    if total_to_download == 0:
        print("\n所有文件均已下载或被跳过，无需执行任何操作。")
    else:
        print(f"将要下载 {total_to_download} 个新文件。使用 {MAX_WORKERS} 个线程开始并发下载...")

    success_count = 0
    failed_count = 0
    
    # --- 仅将需要处理的记录提交给线程池 ---
    if total_to_download > 0:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_record = {executor.submit(process_record, record): record for record in records_to_process}
            
            progress_bar = tqdm(concurrent.futures.as_completed(future_to_record), total=total_to_download, desc="下载进度")
            
            for future in progress_bar:
                try:
                    status, _, _ = future.result()
                    if status == "success":
                        success_count += 1
                    else: # status == "failed"
                        failed_count += 1
                    
                    progress_bar.set_postfix(success=success_count, failed=failed_count)

                except Exception as exc:
                    record = future_to_record[future]
                    url = record.get('url', 'unknown_url')
                    log_failure(url, f"executor_error: {exc}")
                    failed_count += 1
                    progress_bar.set_postfix(success=success_count, failed=failed_count)

    print("\n✅ 下载完成！")
    print("========== 结果统计 ==========")
    print(f"  成功下载: {success_count}")
    print(f"  跳过 (已存在): {skipped_exists_count}")
    print(f"  跳过 (非200状态): {skipped_status_count}")
    print(f"  下载失败: {failed_count}")
    if failed_count > 0:
        print(f"  失败详情请查看日志文件: '{LOG_FILE}'")
    print("==============================")


if __name__ == "__main__":
    main()