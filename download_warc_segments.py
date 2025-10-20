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
            # 在并发模式下，直接打印可能会扰乱tqdm进度条，但对于调试很有用
            # print(f"\nAttempt {attempt + 1} for {record['url']} failed: {e}") 
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt)) # 指数退避
            else:
                # 所有重试失败后，抛出异常
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
    返回一个元组 (status, url, message)
    """
    url = record.get("url")
    status = record.get("status")

    # 跳过非200状态码的记录
    if status != "200" or not url:
        return ("skipped_status", url, f"Status was {status}")

    filename = safe_filename(url)
    output_path = os.path.join(OUTPUT_DIR, filename)

    # 如果文件已存在，则跳过，实现断点续传
    if os.path.exists(output_path):
        return ("skipped_exists", url, "File already exists")

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
        
    total_records = len(lines)
    print(f"共找到 {total_records} 条索引记录。使用 {MAX_WORKERS} 个线程开始并发下载...")

    # 统计计数器
    success_count = 0
    skipped_exists_count = 0
    skipped_status_count = 0
    failed_count = 0
    
    # 使用ThreadPoolExecutor进行并发下载
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务
        future_to_record = {executor.submit(process_record, record): record for record in lines}
        
        # 使用tqdm处理已完成的任务，提供实时进度
        progress_bar = tqdm(concurrent.futures.as_completed(future_to_record), total=total_records, desc="下载进度")
        
        for future in progress_bar:
            try:
                status, url, message = future.result()
                if status == "success":
                    success_count += 1
                elif status == "skipped_exists":
                    skipped_exists_count += 1
                elif status == "skipped_status":
                    skipped_status_count += 1
                elif status == "failed":
                    failed_count += 1
                
                # 可以在进度条后显示统计信息
                progress_bar.set_postfix(
                    success=success_count, 
                    skipped=skipped_exists_count + skipped_status_count, 
                    failed=failed_count
                )

            except Exception as exc:
                # 捕获工作函数本身可能抛出的意外异常
                record = future_to_record[future]
                url = record.get('url', 'unknown_url')
                log_failure(url, f"executor_error: {exc}")
                failed_count += 1
                progress_bar.set_postfix(
                    success=success_count, 
                    skipped=skipped_exists_count + skipped_status_count, 
                    failed=failed_count
                )

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