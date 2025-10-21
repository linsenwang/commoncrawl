# download_warc.py (v2 - with persistent completion log)
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
INPUT_JSONL = "/Volumes/T7/cc/guardian_index/guardian_index_all.jsonl"
OUTPUT_DIR = "/Volumes/T7/cc/guardian_warc_segments"
LOG_FILE = "/Volumes/T7/cc/guardian_warc_segments/download_failed.log"
# 新增：持久化记录成功下载的文件哈希
SUCCESS_LOG = "/Volumes/T7/cc/guardian_warc_segments/download_success.log"
BASE_URL = "https://data.commoncrawl.org/"
MAX_WORKERS = 16
MAX_FILES_PER_DIR = 5000

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========== 线程锁 ==========
# 用于安全地写入失败日志
fail_log_lock = threading.Lock()
# 新增：用于安全地写入成功日志
success_log_lock = threading.Lock()


# ========== 工具函数 ==========
def safe_filename(url: str) -> str:
    h = hashlib.md5(url.encode()).hexdigest()
    return f"{h}.warc.gz"

def fetch_segment(record, retries=3, backoff=2):
    warc_path = record["filename"]
    offset = int(record["offset"])
    length = int(record["length"])
    end = offset + length - 1
    warc_url = BASE_URL + warc_path
    headers = {"Range": f"bytes={offset}-{end}"}

    for attempt in range(retries):
        try:
            with requests.get(warc_url, headers=headers, timeout=(10, 60), stream=True) as resp:
                resp.raise_for_status()
                content_length = int(resp.headers.get('Content-Length', 0))
                if content_length != length:
                     raise RequestException(f"Incomplete download. Expected {length} bytes, got {content_length}")
                return resp.content
        except (ChunkedEncodingError, ConnectionError, ReadTimeout, RequestException) as e:
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
            else:
                raise e

def log_failure(url, reason):
    with fail_log_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{url}\t{reason}\n")

# 新增：记录成功下载的文件哈希
def log_success(filename_hash: str):
    """记录成功下载的文件哈希（线程安全）"""
    with success_log_lock:
        with open(SUCCESS_LOG, "a", encoding="utf-8") as f:
            f.write(f"{filename_hash}\n")

# ========== 批处理逻辑（无变化） ==========
def get_target_directory(base_dir: str) -> str:
    try:
        subdirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d)) and d.startswith("batch_")]
    except FileNotFoundError:
        subdirs = []

    if not subdirs:
        target_dir = os.path.join(base_dir, "batch_0000")
    else:
        latest_dir_name = sorted(subdirs)[-1]
        latest_dir_path = os.path.join(base_dir, latest_dir_name)
        try:
            num_files = len(os.listdir(latest_dir_path))
        except FileNotFoundError:
            num_files = 0
        if num_files >= MAX_FILES_PER_DIR:
            latest_batch_num = int(latest_dir_name.split('_')[-1])
            new_batch_num = latest_batch_num + 1
            target_dir = os.path.join(base_dir, f"batch_{new_batch_num:04d}")
        else:
            target_dir = latest_dir_path
    os.makedirs(target_dir, exist_ok=True)
    return target_dir


# ========== 主逻辑修改 ==========
def process_record(record, target_dir):
    url = record["url"]
    filename = safe_filename(url)
    output_path = os.path.join(target_dir, filename)

    try:
        warc_bytes = fetch_segment(record)
        with open(output_path, "wb") as f:
            f.write(warc_bytes)
        
        # 关键步骤：下载并写入成功后，记录到成功日志
        log_success(filename)
        
        return ("success", url, None)
    except Exception as e:
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

    # --- 关键改动：从日志文件和文件系统加载已完成的哈希 ---
    print("正在加载已完成的下载记录...")
    completed_hashes = set()

    # 1. 从成功日志加载
    try:
        with open(SUCCESS_LOG, "r", encoding="utf-8") as f:
            for line in f:
                completed_hashes.add(line.strip())
        print(f"从 '{SUCCESS_LOG}' 加载了 {len(completed_hashes)} 条记录。")
    except FileNotFoundError:
        print(f"成功日志 '{SUCCESS_LOG}' 未找到，将自动创建。")

    # 2. 从文件系统扫描（作为补充，确保兼容性）
    print("正在扫描文件系统以补充记录...")
    fs_found_count = 0
    initial_set_size = len(completed_hashes)
    try:
        subdirs = [d for d in os.listdir(OUTPUT_DIR) if os.path.isdir(os.path.join(OUTPUT_DIR, d))]
        for subdir in subdirs:
            subdir_path = os.path.join(OUTPUT_DIR, subdir)
            for filename in os.listdir(subdir_path):
                completed_hashes.add(filename)
                fs_found_count += 1
    except FileNotFoundError:
        pass
    
    newly_added_from_fs = len(completed_hashes) - initial_set_size
    print(f"从文件系统发现 {fs_found_count} 个文件，新补充了 {newly_added_from_fs} 条记录。")
    print(f"总计 {len(completed_hashes)} 个文件被标记为已完成。")

    # --- 过滤记录 ---
    records_to_process = []
    skipped_exists_count = 0
    skipped_status_count = 0
    
    for record in lines:
        if record.get("status") != "200" or not record.get("url"):
            skipped_status_count += 1
            continue
        
        filename = safe_filename(record["url"])
        if filename in completed_hashes:
            skipped_exists_count += 1
            continue
            
        records_to_process.append(record)

    # --- 后续逻辑与之前基本相同 ---
    target_dir = get_target_directory(OUTPUT_DIR)
    print(f"所有新文件将被下载到: '{target_dir}'")
    
    total_to_download = len(records_to_process)
    if total_to_download == 0:
        print("\n所有文件均已下载或被跳过，无需执行任何操作。")
    else:
        print(f"将要下载 {total_to_download} 个新文件。使用 {MAX_WORKERS} 个线程开始并发下载...")

    success_count = 0
    failed_count = 0
    
    if total_to_download > 0:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_record = {executor.submit(process_record, record, target_dir): record for record in records_to_process}
            progress_bar = tqdm(concurrent.futures.as_completed(future_to_record), total=total_to_download, desc="下载进度")
            
            for future in progress_bar:
                try:
                    status, _, _ = future.result()
                    if status == "success":
                        success_count += 1
                    else:
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
    print(f"  本次成功下载: {success_count}")
    print(f"  跳过 (已存在): {skipped_exists_count}")
    print(f"  跳过 (非200状态): {skipped_status_count}")
    print(f"  下载失败: {failed_count}")
    if failed_count > 0:
        print(f"  失败详情请查看日志文件: '{LOG_FILE}'")
    print("==============================")


if __name__ == "__main__":
    main()