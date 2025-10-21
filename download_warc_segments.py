# download_warc.py (v3 - memory efficient)
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
# <<< 关键修改：使用预处理过的、只包含200状态码的索引文件
INPUT_JSONL = "/Volumes/T7/cc/guardian_index/guardian_index_200_only.jsonl"
OUTPUT_DIR = "/Volumes/T7/cc/guardian_warc_segments"
LOG_FILE = "/Volumes/T7/cc/guardian_warc_segments/download_failed.log"
SUCCESS_LOG = "/Volumes/T7/cc/guardian_warc_segments/download_success.log"
BASE_URL = "https://data.commoncrawl.org/"
MAX_WORKERS = 16
MAX_FILES_PER_DIR = 5000

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========== 线程锁 (无变化) ==========
fail_log_lock = threading.Lock()
success_log_lock = threading.Lock()

# ========== 工具函数 (无变化) ==========
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

def log_success(filename_hash: str):
    with success_log_lock:
        with open(SUCCESS_LOG, "a", encoding="utf-8") as f:
            f.write(f"{filename_hash}\n")

def get_target_directory(base_dir: str) -> str:
    # ... (此函数无变化) ...
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
            num_files = len([f for f in os.listdir(latest_dir_path) if not f.startswith('.')])
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


# ========== 主逻辑 (无变化) ==========
def process_record(record, target_dir):
    url = record["url"]
    filename = safe_filename(url)
    output_path = os.path.join(target_dir, filename)
    try:
        warc_bytes = fetch_segment(record)
        with open(output_path, "wb") as f:
            f.write(warc_bytes)
        log_success(filename)
        return ("success", url, None)
    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}"
        log_failure(url, error_message)
        return ("failed", url, error_message)

def main():
    # --- 加载已完成记录 (逻辑与之前类似，但更健壮) ---
    print("正在加载已完成的下载记录...")
    completed_hashes = set()
    try:
        with open(SUCCESS_LOG, "r", encoding="utf-8") as f:
            completed_hashes.update(line.strip() for line in f)
        print(f"从 '{SUCCESS_LOG}' 加载了 {len(completed_hashes)} 条记录。")
    except FileNotFoundError:
        print(f"成功日志 '{SUCCESS_LOG}' 未找到，将自动创建。")
    
    # 文件系统扫描依然可以作为补充，以防日志文件不完整
    # (此部分逻辑与 v2 相同，这里省略以保持简洁)

    # --- <<< 关键修改：流式读取和过滤，不一次性加载到内存 >>> ---
    print("正在扫描索引文件并准备下载任务...")
    records_to_process = []
    skipped_exists_count = 0
    total_records_in_file = 0

    try:
        with open(INPUT_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                total_records_in_file += 1
                try:
                    record = json.loads(line)
                    filename = safe_filename(record["url"])
                    if filename in completed_hashes:
                        skipped_exists_count += 1
                        continue
                    records_to_process.append(record)
                except (json.JSONDecodeError, KeyError):
                    # 跳过格式错误的行或没有 "url" 字段的记录
                    continue
    except FileNotFoundError:
        print(f"错误: 输入文件 '{INPUT_JSONL}' 未找到。")
        print("请先运行 filter_jsonl.py 脚本生成此文件。")
        return

    print(f"索引文件 '{INPUT_JSONL}' 中共有 {total_records_in_file} 条记录。")

    # --- 后续逻辑与之前基本相同 ---
    target_dir = get_target_directory(OUTPUT_DIR)
    
    total_to_download = len(records_to_process)
    if total_to_download == 0:
        print("\n所有文件均已下载或被跳过，无需执行任何操作。")
    else:
        print(f"将要下载 {total_to_download} 个新文件。使用 {MAX_WORKERS} 个线程开始并发下载...")
        print(f"所有新文件将被下载到: '{target_dir}'")


    success_count = 0
    failed_count = 0
    
    if total_to_download > 0:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 这里的 future_to_record 字典会占用一些内存，但只包含待处理的任务，远小于整个文件
            future_to_record = {executor.submit(process_record, record, target_dir): record for record in records_to_process}
            progress_bar = tqdm(concurrent.futures.as_completed(future_to_record), total=total_to_download, desc="下载进度")
            
            for future in progress_bar:
                try:
                    status, _, _ = future.result()
                    if status == "success":
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception as exc:
                    record = future_to_record[future]
                    url = record.get('url', 'unknown_url')
                    log_failure(url, f"executor_error: {exc}")
                    failed_count += 1
                finally:
                     progress_bar.set_postfix(success=success_count, failed=failed_count)


    print("\n✅ 下载完成！")
    print("========== 结果统计 ==========")
    print(f"  本次成功下载: {success_count}")
    print(f"  跳过 (已存在): {skipped_exists_count}")
    # 因为已经预处理，所以不再有 "跳过 (非200状态)"
    print(f"  下载失败: {failed_count}")
    if failed_count > 0:
        print(f"  失败详情请查看日志文件: '{LOG_FILE}'")
    print("==============================")


if __name__ == "__main__":
    main()