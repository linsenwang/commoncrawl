# download_warc.py (v5 - 精确目录控制 & True Streaming)
import os
import json
import hashlib
import requests
import time
import threading
import concurrent.futures
from tqdm import tqdm
from requests.exceptions import RequestException, ChunkedEncodingError, ConnectionError, ReadTimeout

# ========== 配置 (无变化) ==========
INPUT_JSONL = "/Volumes/T7/cc/guardian_index/guardian_index_200_only.jsonl"
OUTPUT_DIR = "/Volumes/T7/cc/guardian_warc_segments"
LOG_FILE = "/Volumes/T7/cc/guardian_warc_segments/download_failed.log"
SUCCESS_LOG = "/Volumes/T7/cc/guardian_warc_segments/download_success.log"
BASE_URL = "https://data.commoncrawl.org/"
MAX_WORKERS = 64
MAX_FILES_PER_DIR = 5000
MAX_FUTURES_IN_FLIGHT = MAX_WORKERS * 4

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========== 线程锁 (无变化) ==========
fail_log_lock = threading.Lock()
success_log_lock = threading.Lock()
# <<< 新增线程锁：用于控制 get_target_directory 的并发访问 >>>
# 虽然 os.makedirs(exist_ok=True) 是线程安全的，但为了防止多个线程在同一瞬间
# 都发现某个目录满了并尝试创建下一个目录，加锁可以保证目录序号的严格递增。
# 这是一个更稳健的做法。
dir_check_lock = threading.Lock()

# ========== 工具函数 (get_target_directory 增加锁) ==========
def safe_filename(url: str) -> str:
    h = hashlib.md5(url.encode()).hexdigest()
    return f"{h}.warc.gz"

def fetch_segment(record, retries=3, backoff=2):
    # ... (此函数无变化) ...
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
    """
    获取当前应该用于保存文件的目录。
    这个函数现在是线程安全的，并且每次调用都会检查最新的目录状态。
    """
    with dir_check_lock: # <<< 修改：增加锁来保证操作的原子性
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
                # 统计实际文件数，排除隐藏文件
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


# ========== 主逻辑 (核心修改) ==========

# <<< 核心修改 1：修改 process_record 函数 >>>
def process_record(record, base_output_dir):
    """
    处理单个记录，函数内部动态决定存储目录。
    """
    url = record["url"]
    filename = safe_filename(url)
    
    # 在保存文件前，动态获取当前正确的目标目录
    target_dir = get_target_directory(base_output_dir)
    
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

def generate_tasks(input_file, completed_hashes):
    # ... (此函数无变化) ...
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    filename = safe_filename(record["url"])
                    if filename in completed_hashes:
                        continue
                    yield record
                except (json.JSONDecodeError, KeyError):
                    continue
    except FileNotFoundError:
        print(f"错误: 输入文件 '{input_file}' 未找到。")
        return


def main():
    # --- 1. 加载已完成记录 (无变化) ---
    print("正在加载已完成的下载记录...")
    completed_hashes = set()
    try:
        with open(SUCCESS_LOG, "r", encoding="utf-8") as f:
            completed_hashes.update(line.strip() for line in f)
        print(f"从 '{SUCCESS_LOG}' 加载了 {len(completed_hashes)} 条记录。")
    except FileNotFoundError:
        print(f"成功日志 '{SUCCESS_LOG}' 未找到，将自动创建。")

    # --- 2. 预先计算总任务数 (无变化) ---
    print("正在扫描索引文件以计算任务总数...")
    total_records_in_file = 0
    total_to_download = 0
    try:
        with open(INPUT_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                total_records_in_file += 1
                try:
                    url = json.loads(line).get("url")
                    if url and safe_filename(url) not in completed_hashes:
                        total_to_download += 1
                except (json.JSONDecodeError, KeyError):
                    continue
    except FileNotFoundError:
        print(f"错误: 输入文件 '{INPUT_JSONL}' 未找到。程序即将退出。")
        return

    print(f"索引文件 '{INPUT_JSONL}' 中共有 {total_records_in_file} 条记录。")
    print(f"已完成: {len(completed_hashes)} 条。")
    print(f"预计需要下载: {total_to_download} 个新文件。")

    if total_to_download == 0:
        print("\n所有文件均已下载，无需执行任何操作。")
        return

    # --- 3. 流式处理和并发提交 (核心修改) ---
    # <<< 核心修改 2：不再预先获取 target_dir >>>
    # target_dir = get_target_directory(OUTPUT_DIR) # <--- 删除这一行
    print(f"使用 {MAX_WORKERS} 个线程开始并发下载...")
    print(f"文件将被自动下载到 '{OUTPUT_DIR}' 下的 batch_XXXX 目录中。")

    success_count = 0
    failed_count = 0
    
    task_generator = generate_tasks(INPUT_JSONL, completed_hashes)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        progress_bar = tqdm(total=total_to_download, desc="下载进度")

        for record in task_generator:
            if len(futures) >= MAX_FUTURES_IN_FLIGHT:
                done_future = next(concurrent.futures.as_completed(futures))
                futures.remove(done_future)
                
                try:
                    status, _, _ = done_future.result()
                    if status == "success": success_count += 1
                    else: failed_count += 1
                except Exception as e:
                    log_failure("unknown_url", f"executor_error: {e}")
                    failed_count += 1
                finally:
                    progress_bar.update(1)
                    progress_bar.set_postfix(success=success_count, failed=failed_count)

            # <<< 核心修改 3：提交任务时，传递基础输出目录 OUTPUT_DIR >>>
            # 而不是之前固定的 target_dir
            future = executor.submit(process_record, record, OUTPUT_DIR)
            futures.append(future)

        # --- 4. 处理剩余的 future (无变化) ---
        progress_bar.set_description("下载收尾")
        for future in concurrent.futures.as_completed(futures):
            try:
                status, _, _ = future.result()
                if status == "success": success_count += 1
                else: failed_count += 1
            except Exception as e:
                log_failure("unknown_url", f"executor_error: {e}")
                failed_count += 1
            finally:
                progress_bar.update(1)
                progress_bar.set_postfix(success=success_count, failed=failed_count)
        
        progress_bar.close()

    print("\n✅ 下载完成！")
    print("========== 结果统计 ==========")
    print(f"  本次成功下载: {success_count}")
    print(f"  已跳过 (之前已下载): {total_records_in_file - total_to_download}")
    print(f"  下载失败: {failed_count}")
    if failed_count > 0:
        print(f"  失败详情请查看日志文件: '{LOG_FILE}'")
    print("==============================")


if __name__ == "__main__":
    main()