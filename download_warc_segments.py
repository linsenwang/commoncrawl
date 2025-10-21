# download_warc.py (v4 - True Streaming & Memory Efficient)
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
INPUT_JSONL = "/Volumes/T7/cc/guardian_index/guardian_index_200_only.jsonl"
OUTPUT_DIR = "/Volumes/T7/cc/guardian_warc_segments"
LOG_FILE = "/Volumes/T7/cc/guardian_warc_segments/download_failed.log"
SUCCESS_LOG = "/Volumes/T7/cc/guardian_warc_segments/download_success.log"
BASE_URL = "https://data.commoncrawl.org/"
MAX_WORKERS = 64
MAX_FILES_PER_DIR = 5000

# <<< 新增配置：控制内存中的任务队列大小 >>>
# 限制同时在内存中排队的任务数量，防止无限增长。
# 一个好的经验值是最大工作线程数的几倍。
MAX_FUTURES_IN_FLIGHT = MAX_WORKERS * 4

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

# <<< 关键修改：将文件读取和过滤逻辑封装成一个生成器 >>>
def generate_tasks(input_file, completed_hashes):
    """
    流式读取输入文件，跳过已完成的任务，并逐一产出（yield）需要处理的记录。
    这个函数不会将所有记录加载到内存中。
    """
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    filename = safe_filename(record["url"])
                    if filename in completed_hashes:
                        continue # 直接跳到下一行
                    yield record
                except (json.JSONDecodeError, KeyError):
                    # 跳过格式错误的行
                    continue
    except FileNotFoundError:
        print(f"错误: 输入文件 '{input_file}' 未找到。")
        print("请先运行 filter_jsonl.py 脚本生成此文件。")
        return # 结束生成器

def main():
    # --- 1. 加载已完成记录 (这部分仍然需要加载到内存中，但通常是可控的) ---
    print("正在加载已完成的下载记录...")
    completed_hashes = set()
    try:
        with open(SUCCESS_LOG, "r", encoding="utf-8") as f:
            completed_hashes.update(line.strip() for line in f)
        print(f"从 '{SUCCESS_LOG}' 加载了 {len(completed_hashes)} 条记录。")
    except FileNotFoundError:
        print(f"成功日志 '{SUCCESS_LOG}' 未找到，将自动创建。")

    # --- 2. 预先计算总任务数，以便显示准确的进度条 (快速扫描，低内存) ---
    print("正在扫描索引文件以计算任务总数...")
    total_records_in_file = 0
    total_to_download = 0
    try:
        with open(INPUT_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                total_records_in_file += 1
                try:
                    # 只解析URL来检查是否已完成，比 json.loads(line) 更快
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

    # --- 3. <<< 核心修改：流式处理和并发提交 >>> ---
    target_dir = get_target_directory(OUTPUT_DIR)
    print(f"使用 {MAX_WORKERS} 个线程开始并发下载...")
    print(f"所有新文件将被下载到: '{target_dir}'")

    success_count = 0
    failed_count = 0
    
    # 创建任务生成器
    task_generator = generate_tasks(INPUT_JSONL, completed_hashes)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        progress_bar = tqdm(total=total_to_download, desc="下载进度")

        # 主循环：从生成器获取任务并提交，同时处理已完成的任务
        for record in task_generator:
            # 当正在处理的 future 列表达到上限时，等待并处理一个已完成的任务，为新任务腾出空间
            if len(futures) >= MAX_FUTURES_IN_FLIGHT:
                # as_completed 会返回最先完成的 future
                done_future = next(concurrent.futures.as_completed(futures))
                futures.remove(done_future) # 从列表中移除已完成的
                
                # 处理结果
                try:
                    status, _, _ = done_future.result()
                    if status == "success": success_count += 1
                    else: failed_count += 1
                except Exception as e:
                    # 这里的异常通常是任务本身无法预料的错误
                    log_failure("unknown_url", f"executor_error: {e}")
                    failed_count += 1
                finally:
                    progress_bar.update(1)
                    progress_bar.set_postfix(success=success_count, failed=failed_count)

            # 提交新任务
            future = executor.submit(process_record, record, target_dir)
            futures.append(future)

        # --- 4. 处理剩余的 future ---
        # 当生成器耗尽后，列表中可能还有未完成的任务
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