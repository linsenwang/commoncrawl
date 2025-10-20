# 2_download_and_merge.py
import json
import os
import requests
import time
from tqdm import tqdm
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

# ========== 配置区域 ==========
# 此处配置应与脚本1保持一致，或只保留下载和输出相关配置
OUTPUT = "guardian_index_merged.jsonl"
OUTPUT_DIR = "guardian_batches"
TASKS_FILE = "tasks.jsonl"
COMPLETED_LOG_FILE = "completed_tasks.log"

MAX_WORKERS = 30
REQUEST_TIMEOUT = 120

# ========== 下载函数 (无变动) ==========

def fetch_page(session, task):
    """
    Fetches a single page of results.
    Returns a tuple: (task, list_of_records) on success,
                     (task, error_string) on failure.
    """
    page_url = task['url']
    retries = 5
    backoff = 3

    for attempt in range(1, retries + 1):
        try:
            with session.get(page_url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status_code == 200:
                    lines = []
                    for line in resp.iter_lines():
                        if line:
                            try:
                                lines.append(json.loads(line.decode('utf-8')))
                            except json.JSONDecodeError:
                                continue
                    return (task, lines)
                else:
                    error_msg = f"HTTP {resp.status_code}"
                    if resp.status_code in [404, 400]:
                        return (task, f"Fatal error: {error_msg}")
                    time.sleep(backoff * attempt)
        except RequestException as e:
            error_msg = str(e)
            time.sleep(backoff * attempt)

    return (task, f"Failed after {retries} attempts: {error_msg}")


def main_downloader():
    """
    下载器主流程，具备断点续传和自动重试功能。
    【新】每个页面下载成功后立即保存为独立文件。
    【新】通过扫描已有文件和日志来判断完成状态，实现更强的鲁棒性。
    """
    print("===== 阶段 2: 执行下载 =====")
    
    # --- 完整性检测与任务加载 ---
    if not os.path.exists(TASKS_FILE):
        print(f"❌ 错误: 任务文件 '{TASKS_FILE}' 不存在。")
        print("请先运行 `1_create_tasks.py` 来生成任务列表。")
        return False

    print("正在加载任务列表...")
    all_tasks = {}
    with open(TASKS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            task = json.loads(line)
            task_id = f"{task['index']}_{task['page']}"
            all_tasks[task_id] = task
    
    # --- 【新】更鲁棒的已完成任务检测 ---
    completed_tasks_ids = set()
    
    # 1. 通过扫描输出目录下的文件来确定已完成任务
    print(f"正在扫描目录 '{OUTPUT_DIR}' 以检测已下载的文件...")
    if os.path.isdir(OUTPUT_DIR):
        for filename in os.listdir(OUTPUT_DIR):
            if filename.startswith("page_") and filename.endswith(".jsonl"):
                filepath = os.path.join(OUTPUT_DIR, filename)
                # 确保文件不是空的
                if os.path.getsize(filepath) > 0:
                    # 从文件名 'page_{index}_{page}.jsonl' 解析出 task_id
                    task_id = filename[5:-6] 
                    completed_tasks_ids.add(task_id)
    print(f"通过扫描文件，找到 {len(completed_tasks_ids)} 个已完成的任务。")

    # 2. 从日志文件中补充已完成任务记录（作为补充和兼容）
    if os.path.exists(COMPLETED_LOG_FILE):
        initial_count = len(completed_tasks_ids)
        with open(COMPLETED_LOG_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                completed_tasks_ids.add(line.strip())
        added_from_log = len(completed_tasks_ids) - initial_count
        if added_from_log > 0:
            print(f"从日志文件 '{COMPLETED_LOG_FILE}' 中额外加载了 {added_from_log} 条完成记录。")

    tasks_to_do = [task for task_id, task in all_tasks.items() if task_id not in completed_tasks_ids]

    if not tasks_to_do:
        print("✅ 所有任务均已下载完成！")
        return True

    print(f"共 {len(all_tasks)} 个任务，其中 {len(completed_tasks_ids)} 个已完成。")
    print(f"本轮需要下载 {len(tasks_to_do)} 个任务页。")
    
    # --- 持久化下载循环 (此部分逻辑不变) ---
    adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session = requests.Session()
    session.mount('https://', adapter)
    
    attempt = 1
    backoff_time = 10

    with open(COMPLETED_LOG_FILE, "a", encoding="utf-8") as log_file:
        while tasks_to_do:
            print(f"\n--- 第 {attempt} 轮下载尝试 ---")
            print(f"待处理页面数: {len(tasks_to_do)}")
            
            tasks_failed_this_run = []
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_task = {executor.submit(fetch_page, session, task): task for task in tasks_to_do}
                progress = tqdm(as_completed(future_to_task), total=len(tasks_to_do), desc=f"下载中 (第 {attempt} 轮)")
                
                tasks_completed_this_batch = []

                for future in progress:
                    task_done, result = future.result()
                    task_id = f"{task_done['index']}_{task_done['page']}"
                    
                    if isinstance(result, list):
                        page_filename = f"page_{task_id}.jsonl"
                        page_filepath = os.path.join(OUTPUT_DIR, page_filename)
                        
                        try:
                            with open(page_filepath, "w", encoding="utf-8") as f:
                                for rec in result:
                                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                            tasks_completed_this_batch.append(task_id)
                            progress.set_postfix_str(f"已保存 {page_filename}")
                        except IOError:
                            tasks_failed_this_run.append(task_done)
                            progress.set_postfix_str(f"文件保存失败: {page_filename}")
                    else:
                        tasks_failed_this_run.append(task_done)
                        progress.set_postfix_str(f"下载失败: {task_id}")
            
            if tasks_completed_this_batch:
                print(f"本轮成功 {len(tasks_completed_this_batch)} 页，正在记录进度...")
                for task_id in tasks_completed_this_batch:
                    log_file.write(task_id + "\n")
                log_file.flush()

            tasks_to_do = tasks_failed_this_run
            
            if tasks_to_do:
                print(f"❌ {len(tasks_to_do)} 个页面处理失败。将在 {backoff_time} 秒后重试...")
                time.sleep(backoff_time)
                attempt += 1
                backoff_time = min(backoff_time * 1.5, 300)
            else:
                print("\n🎉🎉🎉 所有页面均已成功下载！🎉🎉🎉")
                break
    
    return True

# ========== 合并与去重函数 (无变动) ==========
def normalize_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = parsed.path.rstrip("/")
        return f"{netloc}{path}"
    except:
        return url

def choose_better_record(old, new):
    if old.get("status") != "200" and new.get("status") == "200": return new
    if old.get("status") == "200" and new.get("status") != "200": return old
    old_html = "html" in (old.get("mime-detected", "") or "").lower()
    new_html = "html" in (new.get("mime-detected", "") or "").lower()
    if old_html and not new_html: return old
    if not old_html and new_html: return new
    try:
        if int(new.get("length", 0)) > int(old.get("length", 0)): return new
    except (ValueError, TypeError): pass
    if new.get("timestamp", "") > old.get("timestamp", ""): return new
    return old

def deduplicate_records(records):
    unique = {}
    for rec in tqdm(records, desc="去重中"):
        url = rec.get("url")
        if not url: continue
        key = normalize_url(url)
        if key in unique:
            unique[key] = choose_better_record(unique[key], rec)
        else:
            unique[key] = rec
    return list(unique.values())


def main_merge_and_deduplicate():
    print("\n===== 阶段 3: 合并与去重 =====")

    all_records = []
    if not os.path.isdir(OUTPUT_DIR):
        print(f"输出目录 {OUTPUT_DIR} 不存在，无法合并。")
        return
        
    # 读取所有 .jsonl 文件，不论其前缀是什么
    batch_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".jsonl")]
    if not batch_files:
        print(f"在输出目录 '{OUTPUT_DIR}' 中没有找到任何 .jsonl 文件，无法合并。")
        return

    for fname in tqdm(batch_files, desc="加载批次文件"):
        file_path = os.path.join(OUTPUT_DIR, fname)
        if os.path.getsize(file_path) > 0:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        all_records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

    if not all_records:
        print("没有加载到任何记录，程序结束。")
        return
        
    print(f"共加载 {len(all_records)} 条记录，正在去重...")

    merged = deduplicate_records(all_records)
    print(f"✅ 去重后剩余 {len(merged)} 条唯一记录")

    with open(OUTPUT, "w", encoding="utf-8") as f:
        for rec in merged:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"\n✅ 已合并并保存到 {OUTPUT}")
    print("下一步：可使用正文下载脚本提取网页内容。")


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 阶段二：执行下载循环
    download_successful = main_downloader()
    
    # 阶段三：仅在下载成功后执行合并与去重
    if download_successful:
        main_merge_and_deduplicate()