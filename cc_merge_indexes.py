import json
import os
import requests
from tqdm import tqdm
import time
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from concurrent.futures import ThreadPoolExecutor, as_completed # <--- NEW: Import concurrent futures
from urllib.parse import urlparse

# ========== 配置区域 ==========
DOMAIN = "theguardian.com/*"
OUTPUT = "guardian_index_merged.jsonl"
OUTPUT_DIR = "guardian_batches"
INDEXES = ['CC-MAIN-2025-38', 'CC-MAIN-2025-33', 'CC-MAIN-2025-30', 'CC-MAIN-2025-26', 'CC-MAIN-2025-21', 'CC-MAIN-2025-18', 'CC-MAIN-2025-13', 'CC-MAIN-2025-08', 'CC-MAIN-2025-05', 'CC-MAIN-2024-51', 'CC-MAIN-2024-46', 'CC-MAIN-2024-42', 'CC-MAIN-2024-38', 'CC-MAIN-2024-33', 'CC-MAIN-2024-30', 'CC-MAIN-2024-26', 'CC-MAIN-2024-22', 'CC-MAIN-2024-18', 'CC-MAIN-2024-10', 'CC-MAIN-2023-50', 'CC-MAIN-2023-40', 'CC-MAIN-2023-23', 'CC-MAIN-2023-14', 'CC-MAIN-2023-06', 'CC-MAIN-2022-49', 'CC-MAIN-2022-40', 'CC-MAIN-2022-33', 'CC-MAIN-2022-27', 'CC-MAIN-2022-21', 'CC-MAIN-2022-05', 'CC-MAIN-2021-49', 'CC-MAIN-2021-43', 'CC-MAIN-2021-39', 'CC-MAIN-2021-31', 'CC-MAIN-2021-25', 'CC-MAIN-2021-21', 'CC-MAIN-2021-17', 'CC-MAIN-2021-10', 'CC-MAIN-2021-04', 'CC-MAIN-2020-50', 'CC-MAIN-2020-45', 'CC-MAIN-2020-40', 'CC-MAIN-2020-34', 'CC-MAIN-2020-29', 'CC-MAIN-2020-24', 'CC-MAIN-2020-16', 'CC-MAIN-2020-10', 'CC-MAIN-2020-05', 'CC-MAIN-2019-51', 'CC-MAIN-2019-47', 'CC-MAIN-2019-43', 'CC-MAIN-2019-39', 'CC-MAIN-2019-35', 'CC-MAIN-2019-30', 'CC-MAIN-2019-26', 'CC-MAIN-2019-22', 'CC-MAIN-2019-18', 'CC-MAIN-2019-13', 'CC-MAIN-2019-09', 'CC-MAIN-2019-04', 'CC-MAIN-2018-51', 'CC-MAIN-2018-47', 'CC-MAIN-2018-43', 'CC-MAIN-2018-39', 'CC-MAIN-2018-34', 'CC-MAIN-2018-30', 'CC-MAIN-2018-26', 'CC-MAIN-2018-22', 'CC-MAIN-2018-17', 'CC-MAIN-2018-13', 'CC-MAIN-2018-09', 'CC-MAIN-2018-05', 'CC-MAIN-2017-51', 'CC-MAIN-2017-47', 'CC-MAIN-2017-43', 'CC-MAIN-2017-39', 'CC-MAIN-2017-34', 'CC-MAIN-2017-30', 'CC-MAIN-2017-26', 'CC-MAIN-2017-22', 'CC-MAIN-2017-17', 'CC-MAIN-2017-13', 'CC-MAIN-2017-09', 'CC-MAIN-2017-04', 'CC-MAIN-2016-50', 'CC-MAIN-2016-44', 'CC-MAIN-2016-40', 'CC-MAIN-2016-36', 'CC-MAIN-2016-30', 'CC-MAIN-2016-26', 'CC-MAIN-2016-22', 'CC-MAIN-2016-18', 'CC-MAIN-2016-07', 'CC-MAIN-2015-48', 'CC-MAIN-2015-40', 'CC-MAIN-2015-35', 'CC-MAIN-2015-32', 'CC-MAIN-2015-27', 'CC-MAIN-2015-22', 'CC-MAIN-2015-18', 'CC-MAIN-2015-14', 'CC-MAIN-2015-11', 'CC-MAIN-2015-06', 'CC-MAIN-2014-52', 'CC-MAIN-2014-49', 'CC-MAIN-2014-42', 'CC-MAIN-2014-41', 'CC-MAIN-2014-35', 'CC-MAIN-2014-23', 'CC-MAIN-2014-15', 'CC-MAIN-2014-10', 'CC-MAIN-2013-48', 'CC-MAIN-2013-20', 'CC-MAIN-2012', 'CC-MAIN-2009-2010', 'CC-MAIN-2008-2009']
MAX_WORKERS = 3  # <--- NEW: Number of parallel download threads

# ========== 函数部分 ==========

# NEW: Create a dedicated function to fetch a single page. This will be run in a thread.
def fetch_page(page_url, page_num, retries=20, backoff=3):
    """
    Fetches a single page of results from the Common Crawl Index.
    Returns a list of records on success, or None on failure.
    """
    for attempt in range(1, retries + 1):
        try:
            with requests.get(page_url, stream=True, timeout=120) as resp:
                if resp.status_code == 200:
                    lines = []
                    for line in resp.iter_lines():
                        if line:
                            try:
                                lines.append(json.loads(line.decode('utf-8')))
                            except json.JSONDecodeError:
                                continue
                    return lines # Success
                else:
                    # Don't print for every attempt, only if it's getting serious
                    if attempt > 2:
                        print(f"  -> Page {page_num}, attempt {attempt}: HTTP {resp.status_code}")
                    if attempt == retries:
                        print(f"❌ 访问页面 {page_num} 失败: HTTP {resp.status_code}")
                        return None # Final failure
                    time.sleep(backoff * attempt)
        except (ChunkedEncodingError, ConnectionError, ReadTimeout) as e:
            if attempt < retries:
                time.sleep(backoff * attempt)
            else:
                print(f"❌ 放弃页面 {page_num}（多次网络失败: {e}）")
                return None # Final failure
    return None # Should not be reached, but as a fallback

# MODIFIED: The main function now orchestrates the parallel fetching of pages.
def fetch_from_index(index_name, domain, retries=20, backoff=3):
    """
    从 Common Crawl Index 并行查询指定域名的记录。
    如果任何一个分页下载失败，则整个索引的下载任务失败，返回空列表。
    """
    base_url = f"https://index.commoncrawl.org/{index_name}-index?url={domain}&output=json"
    
    # 1. 查询总页数 (remains the same)
    num_pages = 1
    try:
        page_check_url = f"{base_url}&showNumPages=true"
        print(f"正在查询 {index_name} 的总页数...")
        resp = requests.get(page_check_url, timeout=60)
        if resp.status_code == 200:
            page_info = json.loads(resp.text.strip().splitlines()[0])
            num_pages = page_info.get("pages", 1)
            print(f"  -> {index_name} 共有 {num_pages} 页")
        else:
            print(f"⚠️ 无法获取 {index_name} 的总页数，将只尝试抓取第1页。状态码: {resp.status_code}")
    except Exception as e:
        print(f"⚠️ 查询总页数时出错: {e}，将只尝试抓取第1页。")

    all_records = []
    has_failed = False

    # 2. NEW: Use ThreadPoolExecutor to fetch pages in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Create a future for each page download
        futures = {
            executor.submit(fetch_page, f"{base_url}&page={page}", page, retries, backoff): page
            for page in range(num_pages)
        }

        # Process results as they complete, with a progress bar
        progress = tqdm(as_completed(futures), total=num_pages, desc=f"  抓取页面", unit="页")
        for future in progress:
            page_result = future.result()
            if page_result is not None:
                all_records.extend(page_result)
            else:
                # If any page fails, we mark the whole index as failed.
                has_failed = True
                # Optional: Cancel remaining jobs to fail faster
                # executor.shutdown(wait=False, cancel_futures=True) 
                print(f"\n❌ 索引 {index_name} 的一个页面下载失败，将中止该索引的抓取。")
                break # Exit the loop immediately

    if has_failed:
        return [] # Return empty list if any page failed, preserving original logic

    return all_records

# ========== 主流程 ==========
# (The main loop and merging/deduplication logic remain unchanged)
os.makedirs(OUTPUT_DIR, exist_ok=True)

for idx in INDEXES:
    batch_path = os.path.join(OUTPUT_DIR, f"guardian_{idx}.jsonl")
    if os.path.exists(batch_path):
        print(f"✅ 跳过 {idx}（已存在）")
        continue

    print(f"\n===== 开始处理索引: {idx} =====")
    recs = fetch_from_index(idx, DOMAIN)
    
    if not recs:
        print(f"❌ {idx} 未抓取到任何记录或下载失败，跳过保存。")
        open(batch_path, 'w').close() # Create an empty file to mark as processed
        continue

    print(f"  -> {idx} 共抓取 {len(recs)} 条记录")

    with open(batch_path, "w", encoding="utf-8") as f:
        for rec in recs:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"✅ 已保存到 {batch_path}")


print("\n🧩 所有批次已抓取并分别保存。开始合并...")

# ========== 合并 ==========
all_records = []
for fname in tqdm(os.listdir(OUTPUT_DIR), desc="加载批次"):
    if not fname.endswith(".jsonl"):
        continue
    file_path = os.path.join(OUTPUT_DIR, fname)
    # Check if the file is empty before trying to read from it
    if os.path.getsize(file_path) > 0:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    all_records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

print(f"共加载 {len(all_records)} 条记录，正在去重...")


# ========== 去重 ==========
# (No changes needed here)
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


merged = deduplicate_records(all_records)
print(f"✅ 去重后剩余 {len(merged)} 条唯一记录")

# ========== 保存 ==========
with open(OUTPUT, "w", encoding="utf-8") as f:
    for rec in merged:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

print(f"\n✅ 已合并并保存到 {OUTPUT}")
print("下一步：可使用正文下载脚本提取网页内容。")