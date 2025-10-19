import json
import os
import requests
from tqdm import tqdm
import time
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout

# ========== 配置区域 ==========
DOMAIN = "theguardian.com/*"
OUTPUT = "guardian_index_merged.jsonl"
OUTPUT_DIR = "guardian_batches"  # 批次文件保存目录

INDEXES = ['CC-MAIN-2025-38', 'CC-MAIN-2025-33', 'CC-MAIN-2025-30', 'CC-MAIN-2025-26', 'CC-MAIN-2025-21', 'CC-MAIN-2025-18', 'CC-MAIN-2025-13', 'CC-MAIN-2025-08', 'CC-MAIN-2025-05', 'CC-MAIN-2024-51', 'CC-MAIN-2024-46', 'CC-MAIN-2024-42', 'CC-MAIN-2024-38', 'CC-MAIN-2024-33', 'CC-MAIN-2024-30', 'CC-MAIN-2024-26', 'CC-MAIN-2024-22', 'CC-MAIN-2024-18', 'CC-MAIN-2024-10', 'CC-MAIN-2023-50', 'CC-MAIN-2023-40', 'CC-MAIN-2023-23', 'CC-MAIN-2023-14', 'CC-MAIN-2023-06', 'CC-MAIN-2022-49', 'CC-MAIN-2022-40', 'CC-MAIN-2022-33', 'CC-MAIN-2022-27', 'CC-MAIN-2022-21', 'CC-MAIN-2022-05', 'CC-MAIN-2021-49', 'CC-MAIN-2021-43', 'CC-MAIN-2021-39', 'CC-MAIN-2021-31', 'CC-MAIN-2021-25', 'CC-MAIN-2021-21', 'CC-MAIN-2021-17', 'CC-MAIN-2021-10', 'CC-MAIN-2021-04', 'CC-MAIN-2020-50', 'CC-MAIN-2020-45', 'CC-MAIN-2020-40', 'CC-MAIN-2020-34', 'CC-MAIN-2020-29', 'CC-MAIN-2020-24', 'CC-MAIN-2020-16', 'CC-MAIN-2020-10', 'CC-MAIN-2020-05', 'CC-MAIN-2019-51', 'CC-MAIN-2019-47', 'CC-MAIN-2019-43', 'CC-MAIN-2019-39', 'CC-MAIN-2019-35', 'CC-MAIN-2019-30', 'CC-MAIN-2019-26', 'CC-MAIN-2019-22', 'CC-MAIN-2019-18', 'CC-MAIN-2019-13', 'CC-MAIN-2019-09', 'CC-MAIN-2019-04', 'CC-MAIN-2018-51', 'CC-MAIN-2018-47', 'CC-MAIN-2018-43', 'CC-MAIN-2018-39', 'CC-MAIN-2018-34', 'CC-MAIN-2018-30', 'CC-MAIN-2018-26', 'CC-MAIN-2018-22', 'CC-MAIN-2018-17', 'CC-MAIN-2018-13', 'CC-MAIN-2018-09', 'CC-MAIN-2018-05', 'CC-MAIN-2017-51', 'CC-MAIN-2017-47', 'CC-MAIN-2017-43', 'CC-MAIN-2017-39', 'CC-MAIN-2017-34', 'CC-MAIN-2017-30', 'CC-MAIN-2017-26', 'CC-MAIN-2017-22', 'CC-MAIN-2017-17', 'CC-MAIN-2017-13', 'CC-MAIN-2017-09', 'CC-MAIN-2017-04', 'CC-MAIN-2016-50', 'CC-MAIN-2016-44', 'CC-MAIN-2016-40', 'CC-MAIN-2016-36', 'CC-MAIN-2016-30', 'CC-MAIN-2016-26', 'CC-MAIN-2016-22', 'CC-MAIN-2016-18', 'CC-MAIN-2016-07', 'CC-MAIN-2015-48', 'CC-MAIN-2015-40', 'CC-MAIN-2015-35', 'CC-MAIN-2015-32', 'CC-MAIN-2015-27', 'CC-MAIN-2015-22', 'CC-MAIN-2015-18', 'CC-MAIN-2015-14', 'CC-MAIN-2015-11', 'CC-MAIN-2015-06', 'CC-MAIN-2014-52', 'CC-MAIN-2014-49', 'CC-MAIN-2014-42', 'CC-MAIN-2014-41', 'CC-MAIN-2014-35', 'CC-MAIN-2014-23', 'CC-MAIN-2014-15', 'CC-MAIN-2014-10', 'CC-MAIN-2013-48', 'CC-MAIN-2013-20', 'CC-MAIN-2012', 'CC-MAIN-2009-2010', 'CC-MAIN-2008-2009']

# ========== 函数部分 ==========
# def fetch_from_index(index_name, domain):
#     url = f"https://index.commoncrawl.org/{index_name}-index?url={domain}&output=json"
#     print(f"正在查询 {index_name} ...")
#     resp = requests.get(url, timeout=60)
#     if resp.status_code != 200:
#         print(f"❌ 无法访问 {index_name}: {resp.status_code}")
#         return []
#     lines = resp.text.strip().splitlines()
#     return [json.loads(line) for line in lines if line.strip()]

def fetch_from_index(index_name, domain, retries=5, backoff=3):
    """
    从 Common Crawl Index 查询指定域名的记录，带自动重试与容错。
    """
    url = f"https://index.commoncrawl.org/{index_name}-index?url={domain}&output=json"
    print(f"正在查询 {index_name} ...")

    for attempt in range(1, retries + 1):
        try:
            # 使用流式请求防止一次性加载大响应
            with requests.get(url, stream=True, timeout=60) as resp:
                if resp.status_code != 200:
                    print(f"❌ 无法访问 {index_name}: HTTP {resp.status_code}")
                    return []

                lines = []
                for line in resp.iter_lines():
                    if line:
                        try:
                            lines.append(json.loads(line.decode('utf-8')))
                        except json.JSONDecodeError:
                            # 如果部分行损坏，跳过
                            continue
                return lines

        except (ChunkedEncodingError, ConnectionError, ReadTimeout) as e:
            print(f"⚠️ 第 {attempt}/{retries} 次请求失败: {e}")
            if attempt < retries:
                wait = backoff * attempt
                print(f"⏳ {wait}s 后重试...")
                time.sleep(wait)
            else:
                print(f"❌ 放弃 {index_name}（多次失败）")
                return []

# ========== 主流程 ==========
os.makedirs(OUTPUT_DIR, exist_ok=True)

for idx in INDEXES:
    batch_path = os.path.join(OUTPUT_DIR, f"guardian_{idx}.jsonl")
    if os.path.exists(batch_path):
        print(f"✅ 跳过 {idx}（已存在）")
        continue

    recs = fetch_from_index(idx, DOMAIN)
    print(f"  -> {idx} 共 {len(recs)} 条")

    with open(batch_path, "w", encoding="utf-8") as f:
        for rec in recs:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

print("\n🧩 所有批次已抓取并分别保存。开始合并...")

# ========== 合并 ==========
all_records = []
for fname in tqdm(os.listdir(OUTPUT_DIR), desc="加载批次"):
    if not fname.endswith(".jsonl"):
        continue
    with open(os.path.join(OUTPUT_DIR, fname), "r", encoding="utf-8") as f:
        for line in f:
            try:
                all_records.append(json.loads(line))
            except:
                continue

print(f"共加载 {len(all_records)} 条记录，正在去重...")

# ========== 去重 ==========
# unique = {}
# for rec in tqdm(all_records, desc="去重中"):
#     url = rec.get("url")
#     if url and url not in unique:
#         unique[url] = rec

# merged = list(unique.values())
import re
from tqdm import tqdm
from urllib.parse import urlparse

def normalize_url(url: str) -> str:
    """标准化 URL：去掉 http/https、末尾斜杠、www 等"""
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    # 去掉 www.
    if netloc.startswith("www."):
        netloc = netloc[4:]
    # 去掉末尾斜杠
    path = parsed.path.rstrip("/")
    return f"{netloc}{path}"

def choose_better_record(old, new):
    """比较两条记录，返回质量更高的一条"""
    # 1. 优先保留 status=200
    if old.get("status") != "200" and new.get("status") == "200":
        return new
    if old.get("status") == "200" and new.get("status") != "200":
        return old

    # 2. 优先保留 HTML 页面
    old_html = "html" in (old.get("mime-detected", "") or "").lower()
    new_html = "html" in (new.get("mime-detected", "") or "").lower()
    if old_html != new_html:
        return new if new_html else old

    # 3. 优先保留长度更大的（更完整）
    try:
        if int(new.get("length", 0)) > int(old.get("length", 0)):
            return new
    except ValueError:
        pass

    # 4. 否则保留最新 timestamp（更接近最近抓取）
    if new.get("timestamp", "") > old.get("timestamp", ""):
        return new

    return old

def deduplicate_records(records):
    unique = {}
    for rec in tqdm(records, desc="去重中"):
        url = rec.get("url")
        if not url:
            continue
        key = normalize_url(url)
        if key in unique:
            unique[key] = choose_better_record(unique[key], rec)
        else:
            unique[key] = rec
    return list(unique.values())


# ✅ 使用方法：
merged = deduplicate_records(all_records)
print(f"✅ 去重后剩余 {len(merged)} 条唯一记录")

# ========== 保存 ==========
with open(OUTPUT, "w", encoding="utf-8") as f:
    for rec in merged:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

print(f"\n✅ 已合并并保存到 {OUTPUT}")
print("下一步：可使用正文下载脚本提取网页内容。")