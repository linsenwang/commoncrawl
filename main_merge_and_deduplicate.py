import os
from tqdm import tqdm
import json
from urllib.parse import urlparse
OUTPUT_DIR = "guardian_batches-old"
OUTPUT = "guardian_index_all.jsonl"

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

main_merge_and_deduplicate()