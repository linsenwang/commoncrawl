import os
from tqdm import tqdm
import json
from urllib.parse import urlparse

OUTPUT_DIR = "guardian_batches_part2"
OUTPUT = "guardian_index_part2.jsonl"
TEMP_MERGED = "guardian_index_partial.jsonl"
BATCH_SIZE = 100  # 一次处理多少个文件，可根据内存调整

def normalize_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = parsed.path.rstrip("/")
        return f"{netloc}{path}"
    except Exception:
        return url

def choose_better_record(old, new):
    if old.get("status") != "200" and new.get("status") == "200":
        return new
    if old.get("status") == "200" and new.get("status") != "200":
        return old
    old_html = "html" in (old.get("mime-detected", "") or "").lower()
    new_html = "html" in (new.get("mime-detected", "") or "").lower()
    if old_html and not new_html:
        return old
    if not old_html and new_html:
        return new
    try:
        if int(new.get("length", 0)) > int(old.get("length", 0)):
            return new
    except (ValueError, TypeError):
        pass
    if new.get("timestamp", "") > old.get("timestamp", ""):
        return new
    return old

def deduplicate_records(records, existing_map=None):
    unique = existing_map or {}
    for rec in tqdm(records, desc="去重中", leave=False):
        url = rec.get("url")
        if not url:
            continue
        key = normalize_url(url)
        if key in unique:
            unique[key] = choose_better_record(unique[key], rec)
        else:
            unique[key] = rec
    return unique

def load_jsonl_file(file_path):
    records = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records

def save_jsonl(data_dict, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        for rec in data_dict.values():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def main_merge_and_deduplicate():
    print("\n===== 阶段 3: 分批合并与去重 =====")

    if not os.path.isdir(OUTPUT_DIR):
        print(f"输出目录 {OUTPUT_DIR} 不存在，无法合并。")
        return

    batch_files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.endswith(".jsonl")])
    if not batch_files:
        print(f"在输出目录 '{OUTPUT_DIR}' 中没有找到任何 .jsonl 文件。")
        return

    global_unique = {}

    # 分批处理
    for i in range(0, len(batch_files), BATCH_SIZE):
        subset = batch_files[i:i + BATCH_SIZE]
        print(f"\n📦 处理第 {i // BATCH_SIZE + 1} 批，共 {len(subset)} 个文件")
        all_records = []
        for fname in tqdm(subset, desc="加载文件"):
            file_path = os.path.join(OUTPUT_DIR, fname)
            if os.path.getsize(file_path) == 0:
                continue
            all_records.extend(load_jsonl_file(file_path))

        print(f"→ 当前批次加载 {len(all_records)} 条记录，正在去重...")
        global_unique = deduplicate_records(all_records, existing_map=global_unique)
        print(f"→ 当前累计唯一记录数：{len(global_unique)}")

        # 中间保存一次，防止中途崩溃丢数据
        save_jsonl(global_unique, TEMP_MERGED)
        print(f"💾 已保存中间结果至 {TEMP_MERGED}")

    print("\n✅ 全部分批处理完毕，最终保存结果...")
    save_jsonl(global_unique, OUTPUT)
    print(f"✅ 已合并并保存到 {OUTPUT}")
    print("下一步：可使用正文下载脚本提取网页内容。")

if __name__ == "__main__":
    main_merge_and_deduplicate()