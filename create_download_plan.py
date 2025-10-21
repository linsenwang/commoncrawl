# create_download_plan.py
import json
from collections import defaultdict
from tqdm import tqdm

# ========== 配置 ==========
# 输入：CDX索引文件
INPUT_JSONL = "guardian_index_part2.jsonl"
# 输出：下载计划文件
OUTPUT_PLAN_JSON = "download_plan.json"

def create_plan():
    """
    读取CDX索引，按WARC文件分组，生成下载和提取计划。
    """
    print(f"正在从 '{INPUT_JSONL}' 创建下载计划...")
    
    # 使用 defaultdict 可以方便地将记录追加到列表中
    # 结构: { "warc_filename": [ { "offset": o, "length": l, "url": u }, ... ] }
    warc_file_groups = defaultdict(list)
    total_records = 0
    valid_records = 0

    try:
        with open(INPUT_JSONL, "r", encoding="utf-8") as f:
            # 首先计算总行数以用于tqdm进度条
            num_lines = sum(1 for line in f)
            f.seek(0) # 重置文件指针
            
            for line in tqdm(f, total=num_lines, desc="分析索引"):
                if not line.strip():
                    continue
                
                total_records += 1
                try:
                    record = json.loads(line)
                    
                    # 仅处理状态码为 '200' 的成功记录
                    if record.get("status") == "200" and "url" in record:
                        warc_filename = record["filename"]
                        segment_info = {
                            "offset": int(record["offset"]),
                            "length": int(record["length"]),
                            "url": record["url"]
                        }
                        warc_file_groups[warc_filename].append(segment_info)
                        valid_records += 1
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"警告: 跳过格式错误的行: {line.strip()} - 错误: {e}")

    except FileNotFoundError:
        print(f"错误: 输入文件 '{INPUT_JSONL}' 未找到。")
        return

    print("\n分析完成。")
    print(f"  总共处理记录: {total_records}")
    print(f"  有效记录 (status=200): {valid_records}")
    print(f"  需要下载的独立WARC文件数: {len(warc_file_groups)}")

    # 保存计划到JSON文件
    print(f"正在将计划写入 '{OUTPUT_PLAN_JSON}'...")
    with open(OUTPUT_PLAN_JSON, "w", encoding="utf-8") as f:
        json.dump(warc_file_groups, f, indent=2)

    print("✅ 计划创建成功！现在可以运行 `execute_plan.py`。")

if __name__ == "__main__":
    create_plan()