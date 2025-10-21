# filter_jsonl_fast.py
import os
from tqdm import tqdm

# ========== 配置 ==========
# 原始的、包含所有记录的索引文件
SOURCE_JSONL = "/Volumes/T7/cc/guardian_index/guardian_index_all.jsonl" 
# 清理后，只包含 status: "200" 记录的输出文件
OUTPUT_JSONL = "/Volumes/T7/cc/guardian_index/guardian_index_200_only.jsonl"

def count_lines(filename):
    """高效地计算文件行数，用于tqdm进度条。"""
    try:
        with open(filename, 'rb') as f:
            lines = 0
            buf_size = 1024 * 1024 * 8 # 使用更大的缓冲区
            read_f = f.raw.read
            buf = read_f(buf_size)
            while buf:
                lines += buf.count(b'\n')
                buf = read_f(buf_size)
        return lines
    except FileNotFoundError:
        return 0

def filter_records_fast():
    """
    极速筛选版本：
    不使用 json.loads() 解析每一行，而是直接进行字符串检查。
    这对于格式固定的 JSONL 文件来说，速度极快。
    """
    print(f"开始处理源文件: {SOURCE_JSONL} (快速模式)")
    
    total_lines = count_lines(SOURCE_JSONL)
    if total_lines == 0:
        print("错误：源文件为空或不存在。")
        return

    kept_count = 0
    skipped_count = 0
    
    # 定义我们要搜索的精确子字符串
    # 检查两种常见情况：带空格和不带空格
    target_substring_1 = '"status":"200"'
    target_substring_2 = '"status": "200"'

    try:
        # 增加读写缓冲区大小，可以提高I/O性能
        buffer_size = 1024 * 1024 * 8  # 8MB buffer

        with open(SOURCE_JSONL, "r", encoding="utf-8", buffering=buffer_size) as infile, \
             open(OUTPUT_JSONL, "w", encoding="utf-8", buffering=buffer_size) as outfile:
            
            # 使用 enumerate 来减少 tqdm 更新的频率，降低开销
            progress_bar = tqdm(enumerate(infile), total=total_lines, desc="筛选记录", unit="行")
            
            for i, line in progress_bar:
                # 核心优化：使用字符串 'in' 操作，而不是 json.loads()
                if target_substring_1 in line or target_substring_2 in line:
                    outfile.write(line)
                    kept_count += 1
                else:
                    skipped_count += 1
                
                # 每 10000 行更新一次 postfix，减少tqdm的性能开销
                if i % 10000 == 0:
                    progress_bar.set_postfix_str(f"保留={kept_count}, 跳过={skipped_count}")

            # 确保最后一次的统计数据被设置
            progress_bar.set_postfix_str(f"保留={kept_count}, 跳过={skipped_count}")

    except FileNotFoundError:
        print(f"错误: 输入文件 '{SOURCE_JSONL}' 未找到。")
        return

    print("\n✅ 筛选完成!")
    print("========== 结果统计 ==========")
    print(f"  总共处理行数: {total_lines}")
    print(f"  保留的记录 (status 200): {kept_count}")
    print(f"  跳过的记录: {skipped_count}")
    print(f"  输出文件已保存至: '{OUTPUT_JSONL}'")
    print("==============================")

if __name__ == "__main__":
    filter_records_fast()