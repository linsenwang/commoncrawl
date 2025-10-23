# 4_extract_html_parallel.py
import os
import re
import gzip
from functools import partial
from tqdm import tqdm
from charset_normalizer import from_bytes
from multiprocessing import Pool, cpu_count

# ========== 配置 ==========
# INPUT_DIR = "guardian_world_warc_segments"
INPUT_DIR = "/Users/yangqian/Downloads/commoncrawl/sample_100/batch_0000"
OUTPUT_HTML_DIR = "sample_100_html"  # 输出目录名更改，更符合内容
LOG_FILE = "html_extraction_failed.log"

# 使用所有可用的CPU核心数减一，留一个给系统
NUM_PROCESSES = max(1, cpu_count() - 1) 

# 创建输出目录
os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)


# ========== 解析函数 (将在子进程中运行) ==========

def extract_html_from_warc(warc_bytes: bytes) -> str:
    """
    从WARC文件的原始字节中解压并提取HTML内容。
    处理gzip压缩和多种字符编码。
    """
    try:
        # 首先尝试解压
        raw_bytes = gzip.decompress(warc_bytes)
    except (OSError, gzip.BadGzipFile):
        # 如果失败（可能文件未压缩），则直接使用原始字节
        raw_bytes = warc_bytes
    
    # WARC响应通常包含两个头部分：WARC头和HTTP头
    # 我们需要跳过这两个头，找到HTTP响应体
    header_end = raw_bytes.find(b'\r\n\r\n')
    if header_end == -1:
        # 如果找不到标准分隔符，可能文件格式有问题，但仍尝试解码
        return raw_bytes.decode('utf-8', errors='ignore')

    # 寻找HTTP头的结尾
    http_header_end = raw_bytes.find(b'\r\n\r\n', header_end + 4)
    if http_header_end == -1:
        # 如果没有HTTP头，则认为头部之后都是内容
        return raw_bytes[header_end + 4:].decode('utf-8', errors='ignore')
    
    # 提取HTTP头和响应体
    http_headers_bytes = raw_bytes[header_end+4:http_header_end]
    body_bytes = raw_bytes[http_header_end+4:]

    # 尝试从HTTP头的Content-Type中获取编码
    match = re.search(br"charset=([\w\-]+)", http_headers_bytes, re.IGNORECASE)
    if match:
        try:
            encoding = match.group(1).decode('ascii')
            return body_bytes.decode(encoding, errors='ignore')
        except (LookupError, UnicodeDecodeError):
            # 如果指定的编码无效，则继续使用自动检测
            pass

    # 如果无法从头部获取编码，使用charset_normalizer进行自动检测
    result = from_bytes(body_bytes).best()
    return str(result) if result else body_bytes.decode('utf-8', errors='ignore')


def process_single_file(filename: str, input_dir: str, output_dir: str) -> dict:
    """
    处理单个文件的完整流程：读取、提取HTML、保存HTML。
    这个函数会被分发到多个进程中并行执行。
    """
    try:
        # 从 "xxxx.warc.gz" 中获取基础文件名 "xxxx"
        base_name = os.path.splitext(os.path.splitext(filename)[0])[0]
        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, f"{base_name}.html")

        # 1. 读取原始WARC文件
        with open(input_path, "rb") as f:
            warc_bytes = f.read()

        # 2. 提取HTML内容
        html_content = extract_html_from_warc(warc_bytes)
        if not html_content or not html_content.strip():
            raise ValueError("Extracted HTML content is empty.")
        
        # 3. 将提取的HTML写入新文件
        with open(output_path, "w", encoding="utf-8") as f_out:
            f_out.write(html_content)

        # 返回成功状态
        return {"status": "success", "filename": filename}

    except Exception as e:
        # 如果过程中出现任何错误，返回失败状态和原因
        return {
            "status": "error", 
            "filename": filename, 
            "reason": f"{type(e).__name__}: {str(e)}"
        }


# ========== 主进程逻辑 ==========

def get_processed_ids(dir_path: str) -> set:
    """
    通过扫描输出目录中的 .html 文件来获取已处理文件的ID。
    """
    if not os.path.exists(dir_path):
        return set()
    
    processed_ids = set()
    for f in os.listdir(dir_path):
        if f.endswith('.html'):
            # 从 "xxxx.html" 中获取 "xxxx"
            processed_ids.add(os.path.splitext(f)[0])
    return processed_ids

def main():
    if not os.path.exists(INPUT_DIR):
        print(f"错误: 输入目录 '{INPUT_DIR}' 不存在。")
        return

    processed_ids = get_processed_ids(OUTPUT_HTML_DIR)
    if processed_ids:
        print(f"已找到 {len(processed_ids)} 个已提取的HTML文件，将跳过它们。")

    all_warc_files = sorted([f for f in os.listdir(INPUT_DIR) if f.endswith('.warc.gz') and not f.startswith('._')])
    
    # 过滤掉已经处理过的文件
    files_to_process = [
        f for f in all_warc_files 
        if os.path.splitext(os.path.splitext(f)[0])[0] not in processed_ids
    ]
            
    if not files_to_process:
        print("所有文件均已处理。程序退出。")
        return

    print(f"总共找到 {len(all_warc_files)} 个WARC文件，需处理 {len(files_to_process)} 个。")
    print(f"启动 {NUM_PROCESSES} 个并行进程进行处理...")

    # 使用 multiprocessing.Pool 来并行处理文件
    with Pool(processes=NUM_PROCESSES) as pool, open(LOG_FILE, "a", encoding="utf-8") as log_f:
        # 使用 functools.partial 预先填充 process_single_file 的 input_dir 和 output_dir 参数
        worker_func = partial(process_single_file, input_dir=INPUT_DIR, output_dir=OUTPUT_HTML_DIR)
        
        # 使用 imap_unordered 来获得最佳性能，它会按完成顺序返回结果
        results_iterator = pool.imap_unordered(worker_func, files_to_process)
        
        # 使用 tqdm 显示进度
        pbar = tqdm(results_iterator, total=len(files_to_process), desc="提取HTML")
        
        for result in pbar:
            if result and result.get("status") == "error":
                # 记录失败的文件和原因
                log_f.write(f"{result['filename']}\t{result['reason']}\n")

    print(f"\n✅ HTML提取完成！")
    print(f"HTML文件已保存到目录: '{OUTPUT_HTML_DIR}'")
    print(f"失败记录已写入: '{LOG_FILE}'")

if __name__ == "__main__":
    main()