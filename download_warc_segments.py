# 1_download_warc_segments.py
import os
import json
import hashlib
import requests
import time
from tqdm import tqdm
from requests.exceptions import RequestException, ChunkedEncodingError, ConnectionError, ReadTimeout

# ========== 配置 ==========
# 输入：CDX索引文件
INPUT_JSONL = "guardian_index_world_merged.jsonl"
# 输出：存放原始WARC片段的目录
OUTPUT_DIR = "guardian_world_warc_segments"
# 日志：记录下载失败的URL
LOG_FILE = "download_failed.log"
# Common Crawl 基础URL
BASE_URL = "https://data.commoncrawl.org/"

# 创建输出目录
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ========== 工具函数 ==========
def safe_filename(url: str) -> str:
    """将URL转换为安全的、带.warc.gz后缀的文件名"""
    h = hashlib.md5(url.encode()).hexdigest()
    return f"{h}.warc.gz"


def fetch_segment(record, retries=3, backoff=2):
    """
    根据CDX记录下载对应的网页WARC片段（原始压缩格式）。
    """
    warc_path = record["filename"]
    offset = int(record["offset"])
    length = int(record["length"])
    end = offset + length - 1

    warc_url = BASE_URL + warc_path
    headers = {"Range": f"bytes={offset}-{end}"}

    for attempt in range(retries):
        try:
            # 使用流式传输和较长的超时时间，以应对大文件和慢网络
            with requests.get(warc_url, headers=headers, timeout=(10, 60), stream=True) as resp:
                resp.raise_for_status()
                # 检查响应大小是否与预期一致
                if int(resp.headers.get('Content-Length', 0)) != length:
                     raise RequestException(f"Incomplete download. Expected {length} bytes, got {resp.headers.get('Content-Length')}")
                return resp.content
        except (ChunkedEncodingError, ConnectionError, ReadTimeout, RequestException) as e:
            print(f"\nAttempt {attempt + 1} for {record['url']} failed: {e}")
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt)) # 指数退避
            else:
                # 所有重试失败后，抛出异常
                raise e


def log_failure(url, reason):
    """记录失败的URL和原因"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{url}\t{reason}\n")


# ========== 主逻辑 ==========
def main():
    try:
        with open(INPUT_JSONL, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        print(f"错误: 输入文件 '{INPUT_JSONL}' 未找到。")
        return
        
    print(f"共找到 {len(lines)} 条索引记录。开始下载原始WARC片段...")

    for record in tqdm(lines, desc="下载进度"):
        url = record.get("url")
        status = record.get("status")

        # 跳过非200状态码的记录
        if status != "200" or not url:
            continue

        filename = safe_filename(url)
        output_path = os.path.join(OUTPUT_DIR, filename)

        # 如果文件已存在，则跳过，实现断点续传
        if os.path.exists(output_path):
            continue

        try:
            # 下载原始的、可能被压缩的WARC字节数据
            warc_bytes = fetch_segment(record)

            # 以二进制模式写入文件，保留原始压缩格式
            with open(output_path, "wb") as f:
                f.write(warc_bytes)

            # 友好的网络请求间隔
            time.sleep(0.1) 
            
        except RequestException as e:
            log_failure(url, f"request_error: {str(e)}")
        except Exception as e:
            # 捕获其他潜在错误
            log_failure(url, f"unexpected_error: {str(e)}")

    print(f"\n✅ 下载完成！失败记录已写入 '{LOG_FILE}'")


if __name__ == "__main__":
    main()