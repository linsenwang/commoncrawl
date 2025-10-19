import os
import json
import hashlib
import requests
import time
import re
import io
import gzip
from tqdm import tqdm
from requests.exceptions import RequestException, ChunkedEncodingError, ConnectionError, ReadTimeout
from charset_normalizer import from_bytes

# ========== 配置 ==========
INPUT_JSONL = "guardian_index_world_merged.jsonl"
OUTPUT_DIR = "guardian_world_pages"
LOG_FILE = "failed.log"
BASE_URL = "https://data.commoncrawl.org/"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ========== 工具函数 ==========
def safe_filename(url: str) -> str:
    """将URL转换为安全文件名"""
    h = hashlib.md5(url.encode()).hexdigest()
    return f"{h}.html"


def fetch_segment(record, retries=3, backoff=2):
    """根据CDX记录下载对应的网页片段"""
    warc_path = record["filename"]
    offset = int(record["offset"])
    length = int(record["length"])
    end = offset + length - 1

    warc_url = BASE_URL + warc_path
    headers = {"Range": f"bytes={offset}-{end}"}

    for attempt in range(retries):
        try:
            resp = requests.get(warc_url, headers=headers, timeout=60)
            resp.raise_for_status()
            return resp.content
        except (ChunkedEncodingError, ConnectionError, ReadTimeout, RequestException) as e:
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
            else:
                raise e


def maybe_decompress(warc_bytes: bytes) -> bytes:
    """尝试解压 gzip 数据"""
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(warc_bytes)) as f:
            return f.read()
    except OSError:
        # 不是 gzip 格式，原样返回
        return warc_bytes


def extract_http_payload(warc_bytes: bytes):
    """从WARC响应中提取HTML正文，并自动检测编码"""
    try:
        # Step 1: gzip 解压
        raw_bytes = maybe_decompress(warc_bytes)

        # Step 2: 解码 header + body
        text = raw_bytes.decode("iso-8859-1", errors="ignore")
        parts = text.split("\r\n\r\n", 2)
        if len(parts) < 3:
            return text

        http_headers = parts[1]
        body_bytes = parts[2].encode("iso-8859-1", errors="ignore")

        # Step 3: 从 HTTP 头提取 charset
        match = re.search(r"charset=([\w\-]+)", http_headers, re.IGNORECASE)
        if match:
            encoding = match.group(1)
            try:
                return body_bytes.decode(encoding, errors="ignore")
            except Exception:
                pass

        # Step 4: 自动检测编码
        result = from_bytes(body_bytes).best()
        if result:
            return str(result)
        else:
            return body_bytes.decode("utf-8", errors="ignore")

    except Exception as e:
        return warc_bytes.decode("utf-8", errors="ignore")


def log_failure(url, reason):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{url}\t{reason}\n")


# ========== 主逻辑 ==========
def main():
    with open(INPUT_JSONL, "r", encoding="utf-8") as f:
        lines = [json.loads(line) for line in f if line.strip()]

    print(f"共 {len(lines)} 条索引记录。开始下载……")

    for record in tqdm(lines, desc="下载进度"):
        url = record.get("url")
        status = record.get("status")

        # 跳过非 200 状态
        if status != "200":
            continue

        filename = safe_filename(url)
        output_path = os.path.join(OUTPUT_DIR, filename)

        # 已下载跳过
        if os.path.exists(output_path):
            continue

        try:
            warc_bytes = fetch_segment(record)
            html = extract_http_payload(warc_bytes)

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html)

            time.sleep(0.2)
        except RequestException as e:
            log_failure(url, f"request_error: {str(e)}")
        except Exception as e:
            log_failure(url, f"parse_error: {e}")

    print("✅ 下载完成！失败记录已写入 failed.log")


if __name__ == "__main__":
    main()