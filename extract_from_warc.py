# 2_extract_from_warc.py
import os
import re
import io
import gzip
from tqdm import tqdm
from charset_normalizer import from_bytes
from bs4 import BeautifulSoup

# ========== 配置 ==========
# 输入：存放原始WARC片段的目录
INPUT_DIR = "guardian_world_warc_segments"
# 输出：存放提取出的干净HTML文件的目录
OUTPUT_HTML_DIR = "guardian_world_html"
# 输出：存放提取出的纯文本内容的目录
OUTPUT_TEXT_DIR = "guardian_world_text"
# 日志：记录解析或提取失败的文件
LOG_FILE = "extraction_failed.log"

# 创建输出目录
os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)
os.makedirs(OUTPUT_TEXT_DIR, exist_ok=True)


# ========== 工具函数 ==========
def maybe_decompress(warc_bytes: bytes) -> bytes:
    """尝试解压 gzip 数据，如果失败则原样返回"""
    try:
        # 使用 BytesIO 作为文件对象来处理内存中的字节数据
        with gzip.GzipFile(fileobj=io.BytesIO(warc_bytes)) as f:
            return f.read()
    except (OSError, gzip.BadGzipFile):
        # 不是有效的 gzip 格式，可能已经是解压后的数据
        return warc_bytes


def extract_html_from_warc(warc_bytes: bytes) -> str:
    """
    从WARC响应中提取HTML正文，并自动检测和转换编码。
    """
    # 1. 解压 Gzip
    raw_bytes = maybe_decompress(warc_bytes)

    # 2. 分离HTTP头和正文
    # WARC响应通常包含WARC头、HTTP头和HTTP正文，用\r\n\r\n分隔
    header_end = raw_bytes.find(b'\r\n\r\n')
    if header_end == -1:
        # 如果找不到分隔符，可能格式有问题，尝试作为纯文本解码
        return raw_bytes.decode('utf-8', errors='ignore')

    # 再次查找，以分离HTTP头和HTML正文
    http_header_end = raw_bytes.find(b'\r\n\r\n', header_end + 4)
    if http_header_end == -1:
        return raw_bytes.decode('utf-8', errors='ignore')

    http_headers_bytes = raw_bytes[header_end+4:http_header_end]
    body_bytes = raw_bytes[http_header_end+4:]

    # 3. 确定编码并解码
    encoding = None
    # 从HTTP头中用正则查找charset
    match = re.search(br"charset=([\w\-]+)", http_headers_bytes, re.IGNORECASE)
    if match:
        try:
            encoding = match.group(1).decode('ascii')
            return body_bytes.decode(encoding, errors='ignore')
        except (LookupError, UnicodeDecodeError):
            pass  # 如果编码无效，则继续往下走

    # 如果HTTP头中没有或编码无效，使用charset_normalizer自动检测
    result = from_bytes(body_bytes).best()
    if result:
        return str(result)
    
    # 最终回退到UTF-8
    return body_bytes.decode('utf-8', errors='ignore')

def extract_article_text(html: str) -> str:
    """
    使用BeautifulSoup从HTML中提取主要文章内容。
    这是一个通用示例，针对特定网站（如The Guardian）可以优化。
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    # 针对The Guardian网站的优化选择器 (截至2023年常见结构)
    # 尝试找到文章主要内容容器
    article_body = soup.find('div', attrs={'itemprop': 'articleBody'})
    if not article_body:
        article_body = soup.find('div', class_=re.compile(r'content__article-body'))
    
    # 如果找不到特定结构，回退到通用标签
    if not article_body:
        article_body = soup.find('article') or soup.find('main') or soup.body

    if article_body:
        # 移除脚本和样式标签
        for element in article_body(["script", "style"]):
            element.decompose()
        # 获取文本，用换行符分隔，并去除多余空白
        text = article_body.get_text(separator='\n', strip=True)
        return text
    
    return "" # 如果无法提取，返回空字符串


def log_failure(filename, reason):
    """记录失败的文件和原因"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{filename}\t{reason}\n")


# ========== 主逻辑 ==========
def main():
    if not os.path.exists(INPUT_DIR):
        print(f"错误: 输入目录 '{INPUT_DIR}' 不存在。请先运行 `1_download_warc_segments.py`。")
        return
        
    warc_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.warc.gz')]
    print(f"找到 {len(warc_files)} 个WARC文件。开始提取内容...")

    for filename in tqdm(warc_files, desc="处理进度"):
        base_name = os.path.splitext(os.path.splitext(filename)[0])[0]
        html_output_path = os.path.join(OUTPUT_HTML_DIR, f"{base_name}.html")
        text_output_path = os.path.join(OUTPUT_TEXT_DIR, f"{base_name}.txt")

        # 如果两种产出文件都已存在，则跳过
        if os.path.exists(html_output_path) and os.path.exists(text_output_path):
            continue

        input_path = os.path.join(INPUT_DIR, filename)
        
        try:
            # 1. 读取原始WARC字节
            with open(input_path, "rb") as f:
                warc_bytes = f.read()

            # 2. 从WARC中提取HTML字符串
            html_content = extract_html_from_warc(warc_bytes)
            if not html_content.strip():
                raise ValueError("Extracted HTML is empty.")

            # 3. 将完整的HTML保存下来（用于调试或未来分析）
            with open(html_output_path, "w", encoding="utf-8") as f:
                f.write(html_content)
                
            # 4. 从HTML中提取干净的纯文本
            article_text = extract_article_text(html_content)
            if not article_text.strip():
                raise ValueError("Extracted article text is empty.")

            # 5. 保存纯文本
            with open(text_output_path, "w", encoding="utf-8") as f:
                f.write(article_text)

        except Exception as e:
            log_failure(filename, f"{type(e).__name__}: {str(e)}")

    print(f"\n✅ 内容提取完成！失败记录已写入 '{LOG_FILE}'")

if __name__ == "__main__":
    main()