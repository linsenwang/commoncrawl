# 1_create_tasks.py
import json
import os
import requests
import time
from tqdm import tqdm
from requests.exceptions import RequestException

# ========== 配置区域 ==========
DOMAIN = "theguardian.com/*"
# 完整索引列表
INDEXES = ['CC-MAIN-2025-38', 'CC-MAIN-2025-33', 'CC-MAIN-2025-30', 'CC-MAIN-2025-26', 'CC-MAIN-2025-21', 'CC-MAIN-2025-18', 'CC-MAIN-2025-13', 'CC-MAIN-2025-08', 'CC-MAIN-2025-05', 'CC-MAIN-2024-51', 'CC-MAIN-2024-46', 'CC-MAIN-2024-42', 'CC-MAIN-2024-38', 'CC-MAIN-2024-33', 'CC-MAIN-2024-30', 'CC-MAIN-2024-26', 'CC-MAIN-2024-22', 'CC-MAIN-2024-18', 'CC-MAIN-2024-10', 'CC-MAIN-2023-50', 'CC-MAIN-2023-40', 'CC-MAIN-2023-23', 'CC-MAIN-2023-14', 'CC-MAIN-2023-06', 'CC-MAIN-2022-49', 'CC-MAIN-2022-40', 'CC-MAIN-2022-33', 'CC-MAIN-2022-27', 'CC-MAIN-2022-21', 'CC-MAIN-2022-05', 'CC-MAIN-2021-49', 'CC-MAIN-2021-43', 'CC-MAIN-2021-39', 'CC-MAIN-2021-31', 'CC-MAIN-2021-25', 'CC-MAIN-2021-21', 'CC-MAIN-2021-17', 'CC-MAIN-2021-10', 'CC-MAIN-2021-04', 'CC-MAIN-2020-50', 'CC-MAIN-2020-45', 'CC-MAIN-2020-40', 'CC-MAIN-2020-34', 'CC-MAIN-2020-29', 'CC-MAIN-2020-24', 'CC-MAIN-2020-16', 'CC-MAIN-2020-10', 'CC-MAIN-2020-05', 'CC-MAIN-2019-51', 'CC-MAIN-2019-47', 'CC-MAIN-2019-43', 'CC-MAIN-2019-39', 'CC-MAIN-2019-35', 'CC-MAIN-2019-30', 'CC-MAIN-2019-26', 'CC-MAIN-2019-22', 'CC-MAIN-2019-18', 'CC-MAIN-2019-13', 'CC-MAIN-2019-09', 'CC-MAIN-2019-04', 'CC-MAIN-2018-51', 'CC-MAIN-2018-47', 'CC-MAIN-2018-43', 'CC-MAIN-2018-39', 'CC-MAIN-2018-34', 'CC-MAIN-2018-30', 'CC-MAIN-2018-26', 'CC-MAIN-2018-22', 'CC-MAIN-2018-17', 'CC-MAIN-2018-13', 'CC-MAIN-2018-09', 'CC-MAIN-2018-05', 'CC-MAIN-2017-51', 'CC-MAIN-2017-47', 'CC-MAIN-2017-43', 'CC-MAIN-2017-39', 'CC-MAIN-2017-34', 'CC-MAIN-2017-30', 'CC-MAIN-2017-26', 'CC-MAIN-2017-22', 'CC-MAIN-2017-17', 'CC-MAIN-2017-13', 'CC-MAIN-2017-09', 'CC-MAIN-2017-04', 'CC-MAIN-2016-50', 'CC-MAIN-2016-44', 'CC-MAIN-2016-40', 'CC-MAIN-2016-36', 'CC-MAIN-2016-30', 'CC-MAIN-2016-26', 'CC-MAIN-2016-22', 'CC-MAIN-2016-18', 'CC-MAIN-2016-07', 'CC-MAIN-2015-48', 'CC-MAIN-2015-40', 'CC-MAIN-2015-35', 'CC-MAIN-2015-32', 'CC-MAIN-2015-27', 'CC-MAIN-2015-22', 'CC-MAIN-2015-18', 'CC-MAIN-2015-14', 'CC-MAIN-2015-11', 'CC-MAIN-2015-06', 'CC-MAIN-2014-52', 'CC-MAIN-2014-49', 'CC-MAIN-2014-42', 'CC-MAIN-2014-41', 'CC-MAIN-2014-35', 'CC-MAIN-2014-23', 'CC-MAIN-2014-15', 'CC-MAIN-2014-10', 'CC-MAIN-2013-48', 'CC-MAIN-2013-20', 'CC-MAIN-2012', 'CC-MAIN-2009-2010', 'CC-MAIN-2008-2009']
TASKS_FILE = "tasks.jsonl"
REQUEST_TIMEOUT = 120 # 请求超时时间

def get_processed_indexes(filename):
    """从任务文件中读取已经处理过的索引列表。"""
    processed = set()
    if not os.path.exists(filename):
        return processed
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            try:
                task = json.loads(line)
                processed.add(task['index'])
            except (json.JSONDecodeError, KeyError):
                continue
    return processed

def get_num_pages(session, index_name):
    """获取指定索引的总页数，带重试逻辑。"""
    url = f"https://index.commoncrawl.org/{index_name}-index?url={DOMAIN}&output=json&showNumPages=true"
    retries = 5
    backoff = 3
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            # Common Crawl API有时会在JSON数据前返回一行空行或非JSON文本
            for line in resp.text.strip().splitlines():
                try:
                    page_info = json.loads(line)
                    return page_info.get("pages", 1)
                except json.JSONDecodeError:
                    continue
            # 如果循环结束都没找到有效的JSON
            print(f"⚠️ 警告: {index_name} 的响应中未找到有效的JSON。")
            return 1 # 默认至少有1页
        except RequestException as e:
            print(f"❌ 获取 {index_name} 页数失败 (尝试 {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
    
    print(f"❌ 最终无法获取 {index_name} 的总页数，将跳过该索引。")
    return None

def main():
    """主函数，生成所有下载任务。"""
    print("===== 阶段 1: 生成下载任务列表 =====")
    
    # 完整性检测：找出尚未处理的索引
    processed_indexes = get_processed_indexes(TASKS_FILE)
    if processed_indexes:
        print(f"检测到 {len(processed_indexes)} 个已处理的索引，将从断点处继续。")
        
    indexes_to_query = [idx for idx in INDEXES if idx not in processed_indexes]

    if not indexes_to_query:
        print("✅ 所有索引的任务均已生成完毕！")
        print(f"任务列表保存在: {TASKS_FILE}")
        return

    print(f"需要为 {len(indexes_to_query)} 个新索引生成任务...")
    
    total_tasks_generated = 0
    with requests.Session() as session, open(TASKS_FILE, "a", encoding="utf-8") as f:
        progress = tqdm(indexes_to_query, desc="查询索引总页数")
        for index_name in progress:
            progress.set_postfix_str(index_name)
            num_pages = get_num_pages(session, index_name)

            if num_pages is not None:
                base_url = f"https://index.commoncrawl.org/{index_name}-index?url={DOMAIN}&output=json"
                tasks_for_index = []
                for page in range(num_pages):
                    task = {
                        "index": index_name,
                        "page": page,
                        "url": f"{base_url}&page={page}"
                    }
                    tasks_for_index.append(json.dumps(task, ensure_ascii=False) + "\n")
                
                # 一次性写入一个索引的所有任务，提高效率
                f.writelines(tasks_for_index)
                f.flush() # 确保写入磁盘
                total_tasks_generated += len(tasks_for_index)

    print(f"\n✅ 任务生成完成！本轮共生成 {total_tasks_generated} 个新任务。")
    print(f"完整的任务列表已保存/追加到: {TASKS_FILE}")
    print("\n下一步：请运行 `2_download_and_merge.py` 来开始下载。")

if __name__ == "__main__":
    main()