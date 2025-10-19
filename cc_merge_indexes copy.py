import json
import requests
from tqdm import tqdm

# ========== 配置区域 ==========
DOMAIN = "theguardian.com/world/*"
OUTPUT = "guardian_index.jsonl"

# 想抓取的批次（可以按需添加或删减）
INDEXES = ['CC-MAIN-2025-38', 'CC-MAIN-2025-33', 'CC-MAIN-2025-30', 'CC-MAIN-2025-26', 'CC-MAIN-2025-21', 'CC-MAIN-2025-18', 'CC-MAIN-2025-13', 'CC-MAIN-2025-08', 'CC-MAIN-2025-05', 'CC-MAIN-2024-51', 'CC-MAIN-2024-46', 'CC-MAIN-2024-42', 'CC-MAIN-2024-38', 'CC-MAIN-2024-33', 'CC-MAIN-2024-30', 'CC-MAIN-2024-26', 'CC-MAIN-2024-22', 'CC-MAIN-2024-18', 'CC-MAIN-2024-10', 'CC-MAIN-2023-50', 'CC-MAIN-2023-40', 'CC-MAIN-2023-23', 'CC-MAIN-2023-14', 'CC-MAIN-2023-06', 'CC-MAIN-2022-49', 'CC-MAIN-2022-40', 'CC-MAIN-2022-33', 'CC-MAIN-2022-27', 'CC-MAIN-2022-21', 'CC-MAIN-2022-05', 'CC-MAIN-2021-49', 'CC-MAIN-2021-43', 'CC-MAIN-2021-39', 'CC-MAIN-2021-31', 'CC-MAIN-2021-25', 'CC-MAIN-2021-21', 'CC-MAIN-2021-17', 'CC-MAIN-2021-10', 'CC-MAIN-2021-04', 'CC-MAIN-2020-50', 'CC-MAIN-2020-45', 'CC-MAIN-2020-40', 'CC-MAIN-2020-34', 'CC-MAIN-2020-29', 'CC-MAIN-2020-24', 'CC-MAIN-2020-16', 'CC-MAIN-2020-10', 'CC-MAIN-2020-05', 'CC-MAIN-2019-51', 'CC-MAIN-2019-47', 'CC-MAIN-2019-43', 'CC-MAIN-2019-39', 'CC-MAIN-2019-35', 'CC-MAIN-2019-30', 'CC-MAIN-2019-26', 'CC-MAIN-2019-22', 'CC-MAIN-2019-18', 'CC-MAIN-2019-13', 'CC-MAIN-2019-09', 'CC-MAIN-2019-04', 'CC-MAIN-2018-51', 'CC-MAIN-2018-47', 'CC-MAIN-2018-43', 'CC-MAIN-2018-39', 'CC-MAIN-2018-34', 'CC-MAIN-2018-30', 'CC-MAIN-2018-26', 'CC-MAIN-2018-22', 'CC-MAIN-2018-17', 'CC-MAIN-2018-13', 'CC-MAIN-2018-09', 'CC-MAIN-2018-05', 'CC-MAIN-2017-51', 'CC-MAIN-2017-47', 'CC-MAIN-2017-43', 'CC-MAIN-2017-39', 'CC-MAIN-2017-34', 'CC-MAIN-2017-30', 'CC-MAIN-2017-26', 'CC-MAIN-2017-22', 'CC-MAIN-2017-17', 'CC-MAIN-2017-13', 'CC-MAIN-2017-09', 'CC-MAIN-2017-04', 'CC-MAIN-2016-50', 'CC-MAIN-2016-44', 'CC-MAIN-2016-40', 'CC-MAIN-2016-36', 'CC-MAIN-2016-30', 'CC-MAIN-2016-26', 'CC-MAIN-2016-22', 'CC-MAIN-2016-18', 'CC-MAIN-2016-07', 'CC-MAIN-2015-48', 'CC-MAIN-2015-40', 'CC-MAIN-2015-35', 'CC-MAIN-2015-32', 'CC-MAIN-2015-27', 'CC-MAIN-2015-22', 'CC-MAIN-2015-18', 'CC-MAIN-2015-14', 'CC-MAIN-2015-11', 'CC-MAIN-2015-06', 'CC-MAIN-2014-52', 'CC-MAIN-2014-49', 'CC-MAIN-2014-42', 'CC-MAIN-2014-41', 'CC-MAIN-2014-35', 'CC-MAIN-2014-23', 'CC-MAIN-2014-15', 'CC-MAIN-2014-10', 'CC-MAIN-2013-48', 'CC-MAIN-2013-20', 'CC-MAIN-2012', 'CC-MAIN-2009-2010', 'CC-MAIN-2008-2009']

# ========== 主函数 ==========
def fetch_from_index(index_name, domain):
    url = f"https://index.commoncrawl.org/{index_name}-index?url={domain}&output=json"
    print(f"正在查询 {index_name} ...")
    resp = requests.get(url, timeout=60)
    if resp.status_code != 200:
        print(f"❌ 无法访问 {index_name}: {resp.status_code}")
        return []
    lines = resp.text.strip().splitlines()
    return [json.loads(line) for line in lines if line.strip()]

# ========== 抓取多个批次 ==========
all_records = []
for idx in INDEXES:
    recs = fetch_from_index(idx, DOMAIN)
    print(f"  -> {idx} 共 {len(recs)} 条")
    all_records.extend(recs)

print(f"\n共抓取 {len(all_records)} 条原始记录")

# ========== 去重（按 URL） ==========
unique = {}
for rec in tqdm(all_records, desc="去重中"):
    url = rec.get("url")
    if url and url not in unique:
        unique[url] = rec

merged = list(unique.values())
print(f"✅ 去重后剩余 {len(merged)} 条唯一记录")

# ========== 保存 ==========
with open(OUTPUT, "w", encoding="utf-8") as f:
    for rec in merged:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

print(f"\n✅ 已保存到 {OUTPUT}")
print("下一步：可使用前一个脚本从索引下载正文。")