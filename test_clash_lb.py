import requests
import time
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- ANSI 颜色代码 ---
class Color:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'

# --- 用于获取公网IP的API列表 ---
# 我们使用多个API以增加成功率
IP_CHECK_APIS = [
    "https://api.ipify.org",
    "https://ipinfo.io/ip",
    "https://icanhazip.com",
    "https://ifconfig.me/ip",
]

def get_public_ip(session, proxies):
    """
    通过代理请求一个API来获取当前的公网IP地址。
    关键：使用 'Connection': 'close' 请求头来强制创建新连接。
    """
    headers = {
        'User-Agent': 'Clash-Load-Balance-Tester/1.0',
        'Connection': 'close'  # 这是触发Clash负载均衡的关键！
    }
    
    for api_url in IP_CHECK_APIS:
        try:
            # 设置一个较短的超时，因为这些API通常很快
            with session.get(api_url, proxies=proxies, headers=headers, timeout=10) as response:
                if response.status_code == 200:
                    # 返回清理过的IP地址字符串
                    return response.text.strip()
        except requests.RequestException:
            # 如果一个API失败，就继续尝试下一个
            continue
    return None

def main():
    parser = argparse.ArgumentParser(
        description="一个用于测试Clash负载均衡(load-balance)策略组是否对Python脚本生效的工具。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-p", "--proxy",
        default="http://127.0.0.1:7890",
        help="Clash代理的地址和端口。\n例如: http://127.0.0.1:7890 或 socks5://127.0.0.1:1080"
    )
    parser.add_argument(
        "-n", "--requests",
        type=int,
        default=20,
        help="要发起的测试请求总数。"
    )
    parser.add_argument(
        "-w", "--workers",
        type=int,
        default=10,
        help="并发执行请求的线程数。"
    )
    
    args = parser.parse_args()

    print(f"{Color.BLUE}===== Clash 负载均衡检测工具 ====={Color.END}")
    print(f"代理服务器: {Color.YELLOW}{args.proxy}{Color.END}")
    print(f"测试请求数: {Color.YELLOW}{args.requests}{Color.END}")
    print(f"并发线程数: {Color.YELLOW}{args.workers}{Color.END}")
    print("-" * 35)

    proxies = {
        "http": args.proxy,
        "https": args.proxy,
    }

    # 使用 requests.Session 以利用连接池管理，但每次请求都会被强制关闭
    session = requests.Session()
    
    # 首次测试：检查代理是否可用
    print("正在测试代理连通性...")
    initial_ip = get_public_ip(session, proxies)
    if initial_ip is None:
        print(f"{Color.RED}❌ 错误: 无法通过代理连接到任何IP检测服务。{Color.END}")
        print("请检查:")
        print("1. Clash是否正在运行？")
        print("2. 代理地址和端口是否正确？")
        print("3. Clash的代理节点是否可用？")
        return

    print(f"{Color.GREEN}✅ 代理连接成功！探测到的初始IP: {initial_ip}{Color.END}\n")
    print(f"现在开始发起 {args.requests} 次并发请求来检测负载均衡...")

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # 创建任务列表
        futures = [executor.submit(get_public_ip, session, proxies) for _ in range(args.requests)]
        
        # 使用tqdm显示进度条
        try:
            from tqdm import tqdm
            progress_bar = tqdm(as_completed(futures), total=args.requests, desc="检测中")
        except ImportError:
            print("(提示: `pip install tqdm`可以获得一个漂亮的进度条)")
            progress_bar = as_completed(futures)
            
        for future in progress_bar:
            ip = future.result()
            if ip:
                results.append(ip)

    print("\n" + "=" * 15 + " 检测结果 " + "=" * 15)

    if not results:
        print(f"{Color.RED}❌ 测试失败: 所有 {args.requests} 次请求均未能获取IP地址。{Color.END}")
        return

    ip_counts = Counter(results)
    unique_ip_count = len(ip_counts)

    print(f"在 {len(results)} 次成功请求中，共检测到 {Color.YELLOW}{unique_ip_count}{Color.END} 个不同的出口IP地址：")
    
    for ip, count in ip_counts.items():
        print(f"  - IP: {Color.GREEN}{ip:<15}{Color.END}  |  出现次数: {Color.BLUE}{count}{Color.END}")

    print("\n" + "=" * 16 + " 结论 " + "=" * 17)

    if unique_ip_count > 1:
        print(f"{Color.GREEN}✅ 成功！检测到多个出口IP。{Color.END}")
        print("这表明Clash的 `load-balance` 策略组正在按预期工作，请求被分配到了不同的代理节点。")
    elif unique_ip_count == 1:
        print(f"{Color.YELLOW}⚠️ 注意！只检测到一个出口IP。{Color.END}")
        print("这可能由以下原因造成：")
        print("  1. (最可能) 您的Clash规则没有正确匹配，导致流量未进入 `load-balance` 组。")
        print("     -> {Color.YELLOW}解决方案: 在规则列表底部添加 `- MATCH, 你的策略组名`{Color.END}")
        print("  2. 您的 `load-balance` 策略组中只有一个节点是健康的，Clash只能使用它。")
        print("  3. 您的所有健康节点恰好位于同一个数据中心，拥有相同的出口IP。")
        print("  4. 脚本或 `requests` 库仍在以某种方式复用连接 (尽管`Connection: close`已尽力避免)。")
    else: # unique_ip_count == 0, 这种情况在前面已经处理
        print(f"{Color.RED}❌ 无法得出结论，因为没有成功的请求。{Color.END}")


if __name__ == "__main__":
    main()