import yfinance as yf
import pandas as pd
import json
import os
import time
import glob

# 配置
DATA_DIR = "data"
LISTS_DIR = "lists"
MANIFEST_FILE = "_manifest.json"

# 确保数据目录存在
os.makedirs(DATA_DIR, exist_ok=True)

def load_tickers_from_lists():
    """
    读取 lists/ 目录下所有的 .txt 文件
    返回两个对象:
    1. unique_tickers: 一个去重后的 set，用于抓取数据
    2. list_map: 一个字典 {'nasdaq100': ['AAPL', ...], 'sp500': ['AAPL', ...]} 用于前端展示
    """
    unique_tickers = set()
    list_map = {}

    # 查找 lists 文件夹下所有 txt 文件
    file_paths = glob.glob(os.path.join(LISTS_DIR, "*.txt"))
    
    if not file_paths:
        print(f"Warning: No .txt files found in {LISTS_DIR}. Please create some.")
        return set(), {}

    for file_path in file_paths:
        # 获取文件名作为列表名 (例如 "nasdaq100.txt" -> "nasdaq100")
        list_name = os.path.basename(file_path).replace(".txt", "")
        
        with open(file_path, "r") as f:
            # 读取每一行，去除空行和空格
            tickers = [line.strip().upper() for line in f if line.strip()]
            
        list_map[list_name] = tickers
        unique_tickers.update(tickers)
        print(f"Loaded list '{list_name}': {len(tickers)} tickers")

    return unique_tickers, list_map

def fetch_stock_data(ticker):
    """(保持原有的抓取逻辑不变，为了节省篇幅这里简化，请保留你之前脚本里的完整逻辑)"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # --- 这里粘贴你之前脚本中计算 TTM 和 抓取 Estimates 的核心逻辑 ---
        # 简单示例:
        price = info.get('currentPrice') or info.get('previousClose')
        if not price: return None

        return {
            "ticker": ticker,
            "price": price,
            "name": info.get('shortName'),
            # ... 其他你需要的数据字段 ...
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        # -----------------------------------------------------------
        
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None

def main():
    print("Starting Data Pipeline...")
    
    # 1. 读取列表并构建映射
    unique_tickers, list_map = load_tickers_from_lists()
    print(f"Total unique tickers to fetch: {len(unique_tickers)}")
    
    # 2. 抓取数据并保存个股 JSON
    success_count = 0
    for ticker in unique_tickers:
        print(f"Processing {ticker}...")
        data = fetch_stock_data(ticker)
        
        if data:
            file_path = os.path.join(DATA_DIR, f"{ticker}.json")
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
            success_count += 1
        
        # 避免触发频率限制
        time.sleep(1)

    # 3. 生成 Manifest (目录索引) 文件
    # 前端只需要读取这个文件，就知道有哪些列表，以及每个列表里有哪些股
    manifest_path = os.path.join(DATA_DIR, MANIFEST_FILE)
    with open(manifest_path, "w") as f:
        json.dump({
            "lists": list_map,  # { "nasdaq100": ["AAPL", ...], "my_picks": [...] }
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }, f, indent=2)

    print(f"Job Complete. Updated {success_count} stocks. Manifest saved.")

if __name__ == "__main__":
    main()
