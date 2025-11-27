import yfinance as yf
import json
import os
import time
import glob
import pandas as pd

# 配置
DATA_DIR = "data"
LISTS_DIR = "lists"
MANIFEST_FILE = "_manifest.json"

# 确保数据目录存在
os.makedirs(DATA_DIR, exist_ok=True)

def safe_get(df, keys):
    """尝试从 DataFrame 行索引中获取数据，支持多个可能的 Key 别名"""
    for key in keys:
        if key in df.index:
            return df.loc[key]
    return pd.Series()

def get_ttm_value(quarterly_df, keys):
    """计算 TTM (Trailing Twelve Months): 取最近4个季度的和"""
    if quarterly_df.empty:
        return 0
    
    # 尝试找到对应的行
    row = safe_get(quarterly_df, keys)
    if row.empty:
        return 0
    
    # 取最近4列（最近4个季度），并求和
    # yfinance 的列通常是日期，越新越在前面
    recent_4 = row.iloc[:4]
    return float(recent_4.sum())

def fetch_stock_data(ticker_symbol):
    try:
        print(f"Fetching {ticker_symbol}...")
        stock = yf.Ticker(ticker_symbol)
        
        # 1. 基础信息 (Fast)
        info = stock.info
        price = info.get('currentPrice') or info.get('previousClose')
        
        if not price:
            print(f"  -> Skipped {ticker_symbol}: No price found.")
            return None

        # 2. 财务报表 (Slow & Deep)
        # 必须显式调用这些属性，yfinance 才会去下载
        q_income = stock.quarterly_income_stmt
        q_cashflow = stock.quarterly_cashflow
        q_balance = stock.quarterly_balance_sheet
        
        # --- 计算 TTM 数据 (关键步骤) ---
        # 营收 TTM
        revenue_ttm = get_ttm_value(q_income, ['Total Revenue', 'Operating Revenue'])
        
        # 经营现金流 TTM
        ocf_ttm = get_ttm_value(q_cashflow, ['Operating Cash Flow', 'Total Cash From Operating Activities'])
        
        # 资本开支 TTM (取绝对值，因为报表里通常是负数)
        capex_ttm = abs(get_ttm_value(q_cashflow, ['Capital Expenditure', 'Capital Expenditures']))
        
        # 股权激励 SBC TTM (有些公司可能没有)
        sbc_ttm = get_ttm_value(q_cashflow, ['Stock Based Compensation', 'Issuance Of Stock'])

        # --- 资产负债表 (取最近一期) ---
        total_debt = 0
        cash = 0
        shares = info.get('sharesOutstanding', 0)
        
        if not q_balance.empty:
            # 取最近一列
            recent_bs = q_balance.iloc[:, 0]
            
            # 债务
            if 'Total Debt' in recent_bs:
                total_debt = float(recent_bs['Total Debt'])
            elif 'Long Term Debt' in recent_bs: # 备选
                total_debt = float(recent_bs['Long Term Debt'])
            
            # 现金
            if 'Cash And Cash Equivalents' in recent_bs:
                cash = float(recent_bs['Cash And Cash Equivalents'])
            elif 'Total Assets' in recent_bs: # 极端的备选，通常不会走到这
                 cash = 0

        # --- 增长率与预测 (DCF/PEG 核心) ---
        # 1. 尝试从 Info 获取
        growth_rate = info.get('revenueGrowth', 0) * 100 # yf 返回的是 0.15
        
        # 2. 尝试获取分析师对未来5年的预测 (Earnings Growth)
        # yfinance 经常把这个藏在 'earningsGrowth' 或 'pegRatio' 里
        analyst_growth = info.get('earningsGrowth')
        if analyst_growth:
            analyst_growth = analyst_growth * 100
        else:
            # 如果没有直接的 growth 字段，尝试用 PEG 反推: Growth = PE / PEG
            pe = info.get('trailingPE')
            peg = info.get('pegRatio')
            if pe and peg and peg > 0:
                analyst_growth = pe / peg
            else:
                analyst_growth = 0 # 实在没有就给0，让前端处理

        forward_eps = info.get('forwardEps', 0)
        
        # 3. 行业分类
        sector = info.get('sector', 'General')
        if 'Technology' in sector or 'Software' in sector:
            sector_type = 'SaaS'
        elif 'Financial' in sector or 'Bank' in sector:
            sector_type = 'Financial'
        elif 'Semiconductor' in sector:
            sector_type = 'Semiconductor'
        else:
            sector_type = 'General'

        # --- 组装最终 JSON ---
        data = {
            "ticker": ticker_symbol,
            "name": info.get('shortName') or info.get('longName'),
            "price": price,
            "market_cap": info.get('marketCap', 0),
            
            # Financials TTM
            "revenue_ttm": revenue_ttm,
            "ocf_ttm": ocf_ttm,
            "capex_ttm": capex_ttm,
            "sbc_ttm": sbc_ttm,
            
            # Balance Sheet
            "total_debt": total_debt,
            "cash_and_equivalents": cash,
            "shares_outstanding": shares,
            
            # Estimates
            "beta": info.get('beta', 1.0),
            "analyst_growth_estimate": round(analyst_growth, 2), # e.g., 25.5
            "forward_eps": forward_eps,
            
            # Metadata
            "sector_type": sector_type,
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }

        # 简单的验证：如果营收是 0，说明抓取失败了，不要保存空数据
        if data['revenue_ttm'] == 0:
            print(f"  -> Warning: {ticker_symbol} has 0 Revenue. Detailed data fetch might have failed.")
            # 可以在这里选择 return None 跳过，或者依然保存
            # return None 
        
        return data

    except Exception as e:
        print(f"  -> Error fetching {ticker_symbol}: {e}")
        return None

def load_tickers_from_lists():
    unique_tickers = set()
    list_map = {}
    
    # 如果 lists 目录不存在，回退到读取根目录 tickers.txt
    if not os.path.exists(LISTS_DIR):
        if os.path.exists("tickers.txt"):
            print("Found tickers.txt (Root), loading...")
            with open("tickers.txt", "r") as f:
                tickers = [line.strip().upper() for line in f if line.strip()]
            return set(tickers), {"all": tickers}
        return set(), {}

    file_paths = glob.glob(os.path.join(LISTS_DIR, "*.txt"))
    for file_path in file_paths:
        list_name = os.path.basename(file_path).replace(".txt", "")
        with open(file_path, "r") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        list_map[list_name] = tickers
        unique_tickers.update(tickers)
        print(f"Loaded list '{list_name}': {len(tickers)} tickers")

    return unique_tickers, list_map

def main():
    print("--- Starting Robust Data Pipeline ---")
    
    unique_tickers, list_map = load_tickers_from_lists()
    print(f"Total unique tickers to fetch: {len(unique_tickers)}")
    
    if not unique_tickers:
        print("No tickers found! Please check your 'lists' folder or 'tickers.txt'.")
        return

    success_count = 0
    for ticker in unique_tickers:
        data = fetch_stock_data(ticker)
        
        if data:
            file_path = os.path.join(DATA_DIR, f"{ticker}.json")
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
            success_count += 1
            print(f"  -> Saved {ticker}.json")
        
        # 休息一下，防止被封 IP
        time.sleep(1.5)

    # 生成索引文件
    manifest_path = os.path.join(DATA_DIR, MANIFEST_FILE)
    with open(manifest_path, "w") as f:
        json.dump({
            "lists": list_map,
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }, f, indent=2)

    print(f"--- Job Complete. Updated {success_count} stocks. ---")

if __name__ == "__main__":
    main()
