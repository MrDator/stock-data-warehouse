import yfinance as yf
import json
import os
import time
import glob
import pandas as pd
import math

# --- 配置 ---
DATA_DIR = "data"
LISTS_DIR = "lists"
MANIFEST_FILE = "_manifest.json"

# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LISTS_DIR, exist_ok=True)

# --- 辅助函数 ---

def safe_get_row(df, keys):
    """尝试从 DataFrame 行索引中获取数据，支持多个可能的 Key 别名"""
    if df is None or df.empty:
        return pd.Series()
    for key in keys:
        if key in df.index:
            return df.loc[key]
    return pd.Series()

def get_ttm_value(quarterly_df, keys):
    """计算 TTM (过去12个月): 取最近4个季度的和"""
    row = safe_get_row(quarterly_df, keys)
    if row.empty:
        return 0
    # 取最近4列（yfinance 列是日期，通常最新的在左边或右边，这里假设是标准的 time series）
    # yfinance output usually has newest date as the first column if transposed, 
    # but strictly checking iloc is safer. 
    # Note: yf dataframe columns are dates.
    recent_4 = row.iloc[:4]
    
    # 简单的异常处理：如果有 NaN，填 0
    return float(recent_4.fillna(0).sum())

def calculate_sane_growth_rate(info):
    """
    计算一个'理智'的增长率。
    修复 Bug: AAPL 显示 91% 增长是因为 yf 返回了季度同比增长，而非长期 CAGR。
    """
    ticker = info.get('symbol', 'Unknown')
    market_cap = info.get('marketCap', 0)
    
    # 1. 尝试从 PEG 反推 (Growth = PE / PEG)
    # 这是获取分析师长期共识最准确的方法
    pe = info.get('trailingPE')
    peg = info.get('pegRatio')
    
    implied_growth = 0
    
    if pe and peg and peg > 0:
        implied_growth = pe / peg
    
    # 2. 如果反推失败，尝试读取 analyst estimates (往往 yf info 里没有这个字段，只能退而求其次)
    # 或者读取 revenueGrowth (通常比 earningsGrowth 稳定)
    if implied_growth == 0:
        # revenueGrowth 是小数 (e.g. 0.15)
        implied_growth = (info.get('revenueGrowth') or 0.05) * 100

    # 3. 安全钳位 (Clamping) - 防止脏数据
    # 逻辑：万亿市值的公司不可能长期保持 >30% 的增长。
    # 除非是极个别情况 (如 NVDA 在 AI 爆发期)，否则这就是噪音。
    
    final_growth = implied_growth
    
    if market_cap > 200_000_000_000: # 2000亿以上市值
        # 强制封顶在 25% (除非有 PEG 强力支撑，但在 PE 很高时 PEG 可能会失真)
        # 这里做一个软限制：如果 PE > 50 (如 NVDA)，允许更高增长；否则 AAPL 这种 PE~30 的，不应该 > 25%
        if pe and pe < 40 and final_growth > 25:
             print(f"  [Logic] Clamping growth for large cap {ticker} from {final_growth:.1f}% to 20%")
             final_growth = 20.0
        elif final_growth > 50:
             print(f"  [Logic] Clamping extreme growth for {ticker} from {final_growth:.1f}% to 45%")
             final_growth = 45.0

    return round(final_growth, 2)

def determine_sector(info):
    """更精细的行业分类"""
    sector = info.get('sector', '')
    industry = info.get('industry', '')
    
    if 'Semiconductor' in industry or 'Semiconductor' in sector:
        return 'Semiconductor'
    
    if 'Software' in industry or 'Technology Services' in sector:
        return 'SaaS'
        
    if 'Consumer Electronics' in industry or 'Computer Hardware' in industry:
        return 'Hardware' # For AAPL
        
    if 'Bank' in industry or 'Financial' in sector or 'Credit' in industry:
        return 'Financial'
    
    if 'Biotechnology' in industry or 'Drug' in industry:
        return 'BioTech'
        
    return 'General'

# --- 主逻辑 ---

def fetch_stock_data(ticker_symbol):
    try:
        print(f"Processing {ticker_symbol}...")
        stock = yf.Ticker(ticker_symbol)
        
        # 1. 获取基础 Info (网络请求 1)
        info = stock.info
        
        # 价格检查
        price = info.get('currentPrice') or info.get('previousClose') or info.get('regularMarketPrice')
        if not price:
            print(f"  -> Error: No price found for {ticker_symbol}")
            return None

        # 2. 获取报表 (网络请求 2-4，yfinance 会按需加载)
        # 注意：这里会触发下载
        q_cashflow = stock.quarterly_cashflow
        q_income = stock.quarterly_income_stmt
        q_balance = stock.quarterly_balance_sheet
        
        # 3. 计算 TTM 数据
        revenue_ttm = get_ttm_value(q_income, ['Total Revenue', 'Operating Revenue'])
        ocf_ttm = get_ttm_value(q_cashflow, ['Operating Cash Flow', 'Total Cash From Operating Activities'])
        
        # Capex (取绝对值)
        capex_ttm = abs(get_ttm_value(q_cashflow, ['Capital Expenditure', 'Capital Expenditures']))
        
        # SBC (股权激励)
        sbc_ttm = get_ttm_value(q_cashflow, ['Stock Based Compensation', 'Issuance Of Stock'])
        
        # Buybacks (回购 - 关键数据)
        # 通常是负数 (现金流出)，我们需要转为正数表示"回馈股东的金额"
        buyback_ttm = abs(get_ttm_value(q_cashflow, [
            'Repurchase Of Capital Stock', 
            'Common Stock Repurchased',
            'Purchase Of Treasury Stock'
        ]))

        # 4. 资产负债数据 (取最近一期)
        total_debt = 0
        cash = 0
        shares = info.get('sharesOutstanding', 0)
        
        if not q_balance.empty:
            recent_bs = q_balance.iloc[:, 0] # 最近一个季度
            
            # 债务查找
            keys_debt = ['Total Debt', 'Long Term Debt And Capital Lease Obligation', 'Long Term Debt']
            for k in keys_debt:
                if k in recent_bs:
                    total_debt = float(recent_bs[k])
                    break
            
            # 现金查找
            keys_cash = ['Cash And Cash Equivalents', 'Cash Financial']
            for k in keys_cash:
                if k in recent_bs:
                    cash = float(recent_bs[k])
                    break

        # 5. 增长率与 EPS
        growth_rate = calculate_sane_growth_rate(info)
        forward_eps = info.get('forwardEps', 0)
        
        # 6. 组装数据
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
            "buyback_ttm": buyback_ttm, # [NEW]
            
            # Balance Sheet
            "total_debt": total_debt,
            "cash_and_equivalents": cash,
            "shares_outstanding": shares,
            
            # Estimates
            "beta": info.get('beta', 1.0),
            "analyst_growth_estimate": growth_rate,
            "forward_eps": forward_eps,
            
            # Metadata
            "sector_type": determine_sector(info),
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        
        return data

    except Exception as e:
        print(f"  -> Exception fetching {ticker_symbol}: {e}")
        return None

def main():
    print("--- Starting Enhanced Data Pipeline ---")
    
    # 1. 扫描 lists 文件夹
    unique_tickers = set()
    list_map = {}
    
    # 默认如果没文件，创建个示例
    if not os.path.exists(LISTS_DIR) or not glob.glob(os.path.join(LISTS_DIR, "*.txt")):
        print("No lists found. Creating lists/sample.txt...")
        with open(os.path.join(LISTS_DIR, "sample.txt"), "w") as f:
            f.write("AAPL\nNVDA\nMSFT\n")
    
    file_paths = glob.glob(os.path.join(LISTS_DIR, "*.txt"))
    for file_path in file_paths:
        list_name = os.path.basename(file_path).replace(".txt", "")
        with open(file_path, "r") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        list_map[list_name] = tickers
        unique_tickers.update(tickers)
        print(f"List loaded: {list_name} ({len(tickers)} stocks)")

    # 2. 抓取循环
    success_count = 0
    total_count = len(unique_tickers)
    print(f"\nFetching data for {total_count} unique stocks...\n")

    for i, ticker in enumerate(unique_tickers):
        print(f"[{i+1}/{total_count}] ", end="")
        data = fetch_stock_data(ticker)
        
        if data:
            with open(os.path.join(DATA_DIR, f"{ticker}.json"), "w") as f:
                json.dump(data, f, indent=2)
            success_count += 1
        
        # Rate limit to be nice to Yahoo
        time.sleep(1)

    # 3. 生成索引
    manifest = {
        "lists": list_map,
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    with open(os.path.join(DATA_DIR, MANIFEST_FILE), "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\n--- Done. Updated {success_count}/{total_count} stocks. Manifest saved. ---")

if __name__ == "__main__":
    main()
