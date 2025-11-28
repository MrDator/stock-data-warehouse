import yfinance as yf
import json
import os
import time
import glob
import pandas as pd
import yfinance as yf
import json
import os
import time
import glob
import pandas as pd

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
    # 取最近4列（yfinance 列是日期）
    # 填充 NaN 为 0 防止计算中断
    recent_4 = row.iloc[:4]
    return float(recent_4.fillna(0).sum())

def get_exchange_rate(currency_code):
    """获取 1 USD 兑换多少 Local Currency (e.g. TWD=X -> 32.5)"""
    if not currency_code or currency_code.upper() == 'USD':
        return 1.0
    
    try:
        # yfinance 格式通常是 "货币代码=X"
        pair = f"{currency_code.upper()}=X"
        fx = yf.Ticker(pair)
        # 尝试获取价格
        rate = fx.info.get('currentPrice') or fx.info.get('regularMarketPrice') or fx.info.get('previousClose')
        
        if rate and rate > 0:
            return float(rate)
        return 1.0
    except Exception as e:
        print(f"    [Warning] Could not fetch FX rate for {currency_code}: {e}")
        return 1.0

def calculate_sane_growth_rate(info):
    """计算理智的增长率 (PEG 反推法 + 钳位)"""
    ticker = info.get('symbol', 'Unknown')
    market_cap = info.get('marketCap', 0)
    
    pe = info.get('trailingPE')
    peg = info.get('pegRatio')
    
    implied_growth = 0
    
    # 1. PEG 反推
    if pe and peg and peg > 0:
        implied_growth = pe / peg
    
    # 2. 备选方案
    if implied_growth == 0:
        implied_growth = (info.get('revenueGrowth') or 0.05) * 100

    # 3. 巨头安全钳位 (Clamping)
    final_growth = implied_growth
    if market_cap > 200_000_000_000: # 200B+ 市值
        if pe and pe < 40 and final_growth > 25:
             final_growth = 20.0
        elif final_growth > 50:
             final_growth = 45.0 # AI 股特殊处理

    return round(final_growth, 2)

def determine_sector(info):
    """行业分类"""
    sector = info.get('sector', '')
    industry = info.get('industry', '')
    
    if 'Semiconductor' in industry or 'Semiconductor' in sector:
        return 'Semiconductor'
    if 'Software' in industry or 'Technology Services' in sector:
        return 'SaaS'
    if 'Consumer Electronics' in industry or 'Computer Hardware' in industry:
        return 'Hardware'
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
        info = stock.info
        
        # 1. 价格 (USD)
        price = info.get('currentPrice') or info.get('previousClose')
        if not price:
            print(f"  -> Error: No price found for {ticker_symbol}")
            return None

        # 2. 货币检测与汇率转换
        # yfinance 的 info['financialCurrency'] 告诉我们财报用的什么货币
        fin_currency = info.get('financialCurrency', 'USD')
        fx_rate = 1.0
        
        if fin_currency != 'USD':
            fx_rate = get_exchange_rate(fin_currency)
            print(f"  -> [FX] Converting {fin_currency} to USD (Rate: {fx_rate:.2f})")

        # 3. 获取报表
        q_cashflow = stock.quarterly_cashflow
        q_income = stock.quarterly_income_stmt
        q_balance = stock.quarterly_balance_sheet
        
        # 4. 计算 TTM 数据 (Raw Local Currency)
        raw_rev = get_ttm_value(q_income, ['Total Revenue', 'Operating Revenue'])
        raw_ocf = get_ttm_value(q_cashflow, ['Operating Cash Flow', 'Total Cash From Operating Activities'])
        raw_capex = abs(get_ttm_value(q_cashflow, ['Capital Expenditure', 'Capital Expenditures']))
        raw_sbc = get_ttm_value(q_cashflow, ['Stock Based Compensation', 'Issuance Of Stock'])
        
        # 回购 (Buybacks)
        raw_buyback = abs(get_ttm_value(q_cashflow, [
            'Repurchase Of Capital Stock', 'Common Stock Repurchased', 'Purchase Of Treasury Stock'
        ]))

        # 5. 资产负债数据 (Raw Local Currency)
        raw_debt = 0
        raw_cash = 0
        shares = info.get('sharesOutstanding', 0)
        
        if not q_balance.empty:
            recent_bs = q_balance.iloc[:, 0]
            # 债务
            for k in ['Total Debt', 'Long Term Debt']:
                if k in recent_bs:
                    raw_debt = float(recent_bs[k])
                    break
            # 现金
            for k in ['Cash And Cash Equivalents', 'Cash Financial']:
                if k in recent_bs:
                    raw_cash = float(recent_bs[k])
                    break

        # 6. 执行汇率清洗 (Local -> USD)
        # 公式: USD = Local / Rate
        revenue_ttm = raw_rev / fx_rate
        ocf_ttm = raw_ocf / fx_rate
        capex_ttm = raw_capex / fx_rate
        sbc_ttm = raw_sbc / fx_rate
        buyback_ttm = raw_buyback / fx_rate
        total_debt = raw_debt / fx_rate
        cash = raw_cash / fx_rate

        # 7. Estimates (Growth & EPS)
        growth_rate = calculate_sane_growth_rate(info)
        
        # Forward EPS 经常也是当地货币，需要检查
        # 通常 ADR 的 Forward EPS 在 info 里已经调整过，但也可能没有
        # 我们这里做一个简单的启发式判断: PE = Price / EPS. 
        # 如果 Price(USD) / Raw_EPS 出来的 PE 极其离谱(比如0.5)，说明 EPS 是当地货币
        raw_fwd_eps = info.get('forwardEps', 0)
        forward_eps = raw_fwd_eps
        
        # 如果汇率差距很大 (比如日元/台币 1:30+)，需要转换 EPS
        if fx_rate > 5 and raw_fwd_eps > 0:
            # 简单转换，宁可错杀不能放过
            # 大多数情况下 yf 对 ADR 的 EPS 还是给的当地货币
            forward_eps = raw_fwd_eps / fx_rate

        # 8. 组装数据
        data = {
            "ticker": ticker_symbol,
            "name": info.get('shortName') or info.get('longName'),
            "price": price,
            "market_cap": info.get('marketCap', 0),
            
            # Converted USD Financials
            "revenue_ttm": revenue_ttm,
            "ocf_ttm": ocf_ttm,
            "capex_ttm": capex_ttm,
            "sbc_ttm": sbc_ttm,
            "buyback_ttm": buyback_ttm,
            
            # Converted USD Balance Sheet
            "total_debt": total_debt,
            "cash_and_equivalents": cash,
            "shares_outstanding": shares,
            
            # Estimates
            "beta": info.get('beta', 1.0),
            "analyst_growth_estimate": growth_rate,
            "forward_eps": forward_eps,
            
            "sector_type": determine_sector(info),
            "currency_code": "USD", # 显式标记
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        
        return data

    except Exception as e:
        print(f"  -> Exception fetching {ticker_symbol}: {e}")
        return None

def load_tickers_from_lists():
    unique_tickers = set()
    list_map = {}
    
    if not os.path.exists(LISTS_DIR) or not glob.glob(os.path.join(LISTS_DIR, "*.txt")):
        print("No lists found. Creating sample...")
        with open(os.path.join(LISTS_DIR, "sample.txt"), "w") as f:
            f.write("AAPL\nTSM\nPDD\n")
    
    file_paths = glob.glob(os.path.join(LISTS_DIR, "*.txt"))
    for file_path in file_paths:
        list_name = os.path.basename(file_path).replace(".txt", "")
        with open(file_path, "r") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        list_map[list_name] = tickers
        unique_tickers.update(tickers)
        print(f"List loaded: {list_name} ({len(tickers)} stocks)")

    return unique_tickers, list_map

def main():
    print("--- Starting FX-Aware Data Pipeline ---")
    
    unique_tickers, list_map = load_tickers_from_lists()
    print(f"Total unique tickers to fetch: {len(unique_tickers)}")

    success_count = 0
    for i, ticker in enumerate(unique_tickers):
        # 简单进度条
        print(f"[{i+1}/{len(unique_tickers)}] ", end="")
        data = fetch_stock_data(ticker)
        
        if data:
            with open(os.path.join(DATA_DIR, f"{ticker}.json"), "w") as f:
                json.dump(data, f, indent=2)
            success_count += 1
        
        time.sleep(1.5)

    manifest = {
        "lists": list_map,
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    with open(os.path.join(DATA_DIR, MANIFEST_FILE), "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\n--- Done. Updated {success_count} stocks. ---")

if __name__ == "__main__":
    main()
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
    # 取最近4列（yfinance 列是日期）
    # 填充 NaN 为 0 防止计算中断
    recent_4 = row.iloc[:4]
    return float(recent_4.fillna(0).sum())

def get_exchange_rate(currency_code):
    """获取 1 USD 兑换多少 Local Currency (e.g. TWD=X -> 32.5)"""
    if not currency_code or currency_code.upper() == 'USD':
        return 1.0
    
    try:
        # yfinance 格式通常是 "货币代码=X"
        pair = f"{currency_code.upper()}=X"
        fx = yf.Ticker(pair)
        # 尝试获取价格
        rate = fx.info.get('currentPrice') or fx.info.get('regularMarketPrice') or fx.info.get('previousClose')
        
        if rate and rate > 0:
            return float(rate)
        return 1.0
    except Exception as e:
        print(f"    [Warning] Could not fetch FX rate for {currency_code}: {e}")
        return 1.0

def calculate_sane_growth_rate(info):
    """计算理智的增长率 (PEG 反推法 + 钳位)"""
    ticker = info.get('symbol', 'Unknown')
    market_cap = info.get('marketCap', 0)
    
    pe = info.get('trailingPE')
    peg = info.get('pegRatio')
    
    implied_growth = 0
    
    # 1. PEG 反推
    if pe and peg and peg > 0:
        implied_growth = pe / peg
    
    # 2. 备选方案
    if implied_growth == 0:
        implied_growth = (info.get('revenueGrowth') or 0.05) * 100

    # 3. 巨头安全钳位 (Clamping)
    final_growth = implied_growth
    if market_cap > 200_000_000_000: # 200B+ 市值
        if pe and pe < 40 and final_growth > 25:
             final_growth = 20.0
        elif final_growth > 50:
             final_growth = 45.0 # AI 股特殊处理

    return round(final_growth, 2)

def determine_sector(info):
    """行业分类"""
    sector = info.get('sector', '')
    industry = info.get('industry', '')
    
    if 'Semiconductor' in industry or 'Semiconductor' in sector:
        return 'Semiconductor'
    if 'Software' in industry or 'Technology Services' in sector:
        return 'SaaS'
    if 'Consumer Electronics' in industry or 'Computer Hardware' in industry:
        return 'Hardware'
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
        info = stock.info
        
        # 1. 价格 (USD)
        price = info.get('currentPrice') or info.get('previousClose')
        if not price:
            print(f"  -> Error: No price found for {ticker_symbol}")
            return None

        # 2. 货币检测与汇率转换
        # yfinance 的 info['financialCurrency'] 告诉我们财报用的什么货币
        fin_currency = info.get('financialCurrency', 'USD')
        fx_rate = 1.0
        
        if fin_currency != 'USD':
            fx_rate = get_exchange_rate(fin_currency)
            print(f"  -> [FX] Converting {fin_currency} to USD (Rate: {fx_rate:.2f})")

        # 3. 获取报表
        q_cashflow = stock.quarterly_cashflow
        q_income = stock.quarterly_income_stmt
        q_balance = stock.quarterly_balance_sheet
        
        # 4. 计算 TTM 数据 (Raw Local Currency)
        raw_rev = get_ttm_value(q_income, ['Total Revenue', 'Operating Revenue'])
        raw_ocf = get_ttm_value(q_cashflow, ['Operating Cash Flow', 'Total Cash From Operating Activities'])
        raw_capex = abs(get_ttm_value(q_cashflow, ['Capital Expenditure', 'Capital Expenditures']))
        raw_sbc = get_ttm_value(q_cashflow, ['Stock Based Compensation', 'Issuance Of Stock'])
        
        # 回购 (Buybacks)
        raw_buyback = abs(get_ttm_value(q_cashflow, [
            'Repurchase Of Capital Stock', 'Common Stock Repurchased', 'Purchase Of Treasury Stock'
        ]))

        # 5. 资产负债数据 (Raw Local Currency)
        raw_debt = 0
        raw_cash = 0
        shares = info.get('sharesOutstanding', 0)
        
        if not q_balance.empty:
            recent_bs = q_balance.iloc[:, 0]
            # 债务
            for k in ['Total Debt', 'Long Term Debt']:
                if k in recent_bs:
                    raw_debt = float(recent_bs[k])
                    break
            # 现金
            for k in ['Cash And Cash Equivalents', 'Cash Financial']:
                if k in recent_bs:
                    raw_cash = float(recent_bs[k])
                    break

        # 6. 执行汇率清洗 (Local -> USD)
        # 公式: USD = Local / Rate
        revenue_ttm = raw_rev / fx_rate
        ocf_ttm = raw_ocf / fx_rate
        capex_ttm = raw_capex / fx_rate
        sbc_ttm = raw_sbc / fx_rate
        buyback_ttm = raw_buyback / fx_rate
        total_debt = raw_debt / fx_rate
        cash = raw_cash / fx_rate

        # 7. Estimates (Growth & EPS)
        growth_rate = calculate_sane_growth_rate(info)
        
        # Forward EPS 经常也是当地货币，需要检查
        # 通常 ADR 的 Forward EPS 在 info 里已经调整过，但也可能没有
        # 我们这里做一个简单的启发式判断: PE = Price / EPS. 
        # 如果 Price(USD) / Raw_EPS 出来的 PE 极其离谱(比如0.5)，说明 EPS 是当地货币
        raw_fwd_eps = info.get('forwardEps', 0)
        forward_eps = raw_fwd_eps
        
        # 如果汇率差距很大 (比如日元/台币 1:30+)，需要转换 EPS
        if fx_rate > 5 and raw_fwd_eps > 0:
            # 简单转换，宁可错杀不能放过
            # 大多数情况下 yf 对 ADR 的 EPS 还是给的当地货币
            forward_eps = raw_fwd_eps / fx_rate

        # 8. 组装数据
        data = {
            "ticker": ticker_symbol,
            "name": info.get('shortName') or info.get('longName'),
            "price": price,
            "market_cap": info.get('marketCap', 0),
            
            # Converted USD Financials
            "revenue_ttm": revenue_ttm,
            "ocf_ttm": ocf_ttm,
            "capex_ttm": capex_ttm,
            "sbc_ttm": sbc_ttm,
            "buyback_ttm": buyback_ttm,
            
            # Converted USD Balance Sheet
            "total_debt": total_debt,
            "cash_and_equivalents": cash,
            "shares_outstanding": shares,
            
            # Estimates
            "beta": info.get('beta', 1.0),
            "analyst_growth_estimate": growth_rate,
            "forward_eps": forward_eps,
            
            "sector_type": determine_sector(info),
            "currency_code": "USD", # 显式标记
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        
        return data

    except Exception as e:
        print(f"  -> Exception fetching {ticker_symbol}: {e}")
        return None

def load_tickers_from_lists():
    unique_tickers = set()
    list_map = {}
    
    if not os.path.exists(LISTS_DIR) or not glob.glob(os.path.join(LISTS_DIR, "*.txt")):
        print("No lists found. Creating sample...")
        with open(os.path.join(LISTS_DIR, "sample.txt"), "w") as f:
            f.write("AAPL\nTSM\nPDD\n")
    
    file_paths = glob.glob(os.path.join(LISTS_DIR, "*.txt"))
    for file_path in file_paths:
        list_name = os.path.basename(file_path).replace(".txt", "")
        with open(file_path, "r") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        list_map[list_name] = tickers
        unique_tickers.update(tickers)
        print(f"List loaded: {list_name} ({len(tickers)} stocks)")

    return unique_tickers, list_map

def main():
    print("--- Starting FX-Aware Data Pipeline ---")
    
    unique_tickers, list_map = load_tickers_from_lists()
    print(f"Total unique tickers to fetch: {len(unique_tickers)}")

    success_count = 0
    for i, ticker in enumerate(unique_tickers):
        # 简单进度条
        print(f"[{i+1}/{len(unique_tickers)}] ", end="")
        data = fetch_stock_data(ticker)
        
        if data:
            with open(os.path.join(DATA_DIR, f"{ticker}.json"), "w") as f:
                json.dump(data, f, indent=2)
            success_count += 1
        
        time.sleep(1.5)

    manifest = {
        "lists": list_map,
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    with open(os.path.join(DATA_DIR, MANIFEST_FILE), "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\n--- Done. Updated {success_count} stocks. ---")

if __name__ == "__main__":
    main()
