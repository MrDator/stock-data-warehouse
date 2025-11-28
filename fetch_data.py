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

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LISTS_DIR, exist_ok=True)

# --- 辅助函数 ---

def safe_get_row(df, keys):
    if df is None or df.empty:
        return pd.Series()
    for key in keys:
        if key in df.index:
            return df.loc[key]
    return pd.Series()

def get_ttm_value(quarterly_df, keys):
    row = safe_get_row(quarterly_df, keys)
    if row.empty:
        return 0
    recent_4 = row.iloc[:4]
    return float(recent_4.fillna(0).sum())

def get_exchange_rate(currency_code):
    if not currency_code or currency_code.upper() == 'USD':
        return 1.0
    try:
        pair = f"{currency_code.upper()}=X"
        fx = yf.Ticker(pair)
        rate = fx.info.get('currentPrice') or fx.info.get('regularMarketPrice') or fx.info.get('previousClose')
        if rate and rate > 0:
            return float(rate)
        return 1.0
    except Exception:
        return 1.0

def determine_sector(info):
    sector = info.get('sector', '')
    industry = info.get('industry', '')
    if 'Semiconductor' in industry or 'Semiconductor' in sector: return 'Semiconductor'
    if 'Software' in industry or 'Technology Services' in sector: return 'SaaS'
    if 'Consumer Electronics' in industry or 'Computer Hardware' in industry: return 'Hardware'
    if 'Bank' in industry or 'Financial' in sector: return 'Financial'
    if 'Biotechnology' in industry: return 'BioTech'
    return 'General'

def calculate_sane_growth_rate(info, sector_type):
    # (保持之前的智能增长率逻辑不变，此处省略以节省篇幅，实际运行时请保留之前的逻辑)
    # ...[请保留之前的 calculate_sane_growth_rate 代码]...
    # 为方便直接运行，这里提供简化版（包含之前的核心钳位逻辑）
    ticker = info.get('symbol', 'Unknown')
    market_cap = info.get('marketCap', 0)
    
    SECTOR_CONFIG = {
        'Semiconductor': {'max': 60.0, 'min': -5.0, 'cyclical': True},
        'SaaS':          {'max': 45.0, 'min': 0.0,  'cyclical': False},
        'General':       {'max': 20.0, 'min': -2.0, 'cyclical': False}
    }
    config = SECTOR_CONFIG.get(sector_type, SECTOR_CONFIG['General'])

    pe = info.get('trailingPE')
    peg = info.get('pegRatio')
    implied_growth = 0
    
    if pe and peg and peg > 0:
        implied_growth = pe / peg
    
    if implied_growth == 0:
        raw_rev_growth = info.get('revenueGrowth')
        if raw_rev_growth is not None:
             implied_growth = raw_rev_growth * 100
        else:
             implied_growth = 3.0

    final_growth = implied_growth
    if implied_growth < config['min']:
        final_growth = 3.0 if config['cyclical'] else config['min']
    elif implied_growth > config['max']:
        final_growth = config['max']
        
    if market_cap > 500_000_000_000 and final_growth > 30:
        final_growth = 30.0

    return round(final_growth, 2)

# --- Beta 消毒函数 (NEW) ---
def sanitize_beta(raw_beta, sector_type):
    """
    修复 Beta 值。
    PDD 的 Beta 0.05 是错误的。对于高风险/高增长行业，Beta 不应该小于 0.8。
    """
    if raw_beta is None:
        return 1.0 # 默认市场平均
    
    # 强制 Beta 修正
    # 如果 Beta < 0.5 (极低相关性，通常是数据错误)，强制拉回
    if raw_beta < 0.5:
        if sector_type in ['SaaS', 'Semiconductor', 'BioTech']:
            print(f"  [Logic] Beta {raw_beta} is too low for Tech. Resetting to 1.3")
            return 1.3 # 科技股默认 Beta
        else:
            print(f"  [Logic] Beta {raw_beta} is too low. Resetting to 1.0")
            return 1.0
            
    # 如果 Beta > 3.0 (极高波动，可能是噪音)，限制一下
    if raw_beta > 3.0:
        return 2.5
        
    return raw_beta

# --- 主逻辑 ---

def fetch_stock_data(ticker_symbol):
    try:
        print(f"Processing {ticker_symbol}...")
        stock = yf.Ticker(ticker_symbol)
        info = stock.info
        
        # 1. 价格
        price = info.get('currentPrice') or info.get('previousClose')
        if not price: return None

        # 2. 汇率 (只转换财报，不转换 Price/EPS)
        fin_currency = info.get('financialCurrency', 'USD')
        fx_rate = 1.0
        if fin_currency and fin_currency.upper() != 'USD':
            fx_rate = get_exchange_rate(fin_currency)
            print(f"  -> [FX] Financials in {fin_currency}. Rate: {fx_rate:.2f}")

        # 3. 报表
        q_cashflow = stock.quarterly_cashflow
        q_income = stock.quarterly_income_stmt
        q_balance = stock.quarterly_balance_sheet
        
        # 4. TTM 计算 + 汇率清洗
        # 只有这些来自财报的数据需要除以 fx_rate
        revenue_ttm = get_ttm_value(q_income, ['Total Revenue', 'Operating Revenue']) / fx_rate
        ocf_ttm = get_ttm_value(q_cashflow, ['Operating Cash Flow', 'Total Cash From Operating Activities']) / fx_rate
        capex_ttm = abs(get_ttm_value(q_cashflow, ['Capital Expenditure', 'Capital Expenditures'])) / fx_rate
        sbc_ttm = get_ttm_value(q_cashflow, ['Stock Based Compensation', 'Issuance Of Stock']) / fx_rate
        buyback_ttm = abs(get_ttm_value(q_cashflow, ['Repurchase Of Capital Stock', 'Common Stock Repurchased'])) / fx_rate

        # 5. 资产负债 (汇率清洗)
        raw_debt = 0
        raw_cash = 0
        shares = info.get('sharesOutstanding', 0)
        
        if not q_balance.empty:
            recent_bs = q_balance.iloc[:, 0]
            for k in ['Total Debt', 'Long Term Debt']:
                if k in recent_bs:
                    raw_debt = float(recent_bs[k])
                    break
            for k in ['Cash And Cash Equivalents', 'Cash Financial']:
                if k in recent_bs:
                    raw_cash = float(recent_bs[k])
                    break
        
        total_debt = raw_debt / fx_rate
        cash = raw_cash / fx_rate

        # 6. Estimates (Beta & EPS 修复)
        sector_type = determine_sector(info)
        growth_rate = calculate_sane_growth_rate(info, sector_type)
        
        # [Fix] Forward EPS 不再除以汇率，默认 Yahoo 给的就是 USD
        forward_eps = info.get('forwardEps', 0) 
        
        # [Fix] Beta 消毒
        raw_beta = info.get('beta')
        beta = sanitize_beta(raw_beta, sector_type)

        # 7. 组装
        data = {
            "ticker": ticker_symbol,
            "name": info.get('shortName'),
            "price": price,
            "market_cap": info.get('marketCap', 0),
            
            "revenue_ttm": revenue_ttm,
            "ocf_ttm": ocf_ttm,
            "capex_ttm": capex_ttm,
            "sbc_ttm": sbc_ttm,
            "buyback_ttm": buyback_ttm,
            
            "total_debt": total_debt,
            "cash_and_equivalents": cash,
            "shares_outstanding": shares,
            
            "beta": beta, # 使用修复后的 Beta
            "analyst_growth_estimate": growth_rate,
            "forward_eps": forward_eps,
            
            "sector_type": sector_type,
            "currency_code": "USD",
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
        print("Creating sample list...")
        with open(os.path.join(LISTS_DIR, "sample.txt"), "w") as f:
            f.write("AAPL\nPDD\nTSM\n")
    
    file_paths = glob.glob(os.path.join(LISTS_DIR, "*.txt"))
    for file_path in file_paths:
        list_name = os.path.basename(file_path).replace(".txt", "")
        with open(file_path, "r") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        list_map[list_name] = tickers
        unique_tickers.update(tickers)
        print(f"List loaded: {list_name}")

    return unique_tickers, list_map

def main():
    print("--- Starting Fixed Data Pipeline ---")
    unique_tickers, list_map = load_tickers_from_lists()
    
    for i, ticker in enumerate(unique_tickers):
        print(f"[{i+1}/{len(unique_tickers)}] ", end="")
        data = fetch_stock_data(ticker)
        if data:
            with open(os.path.join(DATA_DIR, f"{ticker}.json"), "w") as f:
                json.dump(data, f, indent=2)
        time.sleep(1.0)

    with open(os.path.join(DATA_DIR, MANIFEST_FILE), "w") as f:
        json.dump({"lists": list_map, "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, f)

if __name__ == "__main__":
    main()
