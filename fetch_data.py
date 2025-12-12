import yfinance as yf
import json
import os
import time
import glob
import pandas as pd
import math

# --- 全局配置 ---
DATA_DIR = "data"
LISTS_DIR = "lists"
MANIFEST_FILE = "_manifest.json"

# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LISTS_DIR, exist_ok=True)

# --- 核心工具函数 ---

def safe_get_row(df, keys):
    """从 DataFrame 中模糊查找行数据，支持别名列表"""
    if df is None or df.empty:
        return pd.Series()
    for key in keys:
        if key in df.index:
            return df.loc[key]
    return pd.Series()

def get_ttm_value(quarterly_df, keys):
    """计算 TTM (Trailing Twelve Months) 数值"""
    row = safe_get_row(quarterly_df, keys)
    if row.empty:
        return 0
    # 取最近 4 个季度
    recent_4 = row.iloc[:4]
    return float(recent_4.fillna(0).sum())

def get_exchange_rate(currency_code):
    """获取汇率: 1 USD = ? Local Currency"""
    if not currency_code or currency_code.upper() == 'USD':
        return 1.0
    try:
        pair = f"{currency_code.upper()}=X"
        fx = yf.Ticker(pair)
        rate = fx.info.get('currentPrice') or fx.info.get('regularMarketPrice') or fx.info.get('previousClose')
        if rate and rate > 0:
            return float(rate)
        return 1.0
    except Exception as e:
        print(f"    [FX Warning] Could not fetch rate for {currency_code}: {e}")
        return 1.0

def determine_sector(info):
    """行业分类器"""
    sector = info.get('sector', '')
    industry = info.get('industry', '')
    
    if 'Semiconductor' in industry or 'Semiconductor' in sector: return 'Semiconductor'
    if 'Software' in industry or 'Technology Services' in sector: return 'SaaS'
    if 'Consumer Electronics' in industry or 'Computer Hardware' in industry: return 'Hardware'
    if 'Biotechnology' in industry or 'Drug' in industry: return 'BioTech'
    if 'Bank' in industry or 'Financial' in sector or 'Insurance' in industry: return 'Financial'
    if 'Energy' in sector or 'Oil' in industry or 'Utilities' in sector: return 'Energy/Utility'
    if 'Real Estate' in sector or 'REIT' in industry: return 'REIT'
    
    return 'General'

def calculate_sane_growth_rate(info, sector_type):
    """智能增长率 (用于默认参考)"""
    market_cap = info.get('marketCap', 0)
    
    SECTOR_CONFIG = {
        'Semiconductor': {'max': 60.0, 'min': -5.0, 'cyclical': True},
        'SaaS':          {'max': 45.0, 'min': 0.0,  'cyclical': False},
        'BioTech':       {'max': 40.0, 'min': -10.0, 'cyclical': True},
        'Financial':     {'max': 15.0, 'min': 0.0,  'cyclical': True},
        'REIT':          {'max': 10.0, 'min': 0.0,  'cyclical': False},
        'Energy/Utility':{'max': 10.0, 'min': -5.0, 'cyclical': True},
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

def sanitize_beta(raw_beta, sector_type, market_cap):
    """Beta 平滑修正 (PDD底 / NVDA顶)"""
    if raw_beta is None: return 1.0
    
    if raw_beta < 0.5:
        if sector_type in ['SaaS', 'Semiconductor', 'BioTech']: return 1.2
        else: return 0.8

    if market_cap > 1_000_000_000_000:
        if raw_beta > 1.35: return 1.35
    elif market_cap > 200_000_000_000:
        if raw_beta > 1.6: return 1.6

    if raw_beta > 2.5: return 2.5
    return round(raw_beta, 2)

# --- 核心抓取逻辑 ---

def fetch_stock_data(ticker_symbol):
    # 1. Ticker 标准化 (BRK.B -> BRK-B)
    yf_ticker = ticker_symbol.replace('.', '-')
    
    try:
        print(f"Processing {yf_ticker}...")
        stock = yf.Ticker(yf_ticker)
        
        try:
            info = stock.info
        except:
            time.sleep(1)
            info = stock.info
        # 1. 价格 (USD)
        price = (
            info.get('currentPrice')
            or info.get('regularMarketPrice')
            or info.get('previousClose')
        )
        if not price or price <= 0:
            try:
                fi = getattr(stock, 'fast_info', None)
                if fi and isinstance(fi, dict):
                    price = fi.get('last_price') or price
            except Exception:
                pass
        if not price or price <= 0:
            try:
                hist = stock.history(period="5d", interval="1d")
                if hist is not None and not hist.empty and 'Close' in hist.columns:
                    close_series = hist['Close'].dropna()
                    if not close_series.empty:
                        price = float(close_series.iloc[-1])
            except Exception:
                pass
        if not price or price <= 0:
            print(f"  -> Error: No price found for {ticker_symbol}")
            return None

        # 2. 汇率处理
        fin_currency = info.get('financialCurrency', 'USD')
        fx_rate = 1.0
        if fin_currency and fin_currency.upper() != 'USD':
            fx_rate = get_exchange_rate(fin_currency)
            print(f"  -> [FX] Financials in {fin_currency}. Rate: {fx_rate:.2f}")

        # 3. 下载报表
        q_cashflow = stock.quarterly_cashflow
        q_income = stock.quarterly_income_stmt
        q_balance = stock.quarterly_balance_sheet
        
        # 4. 损益与现金流
        revenue_ttm = get_ttm_value(q_income, ['Total Revenue', 'Operating Revenue']) / fx_rate
        ocf_ttm = get_ttm_value(q_cashflow, ['Operating Cash Flow', 'Total Cash From Operating Activities']) / fx_rate
        capex_ttm = abs(get_ttm_value(q_cashflow, ['Capital Expenditure', 'Capital Expenditures'])) / fx_rate
        sbc_ttm = get_ttm_value(q_cashflow, ['Stock Based Compensation', 'Issuance Of Stock']) / fx_rate
        buyback_ttm = abs(get_ttm_value(q_cashflow, ['Repurchase Of Capital Stock', 'Common Stock Repurchased'])) / fx_rate
        
        # [NEW] Net Income TTM (用于计算再投资率)
        net_income_ttm = get_ttm_value(q_income, [
            'Net Income', 'Net Income Common Stockholders', 'Net Income From Continuing And Discontinued Operation'
        ]) / fx_rate

        # 5. 资产负债表 (Book Value & Liquidity)
        raw_debt = 0
        raw_total_liquidity = 0
        raw_book_value = 0
        shares = info.get('sharesOutstanding', 0)
        
        if not q_balance.empty:
            recent_bs = q_balance.iloc[:, 0]
            
            # Debt
            for k in ['Total Debt', 'Long Term Debt']:
                if k in recent_bs:
                    raw_debt = float(recent_bs[k])
                    break
            
            # Liquidity (Cash + Short Term Invest)
            cash_part = 0
            invest_part = 0
            for k in ['Cash And Cash Equivalents', 'Cash Financial']:
                if k in recent_bs: cash_part = float(recent_bs[k]); break
            for k in ['Other Short Term Investments', 'Short Term Investments', 'Available For Sale Securities']:
                if k in recent_bs:
                    candidate = float(recent_bs[k])
                    if candidate > 0:
                        invest_part = candidate
                        break
            raw_total_liquidity = cash_part + invest_part
            
            # Book Value
            for k in ['Total Stockholder Equity', 'Total Equity Gross Minority', 'Stockholders Equity']:
                if k in recent_bs:
                    raw_book_value = float(recent_bs[k])
                    break

        # Fallback for Book Value
        if raw_book_value == 0:
             raw_book_value = info.get('bookValue', 0) * shares

        total_debt = raw_debt / fx_rate
        cash = raw_total_liquidity / fx_rate
        book_value_ttm = raw_book_value / fx_rate

        # 6. 估值因子
        sector_type = determine_sector(info)
        growth_rate = calculate_sane_growth_rate(info, sector_type)
        beta = sanitize_beta(info.get('beta'), sector_type, info.get('marketCap', 0))
        forward_eps = info.get('forwardEps', 0)
        
        # ROE & Dividend
        raw_roe = info.get('returnOnEquity', 0)
        roe = raw_roe * 100 if raw_roe else 0.0
        raw_div_yield = info.get('dividendYield')
        dividend_yield = (raw_div_yield * 100) if raw_div_yield else 0.0

        # 7. 数据打包
        data = {
            "ticker": yf_ticker,
            "name": info.get('shortName') or info.get('longName'),
            "price": price,
            "market_cap": info.get('marketCap', 0),
            
            "revenue_ttm": revenue_ttm,
            "net_income_ttm": net_income_ttm, # [NEW]
            "ocf_ttm": ocf_ttm,
            "capex_ttm": capex_ttm,
            "sbc_ttm": sbc_ttm,
            "buyback_ttm": buyback_ttm,
            
            "total_debt": total_debt,
            "cash_and_equivalents": cash,
            "book_value_ttm": book_value_ttm, # [NEW]
            "shares_outstanding": shares,
            
            "beta": beta,
            "roe": round(roe, 2), # [NEW]
            "analyst_growth_estimate": growth_rate,
            "forward_eps": forward_eps,
            "dividend_yield": round(dividend_yield, 2),
            
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
        print("Creating sample...")
        with open(os.path.join(LISTS_DIR, "sample.txt"), "w") as f:
            f.write("AAPL\nBRK.B\n")
    
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
    print("--- Starting Hybrid Valuation Data Pipeline (v5) ---")
    unique_tickers, list_map = load_tickers_from_lists()
    
    success_count = 0
    total = len(unique_tickers)

    for i, ticker in enumerate(unique_tickers):
        print(f"[{i+1}/{total}] ", end="")
        data = fetch_stock_data(ticker)
        if data:
            save_name = data['ticker']
            with open(os.path.join(DATA_DIR, f"{save_name}.json"), "w") as f:
                json.dump(data, f, indent=2)
            success_count += 1
        time.sleep(1.0)

    with open(os.path.join(DATA_DIR, MANIFEST_FILE), "w") as f:
        json.dump({"lists": list_map, "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, f)
        
    print(f"\n--- Done. Updated {success_count}/{total} stocks. ---")

if __name__ == "__main__":
    main()
