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
    # yfinance 的列通常是日期，按时间倒序排列 (最新的在左边)
    # 取前4列即为最近4个季度
    recent_4 = row.iloc[:4]
    return float(recent_4.fillna(0).sum())

def get_exchange_rate(currency_code):
    """获取 1 USD 兑换多少 Local Currency"""
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
    """行业分类"""
    sector = info.get('sector', '')
    industry = info.get('industry', '')
    
    if 'Semiconductor' in industry or 'Semiconductor' in sector: return 'Semiconductor'
    if 'Software' in industry or 'Technology Services' in sector: return 'SaaS'
    if 'Consumer Electronics' in industry or 'Computer Hardware' in industry: return 'Hardware'
    # 增加对 BRK 的识别 (Insurance)
    if 'Bank' in industry or 'Financial' in sector or 'Insurance' in industry: return 'Financial'
    if 'Biotechnology' in industry: return 'BioTech'
    return 'General'

def calculate_sane_growth_rate(info, sector_type):
    """计算理智的增长率 (智能钳位 + 行业分层)"""
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
    
    # 1. PEG 反推 (最优先)
    if pe and peg and peg > 0:
        implied_growth = pe / peg
    
    # 2. 备选：营收增长
    if implied_growth == 0:
        raw_rev_growth = info.get('revenueGrowth')
        if raw_rev_growth is not None:
             implied_growth = raw_rev_growth * 100
        else:
             implied_growth = 3.0

    # 3. 智能清洗
    final_growth = implied_growth
    
    if implied_growth < config['min']:
        final_growth = 3.0 if config['cyclical'] else config['min']
    elif implied_growth > config['max']:
        final_growth = config['max']
        
    # 巨头惩罚 (>5000亿)
    if market_cap > 500_000_000_000 and final_growth > 30:
        final_growth = 30.0

    return round(final_growth, 2)

def sanitize_beta(raw_beta, sector_type, market_cap):
    """
    Beta 平滑修正算法。
    解决 NVDA WACC 过高 (14%) 和 PDD WACC 过低 (5%) 的问题。
    """
    if raw_beta is None:
        return 1.0
    
    # 1. 低 Beta 修复 (数据错误或极低波动)
    # 任何科技/成长股不应该小于 0.8
    if raw_beta < 0.5:
        if sector_type in ['SaaS', 'Semiconductor', 'BioTech']:
            print(f"    [Beta Fix] Too low ({raw_beta}) for Tech. Smoothing to 1.2")
            return 1.2
        else:
            return 0.8 # 传统行业最低给 0.8

    # 2. 高 Beta 平滑 (针对巨头)
    # 逻辑：万亿市值的公司 (1T USD)，其真实的系统性风险不应该被视为"极高波动"。
    # 即使历史上 NVDA 波动很大，但在 DCF 中给 1.9 的 Beta 会导致 WACC 炸裂。
    # 我们采用 "Blume's Adjustment" 的变体，针对市值进行收敛。
    
    adjusted_beta = raw_beta
    
    # 门槛：2000亿市值
    if market_cap > 200_000_000_000:
        # 如果是万亿俱乐部 (> 1 Trillion)
        if market_cap > 1_000_000_000_000:
            # 强制收敛向 1.2 (软件/垄断巨头的典型值)
            if raw_beta > 1.35:
                print(f"    [Beta Fix] Dampening Trillion-Cap Beta from {raw_beta} to 1.35")
                adjusted_beta = 1.35
        # 如果是 2000亿 - 1万亿
        elif raw_beta > 1.6:
             print(f"    [Beta Fix] Dampening Large-Cap Beta from {raw_beta} to 1.6")
             adjusted_beta = 1.6

    # 3. 极端值封顶 (防止垃圾股数据干扰)
    if adjusted_beta > 2.5:
        adjusted_beta = 2.5

    return round(adjusted_beta, 2)

# --- 主逻辑 ---

def fetch_stock_data(ticker_symbol):
    # [关键修复] 自动转换 BRK.B -> BRK-B
    # 这样 yfinance 才能识别，保存的文件名也会变成 BRK-B.json
    yf_ticker = ticker_symbol.replace('.', '-')
    
    try:
        print(f"Processing {yf_ticker} (Input: {ticker_symbol})...")
        stock = yf.Ticker(yf_ticker)
        info = stock.info
        
        # 1. 价格
        price = info.get('currentPrice') or info.get('previousClose')
        if not price: 
            print(f"  -> Error: No price found for {yf_ticker}")
            return None

        # 2. 汇率清洗
        fin_currency = info.get('financialCurrency', 'USD')
        fx_rate = 1.0
        if fin_currency and fin_currency.upper() != 'USD':
            fx_rate = get_exchange_rate(fin_currency)
            print(f"  -> [FX] Converting {fin_currency} to USD (Rate: {fx_rate:.2f})")

        # 3. 获取报表
        q_cashflow = stock.quarterly_cashflow
        q_income = stock.quarterly_income_stmt
        q_balance = stock.quarterly_balance_sheet
        
        # 4. TTM 计算 + 汇率转换
        revenue_ttm = get_ttm_value(q_income, ['Total Revenue', 'Operating Revenue']) / fx_rate
        ocf_ttm = get_ttm_value(q_cashflow, ['Operating Cash Flow', 'Total Cash From Operating Activities']) / fx_rate
        capex_ttm = abs(get_ttm_value(q_cashflow, ['Capital Expenditure', 'Capital Expenditures'])) / fx_rate
        sbc_ttm = get_ttm_value(q_cashflow, ['Stock Based Compensation', 'Issuance Of Stock']) / fx_rate
        buyback_ttm = abs(get_ttm_value(q_cashflow, ['Repurchase Of Capital Stock', 'Common Stock Repurchased'])) / fx_rate

        # 5. 资产负债 + 汇率转换
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

        # 6. 估值核心指标 (Beta修正, Growth修正, EPS)
        sector_type = determine_sector(info)
        market_cap = info.get('marketCap', 0)
        
        # [Beta] 应用平滑修正
        beta = sanitize_beta(info.get('beta'), sector_type, market_cap)
        
        # [Growth] 应用智能修正
        growth_rate = calculate_sane_growth_rate(info, sector_type)
        
        # [EPS] 默认 USD
        forward_eps = info.get('forwardEps', 0)
        
        # [Yield] 股息率
        raw_div_yield = info.get('dividendYield')
        dividend_yield = (raw_div_yield * 100) if raw_div_yield else 0.0

        # 7. 组装数据
        data = {
            "ticker": yf_ticker, # 强制使用清洗后的代码 (BRK-B)
            "name": info.get('shortName') or info.get('longName'),
            "price": price,
            "market_cap": market_cap,
            
            "revenue_ttm": revenue_ttm,
            "ocf_ttm": ocf_ttm,
            "capex_ttm": capex_ttm,
            "sbc_ttm": sbc_ttm,
            "buyback_ttm": buyback_ttm,
            
            "total_debt": total_debt,
            "cash_and_equivalents": cash,
            "shares_outstanding": shares,
            
            "beta": beta,
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
        print("Creating sample list...")
        with open(os.path.join(LISTS_DIR, "sample.txt"), "w") as f:
            f.write("AAPL\nBRK.B\nTSM\n")
    
    file_paths = glob.glob(os.path.join(LISTS_DIR, "*.txt"))
    for file_path in file_paths:
        list_name = os.path.basename(file_path).replace(".txt", "")
        with open(file_path, "r") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        
        # 这里的 map 保持原样，方便前端分类
        list_map[list_name] = tickers 
        unique_tickers.update(tickers)
        print(f"List loaded: {list_name}")

    return unique_tickers, list_map

def main():
    print("--- Starting Robust Data Pipeline (Beta Smoothing + BRK Fix) ---")
    unique_tickers, list_map = load_tickers_from_lists()
    
    success_count = 0
    total = len(unique_tickers)

    for i, ticker in enumerate(unique_tickers):
        print(f"[{i+1}/{total}] ", end="")
        data = fetch_stock_data(ticker)
        if data:
            # 使用清洗后的 ticker (BRK-B) 作为文件名
            save_name = data['ticker']
            with open(os.path.join(DATA_DIR, f"{save_name}.json"), "w") as f:
                json.dump(data, f, indent=2)
            success_count += 1
        time.sleep(1.0)

    # 保存 Manifest
    with open(os.path.join(DATA_DIR, MANIFEST_FILE), "w") as f:
        json.dump({"lists": list_map, "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, f)
        
    print(f"\n--- Done. Updated {success_count}/{total} stocks. ---")

if __name__ == "__main__":
    main()
