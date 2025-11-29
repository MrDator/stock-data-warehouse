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
    # 取最近 4 个季度 (yfinance 列通常按日期降序，iloc[:4] 获取最新的4个)
    recent_4 = row.iloc[:4]
    return float(recent_4.fillna(0).sum())

def get_exchange_rate(currency_code):
    """获取汇率: 1 USD = ? Local Currency"""
    if not currency_code or currency_code.upper() == 'USD':
        return 1.0
    try:
        # yfinance 格式通常是 "TWD=X"
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
    """基于行业关键词的自动分类器"""
    sector = info.get('sector', '')
    industry = info.get('industry', '')
    
    # 关键词匹配 (优先级从高到低)
    if 'Semiconductor' in industry or 'Semiconductor' in sector: return 'Semiconductor'
    if 'Software' in industry or 'Technology Services' in sector: return 'SaaS'
    if 'Consumer Electronics' in industry or 'Computer Hardware' in industry: return 'Hardware'
    if 'Biotechnology' in industry or 'Drug' in industry: return 'BioTech'
    # 金融/保险类 (BRK, JPM)
    if 'Bank' in industry or 'Financial' in sector or 'Insurance' in industry: return 'Financial'
    # 能源/公用事业
    if 'Energy' in sector or 'Oil' in industry or 'Utilities' in sector: return 'Energy/Utility'
    
    return 'General'

def calculate_sane_growth_rate(info, sector_type):
    """
    智能增长率计算器
    解决: 周期股负增长过大、巨头增长过高的问题
    """
    market_cap = info.get('marketCap', 0)
    
    # 行业配置: [Max Cap, Min Floor, Is Cyclical?]
    # Cyclical=True 意味着负增长会被强行修正为 GDP 增速，模拟均值回归
    SECTOR_CONFIG = {
        'Semiconductor': {'max': 60.0, 'min': -5.0, 'cyclical': True},
        'SaaS':          {'max': 45.0, 'min': 0.0,  'cyclical': False},
        'BioTech':       {'max': 40.0, 'min': -10.0, 'cyclical': True},
        'Financial':     {'max': 15.0, 'min': 0.0,  'cyclical': True},
        'Energy/Utility':{'max': 10.0, 'min': -5.0, 'cyclical': True},
        'Hardware':      {'max': 25.0, 'min': -5.0, 'cyclical': True},
        'General':       {'max': 20.0, 'min': -2.0, 'cyclical': False}
    }
    
    config = SECTOR_CONFIG.get(sector_type, SECTOR_CONFIG['General'])

    pe = info.get('trailingPE')
    peg = info.get('pegRatio')
    implied_growth = 0
    
    # 1. 优先使用 PEG 反推 (Growth = PE / PEG)
    if pe and peg and peg > 0:
        implied_growth = pe / peg
    
    # 2. 备选：使用营收增长
    if implied_growth == 0:
        raw_rev_growth = info.get('revenueGrowth')
        if raw_rev_growth is not None:
             implied_growth = raw_rev_growth * 100
        else:
             implied_growth = 3.0 # GDP 兜底

    # 3. 智能修正 (Sanitization)
    final_growth = implied_growth
    
    # A. 负增长修正 (周期回归)
    if implied_growth < config['min']:
        if config['cyclical']:
            # 周期性暴跌 -> 假设回归到通胀水平 (3%)
            final_growth = 3.0
        else:
            final_growth = config['min']
            
    # B. 高增长钳位 (防止线性外推泡沫)
    elif implied_growth > config['max']:
        final_growth = config['max']
        
    # C. 巨头重力惩罚 (市值 > 5000亿 USD)
    if market_cap > 500_000_000_000 and final_growth > 30:
        final_growth = 30.0

    return round(final_growth, 2)

def sanitize_beta(raw_beta, sector_type, market_cap):
    """
    Beta 平滑器
    解决: PDD Beta 过低 (WACC偏低) 和 NVDA Beta 过高 (WACC偏高) 的问题
    """
    if raw_beta is None: return 1.0
    
    # 1. 低 Beta 修复 (数据噪音)
    if raw_beta < 0.5:
        if sector_type in ['SaaS', 'Semiconductor', 'BioTech']: 
            return 1.2 # 科技股地板
        else: 
            return 0.8 # 传统股地板

    # 2. 巨头 Beta 收敛 (针对万亿市值俱乐部)
    # 逻辑: 即使历史波动大，万亿市值的系统性风险不应被视为极高
    if market_cap > 1_000_000_000_000: # 1 Trillion
        if raw_beta > 1.35:
            return 1.35
    elif market_cap > 200_000_000_000: # 200 Billion
        if raw_beta > 1.6:
            return 1.6

    # 3. 极端值封顶
    if raw_beta > 2.5: return 2.5
    
    return round(raw_beta, 2)

# --- 核心抓取逻辑 ---

def fetch_stock_data(ticker_symbol):
    # 1. Ticker 标准化 (BRK.B -> BRK-B)
    yf_ticker = ticker_symbol.replace('.', '-')
    
    try:
        print(f"Processing {yf_ticker}...")
        stock = yf.Ticker(yf_ticker)
        
        # 尝试获取 info，失败则重试一次
        try:
            info = stock.info
        except:
            time.sleep(1)
            info = stock.info
        
        # 价格检查 (必需字段)
        price = info.get('currentPrice') or info.get('previousClose')
        if not price: 
            print(f"  -> Error: No price found for {yf_ticker}")
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
        
        # 4. 损益表与现金流量表 (除以汇率)
        revenue_ttm = get_ttm_value(q_income, ['Total Revenue', 'Operating Revenue']) / fx_rate
        ocf_ttm = get_ttm_value(q_cashflow, ['Operating Cash Flow', 'Total Cash From Operating Activities']) / fx_rate
        capex_ttm = abs(get_ttm_value(q_cashflow, ['Capital Expenditure', 'Capital Expenditures'])) / fx_rate
        sbc_ttm = get_ttm_value(q_cashflow, ['Stock Based Compensation', 'Issuance Of Stock']) / fx_rate
        buyback_ttm = abs(get_ttm_value(q_cashflow, ['Repurchase Of Capital Stock', 'Common Stock Repurchased'])) / fx_rate

        # 5. 资产负债表 - 广义现金与债务 (除以汇率)
        raw_debt = 0
        raw_total_liquidity = 0 # 现金 + 短期投资
        shares = info.get('sharesOutstanding', 0)
        
        if not q_balance.empty:
            recent_bs = q_balance.iloc[:, 0]
            
            # [Debt]
            for k in ['Total Debt', 'Long Term Debt And Capital Lease Obligation', 'Long Term Debt']:
                if k in recent_bs:
                    raw_debt = float(recent_bs[k])
                    break
            
            # [Cash Fix] 泛化搜寻所有流动性资产 (针对 BRK, GOOGL 等)
            cash_part = 0
            invest_part = 0
            
            # A. 现金部分
            for k in ['Cash And Cash Equivalents', 'Cash Financial']:
                if k in recent_bs:
                    cash_part = float(recent_bs[k])
                    break
            
            # B. 短期投资部分 (这部分往往包含巨额国债/理财)
            for k in ['Other Short Term Investments', 'Short Term Investments', 'Available For Sale Securities']:
                if k in recent_bs:
                    invest_part = float(recent_bs[k])
                    # 只要找到一个非零值就停止，避免重复计算子项
                    if invest_part > 0:
                        break
            
            raw_total_liquidity = cash_part + invest_part
            # 调试日志
            # print(f"  -> Cash: {cash_part} + Invest: {invest_part} = {raw_total_liquidity}")
        
        total_debt = raw_debt / fx_rate
        cash = raw_total_liquidity / fx_rate

        # 6. 估值因子计算
        sector_type = determine_sector(info)
        market_cap = info.get('marketCap', 0)
        
        # Growth
        growth_rate = calculate_sane_growth_rate(info, sector_type)
        # Beta
        beta = sanitize_beta(info.get('beta'), sector_type, market_cap)
        # EPS (通常已是 USD，无需转换)
        forward_eps = info.get('forwardEps', 0)
        # Dividend
        raw_div_yield = info.get('dividendYield')
        dividend_yield = (raw_div_yield * 100) if raw_div_yield else 0.0

        # 7. 数据打包
        data = {
            "ticker": yf_ticker, # 使用标准化代码 (BRK-B)
            "name": info.get('shortName') or info.get('longName'),
            "price": price,
            "market_cap": market_cap,
            
            "revenue_ttm": revenue_ttm,
            "ocf_ttm": ocf_ttm,
            "capex_ttm": capex_ttm,
            "sbc_ttm": sbc_ttm,
            "buyback_ttm": buyback_ttm,
            
            "total_debt": total_debt,
            "cash_and_equivalents": cash, # 包含短期投资的广义现金
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
    
    # 默认创建示例
    if not os.path.exists(LISTS_DIR) or not glob.glob(os.path.join(LISTS_DIR, "*.txt")):
        print("No lists found. Creating sample list...")
        with open(os.path.join(LISTS_DIR, "sample.txt"), "w") as f:
            f.write("AAPL\nBRK.B\nTSM\nPDD\nNVDA\n")
    
    file_paths = glob.glob(os.path.join(LISTS_DIR, "*.txt"))
    for file_path in file_paths:
        list_name = os.path.basename(file_path).replace(".txt", "")
        with open(file_path, "r") as f:
            # 读取并清理每一行
            tickers = [line.strip().upper() for line in f if line.strip()]
        
        # 兼容性处理：把 BRK.B 这种展示用的格式保留在 Manifest 里
        # 但在 unique_tickers 集合里，我们也可以存原始格式，fetch_stock_data 会自己处理
        list_map[list_name] = tickers 
        unique_tickers.update(tickers)
        print(f"List loaded: {list_name} ({len(tickers)} stocks)")

    return unique_tickers, list_map

def main():
    print("--- Starting Generalized Data Pipeline ---")
    unique_tickers, list_map = load_tickers_from_lists()
    
    success_count = 0
    total = len(unique_tickers)

    for i, ticker in enumerate(unique_tickers):
        print(f"[{i+1}/{total}] ", end="")
        data = fetch_stock_data(ticker)
        if data:
            # 保存文件名为 BRK-B.json (标准化)
            save_name = data['ticker']
            with open(os.path.join(DATA_DIR, f"{save_name}.json"), "w") as f:
                json.dump(data, f, indent=2)
            success_count += 1
        # 礼貌性延时
        time.sleep(1.0)

    # 保存索引
    with open(os.path.join(DATA_DIR, MANIFEST_FILE), "w") as f:
        json.dump({"lists": list_map, "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, f)
        
    print(f"\n--- Done. Updated {success_count}/{total} stocks. ---")

if __name__ == "__main__":
    main()
