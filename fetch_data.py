import yfinance as yf
import pandas as pd
import json
import os
import sys
from datetime import datetime

# Configuration
DATA_DIR = "data"
TICKER_FILE = "tickers.txt"

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def load_tickers(filename):
    with open(filename, 'r') as f:
        return [line.strip().upper() for line in f if line.strip()]

def get_ttm_value(financials, key):
    """
    Calculates Trailing Twelve Months (TTM) by summing the last 4 quarters.
    Assumes financials columns are dates descending (newest first).
    """
    try:
        if key not in financials.index:
            return None
        
        # Get the 4 most recent quarters
        recent_quarters = financials.loc[key].iloc[:4]
        
        if len(recent_quarters) < 4:
            print(f"Warning: Less than 4 quarters found for {key}")
            return None
            
        return float(recent_quarters.sum())
    except Exception as e:
        print(f"Error calculating TTM for {key}: {e}")
        return None

def fetch_stock_data(symbol):
    print(f"Processing {symbol}...")
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        # Fetch Quarterly Data
        q_financials = ticker.quarterly_financials
        q_cashflow = ticker.quarterly_cashflow
        
        # Calculate TTM Metrics
        # Note: yfinance keys often vary, using standard keys
        revenue_ttm = get_ttm_value(q_financials, "Total Revenue")
        ocf_ttm = get_ttm_value(q_cashflow, "Operating Cash Flow")
        capex_ttm = get_ttm_value(q_cashflow, "Capital Expenditure")
        
        # If Capex is negative in data (outflow), we keep it as is or flip based on preference.
        # Usually represented as negative in cashflow statements.
        
        # Extract estimates and snapshots
        market_cap = info.get('marketCap')
        total_debt = info.get('totalDebt')
        cash = info.get('totalCash')
        shares = info.get('sharesOutstanding')
        price = info.get('currentPrice') or info.get('regularMarketPrice')
        
        # Structure Data
        data = {
            "ticker": symbol,
            "price": price,
            "market_cap": market_cap,
            "revenue_ttm": revenue_ttm,
            "ocf_ttm": ocf_ttm,
            "capex_ttm": capex_ttm,
            "total_debt": total_debt,
            "cash_and_equivalents": cash,
            "shares_outstanding": shares,
            "beta": info.get('beta'),
            "analyst_growth_estimate": info.get('earningsGrowth'), # This is often the YoY quarterly growth, 'pegRatio' or custom analysis might be needed for 5y
            "forward_eps": info.get('forwardEps'),
            "sector_type": info.get('sector'),
            "last_updated": datetime.utcnow().isoformat() + "Z"
        }
        
        return data

    except Exception as e:
        print(f"Failed to fetch data for {symbol}: {e}")
        return None

def main():
    ensure_dir(DATA_DIR)
    
    if not os.path.exists(TICKER_FILE):
        print(f"Error: {TICKER_FILE} not found.")
        sys.exit(1)
        
    tickers = load_tickers(TICKER_FILE)
    
    for symbol in tickers:
        data = fetch_stock_data(symbol)
        if data:
            file_path = os.path.join(DATA_DIR, f"{symbol}.json")
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"Saved {file_path}")
        else:
            print(f"Skipping {symbol} due to errors.")

if __name__ == "__main__":
    main()
