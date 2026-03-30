import os
import pandas as pd
import yfinance as yf


def to_yfinance_symbol(symbol: str) -> str:
    """Convert Alpaca-style 'XRP/USD' to yfinance-style 'XRP-USD'."""
    return symbol.replace("/", "-")


def fetch_data(symbol: str, period: str = "6mo", interval: str = "1h",
               cache_dir: str = "data/", cache_max_hours: float = 1,
               start: str = None, end: str = None) -> pd.DataFrame:
    """Fetch OHLCV data from yfinance with local CSV caching.

    Use period for relative ranges ('6mo', '2y') or start/end for exact dates.
    """
    os.makedirs(cache_dir, exist_ok=True)
    safe_name = symbol.replace("/", "_")
    date_tag = f"{start}_{end}" if start else period
    cache_file = os.path.join(cache_dir, f"{safe_name}_{date_tag}_{interval}.csv")

    if os.path.exists(cache_file):
        age_hours = (pd.Timestamp.now() - pd.Timestamp(os.path.getmtime(cache_file), unit="s")).total_seconds() / 3600
        if age_hours < cache_max_hours:
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            print(f"  Loaded {symbol} from cache ({len(df)} bars)")
            return df

    yf_symbol = to_yfinance_symbol(symbol)
    label = f"{start} to {end}" if start else f"{period}, {interval}"
    print(f"  Fetching {symbol} from yfinance ({label})...")
    ticker = yf.Ticker(yf_symbol)

    if start:
        df = ticker.history(start=start, end=end, interval=interval)
    else:
        df = ticker.history(period=period, interval=interval)

    if df.empty:
        raise ValueError(f"No data returned for {symbol} (yfinance: {yf_symbol})")

    df = df[["Open", "High", "Low", "Close", "Volume"]]
    df.to_csv(cache_file)
    print(f"  Cached {symbol}: {len(df)} bars")
    return df


def fetch_pair(symbol_a: str, symbol_b: str, period: str = "6mo",
               interval: str = "1h", cache_dir: str = "data/",
               cache_max_hours: float = 1) -> pd.DataFrame:
    """Fetch and merge two symbols for pairs trading."""
    df_a = fetch_data(symbol_a, period, interval, cache_dir, cache_max_hours)
    df_b = fetch_data(symbol_b, period, interval, cache_dir, cache_max_hours)

    merged = pd.DataFrame({
        f"{symbol_a}_close": df_a["Close"],
        f"{symbol_b}_close": df_b["Close"],
    }).dropna()

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        merged[col] = df_a.loc[merged.index, col]

    return merged
