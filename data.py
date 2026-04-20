import yfinance as yf
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

STOCKS = [
    "SPY","QQQ","IWM","XLK","XLF","XLE","XLV",
    "XLI","XLC","XLY","XLP","XLU","XLRE","XLB","AAPL",
    "MSFT","GOOGL","AMZN","NVDA","META","JPM","JNJ",
    "UNH","XOM","CVX","BAC","WMT","PG","HD","V",
    "IEF","GLD","TLT","SHY"
]
CRYPTO = ["BTC-USD","ETH-USD","SOL-USD"]
ALL_TICKERS = STOCKS + CRYPTO


def fetch_ticker_data(symbol, period="1y", retries=3):
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period)
            if df.empty:
                logger.warning(f"No data for {symbol}")
                return None
            df.index = pd.to_datetime(df.index).tz_localize(None)
            return df
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed for {symbol}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def fetch_multiple(symbols, period="1y"):
    results = {}
    for sym in symbols:
        data = fetch_ticker_data(sym, period=period)
        if data is not None:
            results[sym] = data
    return results


def get_current_price(symbol, retries=3):
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.fast_info
            price = info.last_price
            if price and price > 0:
                return float(price)
            hist = ticker.history(period="2d")
            if not hist.empty:
                return float(hist["Close"].iloc[-1])
        except Exception as e:
            logger.warning(f"Price fetch attempt {attempt+1} failed for {symbol}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def compute_indicators(df):
    if df is None or len(df) < 30:
        return None
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    df = df.copy()
    df["ema20"] = close.ewm(span=20, adjust=False).mean()
    df["ema50"] = close.ewm(span=50, adjust=False).mean()
    df["ema200"] = close.ewm(span=200, adjust=False).mean()

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))

    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    df["bb_mid"] = bb_mid
    df["bb_upper"] = bb_mid + 2 * bb_std
    df["bb_lower"] = bb_mid - 2 * bb_std

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    df["atr"] = tr.rolling(14).mean()

    ema_fast = close.ewm(span=12, adjust=False).mean()
    ema_slow = close.ewm(span=26, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    df["macd"] = macd_line
    df["macd_signal"] = macd_line.ewm(span=9, adjust=False).mean()

    # ADX
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
    plus_di = 100 * pd.Series(plus_dm, index=df.index).rolling(14).mean() / tr.rolling(14).mean()
    minus_di = 100 * pd.Series(minus_dm, index=df.index).rolling(14).mean() / tr.rolling(14).mean()
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    df["adx"] = dx.rolling(14).mean()

    df["vol_avg30"] = volume.rolling(30).mean()

    return df


def get_vix_data():
    try:
        vix = fetch_ticker_data("^VIX", period="3mo")
        if vix is not None:
            vix["vix_avg20"] = vix["Close"].rolling(20).mean()
            return vix
    except Exception as e:
        logger.warning(f"VIX fetch failed: {e}")
    return None


def is_market_open():
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close
