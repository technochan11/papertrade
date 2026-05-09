import yfinance as yf
import pandas as pd
import ta

STOCKS = [
    "SPY", "QQQ", "IWM", "XLK", "XLF", "XLE", "XLV", "XLI", "XLC", "XLY",
    "XLP", "XLU", "XLRE", "XLB", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "JPM", "JNJ", "UNH", "XOM", "CVX", "BAC", "WMT", "PG", "HD", "V",
    "IEF", "GLD", "TLT", "SHY",
]
CRYPTO = ["BTC-USD", "ETH-USD", "SOL-USD"]
ALL_TICKERS = STOCKS + CRYPTO


def get_ticker_data(ticker, period="6mo", interval="1d"):
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        # yfinance may return MultiIndex columns when auto_adjust=True
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
        return df if not df.empty else None
    except Exception:
        return None


def add_indicators(df):
    if df is None or df.empty or len(df) < 20:
        return df
    try:
        close = df["Close"]
        high = df["High"]
        low = df["Low"]

        df["EMA20"] = ta.trend.ema_indicator(close, window=20)
        df["EMA50"] = ta.trend.ema_indicator(close, window=50)
        df["EMA200"] = ta.trend.ema_indicator(close, window=200)
        df["RSI14"] = ta.momentum.rsi(close, window=14)

        bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
        df["BB_upper"] = bb.bollinger_hband()
        df["BB_middle"] = bb.bollinger_mavg()
        df["BB_lower"] = bb.bollinger_lband()

        adx = ta.trend.ADXIndicator(high, low, close, window=14)
        df["ADX"] = adx.adx()

        df["ATR"] = ta.volatility.average_true_range(high, low, close, window=14)
    except Exception:
        pass
    return df


def get_spy_data():
    df = get_ticker_data("SPY")
    return add_indicators(df) if df is not None else None


def get_vix():
    try:
        df = yf.download("^VIX", period="5d", interval="1d", progress=False, auto_adjust=True)
        if df is None or df.empty:
            return 20.0
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        series = df["Close"].dropna()
        if len(series) == 0:
            return 20.0
        return float(series.iloc[-1])
    except Exception:
        return 20.0


def get_market_data(tickers=None):
    if tickers is None:
        tickers = ALL_TICKERS
    result = {}
    for ticker in tickers:
        df = get_ticker_data(ticker)
        if df is not None:
            result[ticker] = add_indicators(df)
    return result


def get_current_price(ticker):
    try:
        df = yf.download(ticker, period="5d", interval="1d", progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        series = df["Close"].dropna()
        if len(series) == 0:
            return None
        return float(series.iloc[-1])
    except Exception:
        return None
