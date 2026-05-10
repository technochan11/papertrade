import os
import json
from datetime import date

DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    def get_conn():
        return psycopg2.connect(DATABASE_URL)
    PLACEHOLDER = "%s"
else:
    import sqlite3
    DB_PATH = os.path.join(os.path.dirname(__file__), "papertrade.db")
    def get_conn():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    PLACEHOLDER = "?"


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, symbol TEXT, action TEXT, strategy TEXT,
            price REAL, shares REAL, position_value REAL,
            portfolio_value REAL, reason TEXT, profit_pct REAL
        )
    """ if not DATABASE_URL else """
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            date TEXT, symbol TEXT, action TEXT, strategy TEXT,
            price REAL, shares REAL, position_value REAL,
            portfolio_value REAL, reason TEXT, profit_pct REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS equity_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, portfolio_value REAL, spy_value REAL,
            drawdown REAL, regime TEXT, open_positions INTEGER
        )
    """ if not DATABASE_URL else """
        CREATE TABLE IF NOT EXISTS equity_history (
            id SERIAL PRIMARY KEY,
            date TEXT, portfolio_value REAL, spy_value REAL,
            drawdown REAL, regime TEXT, open_positions INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    cur.execute(f"SELECT value FROM portfolio_state WHERE key = {PLACEHOLDER}", ("state",))
    row = cur.fetchone()
    if row is None:
        initial = {
            "positions": [],
            "cash": 500000.0,
            "starting_capital": 500000.0,
            "spy_baseline": None,
        }
        cur.execute(
            f"INSERT INTO portfolio_state (key, value) VALUES ({PLACEHOLDER}, {PLACEHOLDER})",
            ("state", json.dumps(initial)),
        )
        conn.commit()
    cur.close()
    conn.close()


def get_state():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT value FROM portfolio_state WHERE key = {PLACEHOLDER}", ("state",))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return json.loads(row[0] if DATABASE_URL else row["value"])


def save_state(state):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"UPDATE portfolio_state SET value = {PLACEHOLDER} WHERE key = {PLACEHOLDER}",
        (json.dumps(state), "state"),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_portfolio_value(state, market_data):
    total = float(state.get("cash", 0))
    for pos in state.get("positions", []):
        df = market_data.get(pos["ticker"])
        try:
            series = df["Close"].dropna() if df is not None else None
            price = float(series.iloc[-1]) if series is not None and len(series) > 0 else pos["entry_price"]
        except Exception:
            price = pos["entry_price"]
        total += price * pos["shares"]
    return total


def calculate_position_size(portfolio_value, strategy_weight, stop_pct, vix, strategy, regime):
    if stop_pct <= 0:
        return 0.0
    vix_mult = 1.0
    if strategy not in ("vol_rev", "crash"):
        if vix >= 40:
            vix_mult = 0.25
        elif vix >= 30:
            vix_mult = 0.50
        elif vix >= 20:
            vix_mult = 0.75
    drawdown = get_drawdown(portfolio_value, get_state()["starting_capital"])
    caution_mult = 0.5 if drawdown > 0.12 else 1.0
    size = (portfolio_value * 0.035 * strategy_weight * 5) / stop_pct
    size *= vix_mult * caution_mult
    return min(size, portfolio_value * 0.25)


def _df_latest(df, col):
    try:
        series = df[col].dropna()
        return float(series.iloc[-1]) if len(series) > 0 else None
    except Exception:
        return None


def check_exits(positions, market_data, regime):
    exits = []
    for pos in positions:
        ticker = pos["ticker"]
        df = market_data.get(ticker)
        if df is None or len(df) == 0:
            continue
        price = _df_latest(df, "Close") or pos["entry_price"]
        rsi = _df_latest(df, "RSI14")
        ema20 = _df_latest(df, "EMA20")
        ema50 = _df_latest(df, "EMA50")

        profit_pct = (price - pos["entry_price"]) / pos["entry_price"]
        strategy = pos.get("strategy", "")
        days_held = pos.get("days_held", 0)
        reason = None

        if strategy == "short":
            if profit_pct <= -0.08:
                reason = "short_stop"
        elif strategy == "crash":
            if days_held >= 3:
                reason = "crash_time_exit"
            elif rsi is not None and rsi > 45:
                reason = "crash_rsi_exit"
        else:
            if profit_pct >= 1.0:
                stop_pct = 0.93
            elif profit_pct >= 0.50:
                stop_pct = 0.90
            elif profit_pct >= 0.25:
                stop_pct = 0.87
            else:
                stop_pct = 0.78
            if pos.get("stop_price") and price <= pos["stop_price"]:
                reason = "trailing_stop"
            if profit_pct >= 1.0:
                reason = reason or "partial_harvest"
            if strategy == "trend" and ema20 and ema50:
                if ema20 < ema50 * 0.95 or regime == "BEAR":
                    reason = "trend_exit"

        if reason:
            exits.append({**pos, "exit_reason": reason, "exit_price": price, "profit_pct": profit_pct})
    return exits


def execute_trade(signal, portfolio_value, cash, strategy_weights, vix, regime):
    ticker = signal.get("ticker")
    strategy = signal.get("strategy")
    stop_pct = signal.get("stop_pct", 0.05)
    price = signal.get("price")
    if not ticker or not strategy or not price or price <= 0:
        return None
    if regime == "BULL":
        max_positions = 5
    elif regime == "NEUTRAL":
        max_positions = 4
    else:
        max_positions = 3
    state = get_state()
    if len(state["positions"]) >= max_positions:
        return None
    weight = strategy_weights.get(strategy, 1.0)
    size = calculate_position_size(portfolio_value, weight, stop_pct, vix, strategy, regime)
    if size <= 0 or size > cash:
        return None
    shares = size / price
    return {
        "ticker": ticker,
        "strategy": strategy,
        "price": price,
        "shares": shares,
        "position_value": size,
        "stop_price": price * (1 - stop_pct),
        "entry_price": price,
        "entry_date": str(date.today()),
        "days_held": 0,
        "reason": signal.get("reason", ""),
    }


def log_trade(trade):
    conn = get_conn()
    cur = conn.cursor()
    p = PLACEHOLDER
    cur.execute(
        f"INSERT INTO trades (date, symbol, action, strategy, price, shares, position_value, portfolio_value, reason, profit_pct) "
        f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p})",
        (
            trade.get("date", str(date.today())),
            trade.get("ticker"),
            trade.get("action", "BUY"),
            trade.get("strategy"),
            trade.get("price"),
            trade.get("shares"),
            trade.get("position_value"),
            trade.get("portfolio_value"),
            trade.get("reason"),
            trade.get("profit_pct"),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


def log_equity(date_val, portfolio_value, spy_value, drawdown, regime, open_positions):
    conn = get_conn()
    cur = conn.cursor()
    p = PLACEHOLDER
    cur.execute(
        f"INSERT INTO equity_history (date, portfolio_value, spy_value, drawdown, regime, open_positions) "
        f"VALUES ({p},{p},{p},{p},{p},{p})",
        (str(date_val), portfolio_value, spy_value, drawdown, regime, open_positions),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_drawdown(portfolio_value, starting_capital):
    if starting_capital <= 0:
        return 0.0
    return max(0.0, (starting_capital - portfolio_value) / starting_capital)
