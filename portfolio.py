import os
import json
from datetime import date, timedelta

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
    cur.execute("SELECT count(*) FROM trades WHERE date < '2026-04-01'")
    seed_count = cur.fetchone()[0]
    if row is None or seed_count == 0:
        initial = {
            "positions": [
                {"ticker": "JPM",  "strategy": "momentum", "entry_price": 274.80, "shares": 75,  "stop_price": 261.06, "entry_date": "2026-04-28", "days_held": 14, "reason": "momentum_signal"},
                {"ticker": "TSLA", "strategy": "mean_rev", "entry_price": 285.60, "shares": 120, "stop_price": 256.04, "entry_date": "2026-05-02", "days_held": 10, "reason": "mean_rev_signal"},
                {"ticker": "META", "strategy": "breakout", "entry_price": 591.40, "shares": 30,  "stop_price": 562.33, "entry_date": "2026-05-05", "days_held": 7,  "reason": "breakout_signal"},
                {"ticker": "NVDA", "strategy": "momentum", "entry_price": 124.60, "shares": 280, "stop_price": 118.37, "entry_date": "2026-05-07", "days_held": 5,  "reason": "momentum_signal"},
            ],
            "cash": 404587.00,
            "starting_capital": 500000.0,
            "spy_baseline": None,
        }
        if DATABASE_URL:
            cur.execute(
                f"INSERT INTO portfolio_state (key, value) VALUES ({PLACEHOLDER}, {PLACEHOLDER}) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                ("state", json.dumps(initial)),
            )
        else:
            cur.execute(
                "INSERT OR REPLACE INTO portfolio_state (key, value) VALUES (?, ?)",
                ("state", json.dumps(initial)),
            )

        # ── Seed equity_history: 45 days starting at $500k for both portfolio and SPY ──
        p = PLACEHOLDER
        base_date = date.today() - timedelta(days=45)
        port_val, spy_val = 500000.0, 500000.0
        regimes = ["BULL"] * 38 + ["NEUTRAL"] * 5 + ["BULL"] * 2
        equity_deltas = [
            320,-180,410,290,-120,380,250,-90,430,310,
            -160,490,220,-50,360,280,-130,510,190,-80,
            340,420,-70,260,380,-140,460,310,-90,570,
            230,-60,390,280,-110,440,260,-80,490,320,
            -150,380,240,-70,410,
        ]
        spy_deltas = [
            210,-130,290,180,-90,260,160,-60,290,200,
            -110,320,140,-40,240,170,-80,330,120,-55,
            220,280,-50,170,250,-90,300,200,-65,380,
            150,-40,260,180,-75,290,170,-55,320,210,
            -100,250,160,-50,270,
        ]
        for i in range(45):
            d = (base_date + timedelta(days=i)).isoformat()
            if i > 0:
                port_val += equity_deltas[i - 1]
                spy_val  += spy_deltas[i - 1]
            regime    = regimes[i] if i < len(regimes) else "BULL"
            drawdown  = max(-0.015, (port_val - 500000) / 500000) if port_val < 500000 else 0.0
            cur.execute(
                f"INSERT INTO equity_history (date, portfolio_value, spy_value, drawdown, regime, open_positions) "
                f"VALUES ({p},{p},{p},{p},{p},{p})",
                (d, round(port_val, 2), round(spy_val, 2), round(drawdown, 4), regime, 0),
            )

        # ── Seed trades: 22 closed trades + 4 open BUYs ──
        seed_trades = [
            # (date, symbol, action, strategy, price, shares, position_value, portfolio_value, reason, profit_pct)
            ("2026-03-29", "AAPL",  "BUY",  "momentum", 205.30, 85,  17450.50, 500000.00, "momentum_signal",  None),
            ("2026-04-12", "AAPL",  "SELL", "momentum", 218.40, 85,  18564.00, 501113.50, "trailing_stop",    0.0638),
            ("2026-03-31", "MSFT",  "BUY",  "momentum", 429.50, 45,  19327.50, 501113.50, "momentum_signal",  None),
            ("2026-04-14", "MSFT",  "SELL", "momentum", 451.20, 45,  20304.00, 502090.00, "trailing_stop",    0.0505),
            ("2026-04-04", "NVDA",  "BUY",  "breakout", 112.40, 350, 39340.00, 502090.00, "breakout_signal",  None),
            ("2026-04-22", "NVDA",  "SELL", "breakout", 127.80, 350, 44730.00, 507480.00, "trailing_stop",    0.1370),
            ("2026-04-06", "AMZN",  "BUY",  "mean_rev", 191.20, 100, 19120.00, 507480.00, "mean_rev_signal",  None),
            ("2026-04-17", "AMZN",  "SELL", "mean_rev", 196.80, 100, 19680.00, 508040.00, "partial_harvest",  0.0293),
            ("2026-04-09", "META",  "BUY",  "momentum", 585.30, 25,  14632.50, 508040.00, "momentum_signal",  None),
            ("2026-04-20", "META",  "SELL", "momentum", 572.10, 25,  14302.50, 507710.00, "trailing_stop",   -0.0225),
            ("2026-04-11", "AMD",   "BUY",  "breakout", 109.60, 200, 21920.00, 507710.00, "breakout_signal",  None),
            ("2026-04-27", "AMD",   "SELL", "breakout", 118.40, 200, 23680.00, 509470.00, "trailing_stop",    0.0803),
            ("2026-04-14", "GOOGL", "BUY",  "trend",    168.90, 120, 20268.00, 509470.00, "trend_signal",     None),
            ("2026-05-02", "GOOGL", "SELL", "trend",    175.20, 120, 21024.00, 510226.00, "trend_exit",       0.0373),
            ("2026-04-17", "AVGO",  "BUY",  "momentum", 232.10, 55,  12765.50, 510226.00, "momentum_signal",  None),
            ("2026-05-04", "AVGO",  "SELL", "momentum", 241.50, 55,  13282.50, 510743.00, "trailing_stop",    0.0405),
            ("2026-04-20", "COST",  "BUY",  "mean_rev", 895.40, 30,  26862.00, 510743.00, "mean_rev_signal",  None),
            ("2026-04-30", "COST",  "SELL", "mean_rev", 878.20, 30,  26346.00, 510227.00, "trailing_stop",   -0.0192),
            ("2026-04-24", "CRWD",  "BUY",  "breakout", 384.20, 80,  30736.00, 510227.00, "breakout_signal",  None),
            ("2026-05-06", "CRWD",  "SELL", "breakout", 401.30, 80,  32104.00, 511595.00, "trailing_stop",    0.0445),
            ("2026-04-22", "AAPL",  "BUY",  "momentum", 211.40, 60,  12684.00, 511595.00, "momentum_signal",  None),
            ("2026-05-08", "AAPL",  "SELL", "momentum", 219.80, 60,  13188.00, 512099.00, "trailing_stop",    0.0397),
            # open BUYs
            ("2026-04-28", "JPM",   "BUY",  "momentum", 274.80, 75,  20610.00, 512099.00, "momentum_signal",  None),
            ("2026-05-02", "TSLA",  "BUY",  "mean_rev", 285.60, 120, 34272.00, 512099.00, "mean_rev_signal",  None),
            ("2026-05-05", "META",  "BUY",  "breakout", 591.40, 30,  17742.00, 512099.00, "breakout_signal",  None),
            ("2026-05-07", "NVDA",  "BUY",  "momentum", 124.60, 280, 34888.00, 512099.00, "momentum_signal",  None),
        ]
        for t in seed_trades:
            cur.execute(
                f"INSERT INTO trades (date,symbol,action,strategy,price,shares,position_value,portfolio_value,reason,profit_pct) "
                f"VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p})",
                t,
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
    max_positions = 5
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
            trade.get("symbol") or trade.get("ticker"),
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
