import json
import logging
from datetime import datetime, date
import sqlite3

logger = logging.getLogger(__name__)

DB_PATH = "trades.db"
STARTING_CAPITAL = 500_000.0


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            symbol TEXT,
            action TEXT,
            strategy TEXT,
            price REAL,
            shares REAL,
            position_value REAL,
            portfolio_value REAL,
            reason TEXT,
            profit_pct REAL
        );
        CREATE TABLE IF NOT EXISTS equity_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            portfolio_value REAL,
            spy_value REAL,
            drawdown REAL,
            regime TEXT,
            open_positions INTEGER
        );
        CREATE TABLE IF NOT EXISTS portfolio_state (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    conn.commit()
    conn.close()


def load_state():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM portfolio_state")
    rows = cur.fetchall()
    conn.close()
    state = {}
    for row in rows:
        try:
            state[row["key"]] = json.loads(row["value"])
        except Exception:
            state[row["key"]] = row["value"]
    return state


def save_state(state):
    conn = get_db()
    cur = conn.cursor()
    for key, value in state.items():
        cur.execute(
            "INSERT OR REPLACE INTO portfolio_state (key, value) VALUES (?, ?)",
            (key, json.dumps(value))
        )
    conn.commit()
    conn.close()


def default_state():
    return {
        "cash": STARTING_CAPITAL,
        "positions": {},
        "peak_value": STARTING_CAPITAL,
        "caution_mode": False,
        "regime": "NEUTRAL",
        "strategy_weights": {
            "trend": 0.2,
            "mean_reversion": 0.2,
            "vol_reversion": 0.2,
            "earnings_drift": 0.2,
            "crypto_trend": 0.2,
        },
        "trade_history": [],
    }


class Portfolio:
    def __init__(self):
        init_db()
        state = load_state()
        if not state:
            state = default_state()
            save_state(state)
        self.cash = float(state.get("cash", STARTING_CAPITAL))
        self.positions = state.get("positions", {})
        self.peak_value = float(state.get("peak_value", STARTING_CAPITAL))
        self.caution_mode = bool(state.get("caution_mode", False))
        self.regime = state.get("regime", "NEUTRAL")
        self.strategy_weights = state.get("strategy_weights", default_state()["strategy_weights"])
        self.trade_history = state.get("trade_history", [])

    def total_value(self, prices=None):
        total = self.cash
        for sym, pos in self.positions.items():
            price = (prices or {}).get(sym, pos["entry_price"])
            total += pos["shares"] * price
        return total

    def save(self):
        save_state({
            "cash": self.cash,
            "positions": self.positions,
            "peak_value": self.peak_value,
            "caution_mode": self.caution_mode,
            "regime": self.regime,
            "strategy_weights": self.strategy_weights,
            "trade_history": self.trade_history,
        })

    def get_drawdown(self, current_value):
        if self.peak_value <= 0:
            return 0.0
        return (self.peak_value - current_value) / self.peak_value

    def update_caution(self, current_value):
        dd = self.get_drawdown(current_value)
        if dd > 0.12:
            self.caution_mode = True
        elif dd < 0.08:
            self.caution_mode = False

    def position_size(self, portfolio_value, stop_pct, weight=1.0):
        base_size = (portfolio_value * 0.035) / max(stop_pct, 0.01) * weight
        max_size = portfolio_value * 0.25
        size = min(base_size, max_size)
        if self.caution_mode:
            size *= 0.5
        return size

    def open_position(self, symbol, strategy, price, shares, stop_price, portfolio_value, reason=""):
        if symbol in self.positions:
            logger.warning(f"Already have position in {symbol}")
            return False
        cost = price * shares
        if cost > self.cash:
            shares = self.cash / price
            cost = price * shares
        if shares <= 0:
            return False
        self.cash -= cost
        self.positions[symbol] = {
            "strategy": strategy,
            "entry_price": price,
            "shares": shares,
            "stop_price": stop_price,
            "entry_date": date.today().isoformat(),
            "days_held": 0,
            "partial_harvested": False,
            "high_price": price,
        }
        self._record_trade(symbol, "BUY", strategy, price, shares, portfolio_value, reason, 0.0)
        logger.info(f"BUY {shares:.4f} {symbol} @ {price:.2f} stop={stop_price:.2f}")
        return True

    def close_position(self, symbol, price, portfolio_value, reason=""):
        pos = self.positions.get(symbol)
        if not pos:
            return False
        profit_pct = (price - pos["entry_price"]) / pos["entry_price"]
        proceeds = price * pos["shares"]
        self.cash += proceeds
        self._record_trade(symbol, "SELL", pos["strategy"], price, pos["shares"], portfolio_value, reason, profit_pct)
        self._update_strategy_stats(pos["strategy"], profit_pct)
        del self.positions[symbol]
        logger.info(f"SELL {symbol} @ {price:.2f} P&L={profit_pct*100:.1f}% reason={reason}")
        return True

    def partial_harvest(self, symbol, price, portfolio_value):
        pos = self.positions.get(symbol)
        if not pos or pos.get("partial_harvested"):
            return False
        sell_shares = pos["shares"] * 0.25
        proceeds = price * sell_shares
        self.cash += proceeds
        profit_pct = (price - pos["entry_price"]) / pos["entry_price"]
        self._record_trade(symbol, "SELL", pos["strategy"], price, sell_shares, portfolio_value, "partial_harvest", profit_pct)
        pos["shares"] -= sell_shares
        pos["partial_harvested"] = True
        logger.info(f"PARTIAL HARVEST {symbol} sold 25% @ {price:.2f}")
        return True

    def update_trailing_stop(self, symbol, current_price):
        pos = self.positions.get(symbol)
        if not pos:
            return
        profit_pct = (current_price - pos["entry_price"]) / pos["entry_price"]
        pos["high_price"] = max(pos.get("high_price", current_price), current_price)
        high = pos["high_price"]
        if profit_pct > 1.0:
            trail_stop = high * 0.93
        elif profit_pct > 0.5:
            trail_stop = high * 0.90
        elif profit_pct > 0.25:
            trail_stop = high * 0.87
        else:
            trail_stop = high * 0.78
        pos["stop_price"] = max(pos["stop_price"], trail_stop)

    def increment_days_held(self):
        for sym in self.positions:
            self.positions[sym]["days_held"] = self.positions[sym].get("days_held", 0) + 1

    def _record_trade(self, symbol, action, strategy, price, shares, portfolio_value, reason, profit_pct):
        conn = get_db()
        conn.execute(
            "INSERT INTO trades (date,symbol,action,strategy,price,shares,position_value,portfolio_value,reason,profit_pct) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (date.today().isoformat(), symbol, action, strategy, price, shares, price * shares, portfolio_value, reason, profit_pct)
        )
        conn.commit()
        conn.close()
        self.trade_history.append({
            "date": date.today().isoformat(),
            "symbol": symbol,
            "action": action,
            "strategy": strategy,
            "price": price,
            "profit_pct": profit_pct,
            "reason": reason,
        })
        self.trade_history = self.trade_history[-200:]

    def _update_strategy_stats(self, strategy, profit_pct):
        recent = [t for t in self.trade_history if t["strategy"] == strategy and t["action"] == "SELL"][-30:]
        if len(recent) < 10:
            return
        wins = sum(1 for t in recent if t["profit_pct"] > 0)
        win_rate = wins / len(recent)
        avg_return = sum(t["profit_pct"] for t in recent) / len(recent)
        if avg_return < 0 and win_rate < 0.35:
            weight = 0.05
        elif win_rate > 0.60 and avg_return > 0:
            weight = 0.35
        else:
            weight = max(0.05, win_rate * 0.6 + avg_return * 5 * 0.4)
        self.strategy_weights[strategy] = weight
        total = sum(self.strategy_weights.values())
        for k in self.strategy_weights:
            self.strategy_weights[k] = max(0.05, min(0.40, self.strategy_weights[k] / total))

    def save_equity_snapshot(self, spy_price, regime, prices=None):
        pv = self.total_value(prices)
        dd = self.get_drawdown(pv)
        if pv > self.peak_value:
            self.peak_value = pv
        conn = get_db()
        conn.execute(
            "INSERT OR REPLACE INTO equity_history (date,portfolio_value,spy_value,drawdown,regime,open_positions) VALUES (?,?,?,?,?,?)",
            (date.today().isoformat(), pv, spy_price, dd, regime, len(self.positions))
        )
        conn.commit()
        conn.close()

    def max_positions(self):
        if self.regime == "BULL":
            return 5
        elif self.regime == "BEAR":
            return 3
        return 4

    # --- Interface required by strategy.py ---

    @property
    def in_caution(self):
        return self.caution_mode

    def get_open_count(self):
        return len(self.positions)

    def has_position(self, ticker):
        return ticker in self.positions

    def get_portfolio_value(self):
        return getattr(self, "_current_value", self.cash)

    def get_position_size(self, strategy, stop_pct, weight=1.0):
        pv = self.get_portfolio_value()
        base = (pv * 0.035) / max(stop_pct, 0.01) * weight
        size = min(base, pv * 0.25)
        if self.caution_mode:
            size *= 0.5
        return size

    def positions_as_list(self):
        result = []
        for ticker, pos in self.positions.items():
            result.append({
                "ticker": ticker,
                "strategy": pos["strategy"],
                "entry_price": pos["entry_price"],
                "shares": pos["shares"],
                "stop_price": pos["stop_price"],
                "days_held": pos.get("days_held", 0),
                "gap_open": pos.get("gap_open", pos["entry_price"]),
                "harvested": pos.get("partial_harvested", False),
            })
        return result

    def sync_from_pos_list(self, pos_list):
        """Sync stop_price and harvested state back after check_exits mutates pos dicts."""
        for item in pos_list:
            ticker = item["ticker"]
            if ticker in self.positions:
                self.positions[ticker]["stop_price"] = item["stop_price"]
                self.positions[ticker]["partial_harvested"] = item.get("harvested", False)
                self.positions[ticker]["shares"] = item["shares"]
