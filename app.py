import logging
import os
from flask import Flask, jsonify, Response
from flask_cors import CORS
from datetime import datetime, timedelta
import sqlite3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

DB_PATH = "trades.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_app():
    from portfolio import Portfolio, init_db
    init_db()
    p = Portfolio()
    p.save()
    try:
        from scheduler import start_scheduler
        start_scheduler()
    except Exception as e:
        logger.warning(f"Scheduler failed to start: {e}")


@app.route("/")
def index():
    try:
        with open(os.path.join(os.getcwd(), "index.html"), "rb") as f:
            return Response(f.read(), mimetype="text/html")
    except FileNotFoundError:
        return Response("Run app.py from the paper-trading folder", status=404)


@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


@app.route("/api/portfolio")
def api_portfolio():
    try:
        from portfolio import Portfolio
        from data import get_current_price
        portfolio = Portfolio()
        prices = {}
        for sym in portfolio.positions:
            p = get_current_price(sym)
            if p:
                prices[sym] = p
        pv = portfolio.total_value(prices)
        starting = 500_000.0
        total_return = (pv - starting) / starting
        dd = portfolio.get_drawdown(pv)

        # Daily P&L: compare today's snapshot to yesterday
        conn = get_db()
        rows = conn.execute(
            "SELECT portfolio_value FROM equity_history ORDER BY date DESC LIMIT 2"
        ).fetchall()
        conn.close()
        daily_pnl = 0.0
        if len(rows) >= 2:
            daily_pnl = rows[0]["portfolio_value"] - rows[1]["portfolio_value"]

        # SPY alpha
        spy_price = get_current_price("SPY") or 0
        spy_rows = conn = get_db()
        spy_hist = conn.execute(
            "SELECT spy_value FROM equity_history ORDER BY date ASC LIMIT 1"
        ).fetchone()
        conn.close()
        spy_start = spy_hist["spy_value"] if spy_hist else spy_price
        spy_return = (spy_price - spy_start) / spy_start if spy_start else 0
        alpha = total_return - spy_return

        return jsonify({
            "portfolio_value": pv,
            "cash": portfolio.cash,
            "total_return": total_return,
            "alpha": alpha,
            "drawdown": dd,
            "open_positions": len(portfolio.positions),
            "daily_pnl": daily_pnl,
            "caution_mode": portfolio.caution_mode,
            "regime": portfolio.regime,
            "strategy_weights": portfolio.strategy_weights,
        })
    except Exception as e:
        logger.error(f"/api/portfolio error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/positions")
def api_positions():
    try:
        from portfolio import Portfolio
        from data import get_current_price
        portfolio = Portfolio()
        result = []
        for sym, pos in portfolio.positions.items():
            price = get_current_price(sym) or pos["entry_price"]
            profit_pct = (price - pos["entry_price"]) / pos["entry_price"]
            result.append({
                "symbol": sym,
                "strategy": pos["strategy"],
                "entry_price": pos["entry_price"],
                "current_price": price,
                "shares": pos["shares"],
                "stop_price": pos["stop_price"],
                "days_held": pos.get("days_held", 0),
                "profit_pct": profit_pct,
                "position_value": price * pos["shares"],
            })
        return jsonify(result)
    except Exception as e:
        logger.error(f"/api/positions error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/trades")
def api_trades():
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY date DESC, id DESC LIMIT 20"
        ).fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        logger.error(f"/api/trades error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/regime")
def api_regime():
    try:
        from portfolio import Portfolio
        from data import fetch_ticker_data, compute_indicators
        from strategy import detect_regime
        portfolio = Portfolio()
        spy_raw = fetch_ticker_data("SPY", period="1y")
        spy_df = compute_indicators(spy_raw) if spy_raw is not None else None
        regime, stats = detect_regime(spy_df)
        return jsonify({
            "regime": regime,
            "stats": stats,
            "stored_regime": portfolio.regime,
        })
    except Exception as e:
        logger.error(f"/api/regime error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/equity-curve")
def api_equity_curve():
    try:
        conn = get_db()
        cutoff = (datetime.now() - timedelta(days=90)).date().isoformat()
        rows = conn.execute(
            "SELECT date, portfolio_value, spy_value, drawdown, regime FROM equity_history WHERE date >= ? ORDER BY date ASC",
            (cutoff,)
        ).fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        logger.error(f"/api/equity-curve error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/run-strategy", methods=["POST"])
def api_run_strategy():
    try:
        from data import fetch_ticker_data, compute_indicators, get_current_price, ALL_TICKERS, get_vix_data
        from strategy import detect_regime, strategy_trend, strategy_mean_reversion, strategy_vol_reversion, strategy_crypto_trend, strategy_earnings_drift, check_exits
        from portfolio import Portfolio

        portfolio = Portfolio()
        data_map = {}
        for sym in ALL_TICKERS:
            raw = fetch_ticker_data(sym)
            if raw is not None:
                df = compute_indicators(raw)
                if df is not None:
                    data_map[sym] = df

        spy_df = data_map.get("SPY")
        regime, spy_stats = detect_regime(spy_df)
        portfolio.regime = regime

        prices = {}
        for sym in ALL_TICKERS:
            p = get_current_price(sym)
            if p:
                prices[sym] = p

        vix_data = get_vix_data()

        exits = check_exits(portfolio, data_map, regime, prices)
        pv = portfolio.total_value(prices)
        for sym, price, reason in exits:
            portfolio.close_position(sym, price, pv, reason)

        portfolio.increment_days_held()
        for sym in portfolio.positions:
            portfolio.update_trailing_stop(sym, prices.get(sym, portfolio.positions[sym]["entry_price"]))

        all_actions = []
        all_actions += strategy_trend(portfolio, data_map, regime, prices)
        all_actions += strategy_mean_reversion(portfolio, data_map, regime, prices)
        all_actions += strategy_vol_reversion(portfolio, data_map, vix_data, prices)
        all_actions += strategy_earnings_drift(portfolio, data_map, regime, prices)
        all_actions += strategy_crypto_trend(portfolio, data_map, prices)

        pv = portfolio.total_value(prices)
        entered = []
        for action, sym, strat, price, shares, stop, reason in all_actions:
            if len(portfolio.positions) >= portfolio.max_positions():
                break
            if sym not in portfolio.positions:
                ok = portfolio.open_position(sym, strat, price, shares, stop, pv, reason)
                if ok:
                    entered.append({"symbol": sym, "strategy": strat, "price": price})

        portfolio.update_caution(pv)
        spy_price = prices.get("SPY", 0)
        portfolio.save_equity_snapshot(spy_price, regime, prices)
        portfolio.save()

        return jsonify({
            "status": "ok",
            "regime": regime,
            "exits": len(exits),
            "entered": entered,
            "portfolio_value": portfolio.total_value(prices),
            "open_positions": len(portfolio.positions),
        })
    except Exception as e:
        logger.error(f"/api/run-strategy error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    init_app()
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
else:
    init_app()
