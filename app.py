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
        from data import fetch_ticker_data
        from strategy import get_regime, get_adx
        from ta.trend import EMAIndicator
        portfolio = Portfolio()
        spy_data = fetch_ticker_data("SPY", period="1y")
        regime = get_regime(spy_data) if spy_data is not None else "NEUTRAL"
        adx = get_adx(spy_data) if spy_data is not None else 0.0
        stats = {}
        if spy_data is not None and len(spy_data) >= 200:
            close = spy_data["Close"]
            price = float(close.iloc[-1])
            ema50 = float(EMAIndicator(close, window=50).ema_indicator().iloc[-1])
            ema200 = float(EMAIndicator(close, window=200).ema_indicator().iloc[-1])
            ret1m = float(close.iloc[-1] / close.iloc[-21] - 1) if len(close) >= 21 else 0
            ret3m = float(close.iloc[-1] / close.iloc[-63] - 1) if len(close) >= 63 else 0
            stats = {"price": price, "ema50": ema50, "ema200": ema200,
                     "return_1m": ret1m, "return_3m": ret3m, "adx": adx}
        return jsonify({"regime": regime, "stats": stats, "stored_regime": portfolio.regime})
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
        from data import fetch_ticker_data, get_current_price, ALL_TICKERS, get_vix_data, STOCKS, CRYPTO
        from strategy import (
            get_regime, get_adx, calculate_weights, detect_gap_ups,
            trend_following, mean_reversion, volatility_reversion,
            earnings_drift, crypto_trend, check_exits,
        )
        from portfolio import Portfolio

        portfolio = Portfolio()

        # Fetch raw yfinance DataFrames (strategy.py computes indicators via ta library)
        market_data = {}
        for sym in STOCKS:
            df = fetch_ticker_data(sym)
            if df is not None:
                market_data[sym] = df

        crypto_data = {}
        for sym in CRYPTO:
            df = fetch_ticker_data(sym)
            if df is not None:
                crypto_data[sym] = df

        spy_data = market_data.get("SPY")
        btc_data = crypto_data.get("BTC-USD")
        vix_data = get_vix_data()

        regime = get_regime(spy_data)
        adx = get_adx(spy_data)
        portfolio.regime = regime

        # Current prices for portfolio valuation
        prices = {}
        for sym in list(portfolio.positions.keys()):
            p = get_current_price(sym)
            if p:
                prices[sym] = p

        portfolio._current_value = portfolio.total_value(prices)
        weights = calculate_weights(portfolio.trade_history)
        max_pos = portfolio.max_positions()

        # Run exits — check_exits takes a list of position dicts and mutates them
        pos_list = portfolio.positions_as_list()
        all_market = {**market_data, **crypto_data}
        exit_signals = check_exits(pos_list, all_market, crypto_data, regime)

        pv = portfolio._current_value
        exits_done = 0
        for sig in exit_signals:
            ticker = sig["ticker"]
            price = sig["price"]
            if sig.get("partial"):
                pos = portfolio.positions.get(ticker)
                if pos:
                    h_shares = sig["shares"]
                    profit_pct = (price - pos["entry_price"]) / pos["entry_price"]
                    portfolio.cash += price * h_shares
                    pos["shares"] -= h_shares
                    pos["partial_harvested"] = True
                    portfolio._record_trade(ticker, "SELL", sig["strategy"], price, h_shares, pv, sig["reason"], profit_pct)
            else:
                portfolio.close_position(ticker, price, pv, sig.get("reason", "exit"))
                exits_done += 1

        # Sync trailing stops mutated by check_exits back into portfolio
        portfolio.sync_from_pos_list(pos_list)
        portfolio.increment_days_held()

        # Update value after exits, then run entry strategies
        portfolio._current_value = portfolio.total_value(prices)

        gap_up_tickers = detect_gap_ups(STOCKS, market_data)

        all_signals = []
        all_signals += trend_following(STOCKS, market_data, portfolio, regime, adx, weights, max_pos)
        all_signals += mean_reversion(STOCKS, market_data, portfolio, regime, adx, weights, max_pos)
        all_signals += volatility_reversion(spy_data, vix_data, portfolio, weights, max_pos)
        all_signals += earnings_drift(gap_up_tickers, market_data, portfolio, regime, weights, max_pos)
        all_signals += crypto_trend(CRYPTO, crypto_data, btc_data, portfolio, regime, weights, max_pos)

        pv = portfolio._current_value
        entered = []
        for sig in all_signals:
            if len(portfolio.positions) >= max_pos:
                break
            ticker = sig["ticker"]
            if ticker not in portfolio.positions:
                ok = portfolio.open_position(ticker, sig["strategy"], sig["price"],
                                             sig["shares"], sig["stop_price"], pv, sig["reason"])
                if ok:
                    entered.append({"symbol": ticker, "strategy": sig["strategy"], "price": sig["price"]})

        final_prices = {}
        for sym in list(portfolio.positions.keys()):
            p = get_current_price(sym)
            if p:
                final_prices[sym] = p

        portfolio.update_caution(portfolio.total_value(final_prices))
        spy_price = get_current_price("SPY") or 0
        portfolio.save_equity_snapshot(spy_price, regime, final_prices)
        portfolio.save()

        return jsonify({
            "status": "ok",
            "regime": regime,
            "adx": round(adx, 1),
            "exits": exits_done,
            "entered": entered,
            "portfolio_value": portfolio.total_value(final_prices),
            "open_positions": len(portfolio.positions),
        })
    except Exception as e:
        logger.error(f"/api/run-strategy error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    init_app()
    port = int(os.environ.get("PORT", 5005))
    app.run(host="0.0.0.0", port=port, debug=False)
else:
    init_app()
