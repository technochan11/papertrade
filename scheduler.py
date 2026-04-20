import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

logger = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


def _build_market_data():
    from data import fetch_ticker_data, STOCKS, CRYPTO, get_vix_data, get_current_price
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
    vix_data = get_vix_data()
    prices = {}
    for sym in list(market_data) + list(crypto_data):
        p = get_current_price(sym)
        if p:
            prices[sym] = p
    return market_data, crypto_data, vix_data, prices


def run_morning_scan(_ctx=None):
    logger.info("Morning scan (9:30 AM ET)")
    try:
        from data import is_market_open, STOCKS
        from strategy import get_regime, get_adx, calculate_weights, detect_gap_ups, earnings_drift
        from portfolio import Portfolio

        if not is_market_open():
            logger.info("Market closed — skipping morning scan")
            return

        market_data, crypto_data, vix_data, prices = _build_market_data()
        portfolio = Portfolio()
        spy_data = market_data.get("SPY")
        regime = get_regime(spy_data)
        adx = get_adx(spy_data)
        portfolio.regime = regime
        portfolio._current_value = portfolio.total_value(prices)

        weights = calculate_weights(portfolio.trade_history)
        max_pos = portfolio.max_positions()
        gap_up_tickers = detect_gap_ups(STOCKS, market_data)
        signals = earnings_drift(gap_up_tickers, market_data, portfolio, regime, weights, max_pos)

        pv = portfolio._current_value
        for sig in signals:
            if len(portfolio.positions) >= max_pos:
                break
            if sig["ticker"] not in portfolio.positions:
                portfolio.open_position(sig["ticker"], sig["strategy"], sig["price"],
                                        sig["shares"], sig["stop_price"], pv, sig["reason"])
        portfolio.save()
        logger.info(f"Morning scan done. Regime={regime}, positions={len(portfolio.positions)}")
    except Exception as e:
        logger.error(f"Morning scan failed: {e}", exc_info=True)


def run_main_strategy(_ctx=None):
    logger.info("Main strategy run (10:30 AM ET)")
    try:
        from data import is_market_open, STOCKS, CRYPTO
        from strategy import (
            get_regime, get_adx, calculate_weights, detect_gap_ups,
            trend_following, mean_reversion, volatility_reversion,
            earnings_drift, crypto_trend,
        )
        from portfolio import Portfolio

        if not is_market_open():
            logger.info("Market closed — skipping main strategy")
            return

        market_data, crypto_data, vix_data, prices = _build_market_data()
        portfolio = Portfolio()
        spy_data = market_data.get("SPY")
        btc_data = crypto_data.get("BTC-USD")
        regime = get_regime(spy_data)
        adx = get_adx(spy_data)
        portfolio.regime = regime
        portfolio._current_value = portfolio.total_value(prices)

        weights = calculate_weights(portfolio.trade_history)
        max_pos = portfolio.max_positions()
        gap_up_tickers = detect_gap_ups(STOCKS, market_data)

        all_signals = []
        all_signals += trend_following(STOCKS, market_data, portfolio, regime, adx, weights, max_pos)
        all_signals += mean_reversion(STOCKS, market_data, portfolio, regime, adx, weights, max_pos)
        all_signals += volatility_reversion(spy_data, vix_data, portfolio, weights, max_pos)
        all_signals += earnings_drift(gap_up_tickers, market_data, portfolio, regime, weights, max_pos)
        all_signals += crypto_trend(CRYPTO, crypto_data, btc_data, portfolio, regime, weights, max_pos)

        pv = portfolio._current_value
        for sig in all_signals:
            if len(portfolio.positions) >= max_pos:
                break
            if sig["ticker"] not in portfolio.positions:
                portfolio.open_position(sig["ticker"], sig["strategy"], sig["price"],
                                        sig["shares"], sig["stop_price"], pv, sig["reason"])

        portfolio.update_caution(portfolio.total_value(prices))
        portfolio.save()
        logger.info(f"Main strategy done. Positions={len(portfolio.positions)}")
    except Exception as e:
        logger.error(f"Main strategy failed: {e}", exc_info=True)


def run_exit_checks(_ctx=None):
    logger.info("Exit checks (3:45 PM ET)")
    try:
        from data import is_market_open, get_current_price
        from strategy import get_regime, check_exits
        from portfolio import Portfolio

        if not is_market_open():
            return

        market_data, crypto_data, vix_data, prices = _build_market_data()
        portfolio = Portfolio()
        spy_data = market_data.get("SPY")
        regime = get_regime(spy_data)
        portfolio._current_value = portfolio.total_value(prices)

        pos_list = portfolio.positions_as_list()
        all_market = {**market_data, **crypto_data}
        exit_signals = check_exits(pos_list, all_market, crypto_data, regime)

        pv = portfolio._current_value
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

        portfolio.sync_from_pos_list(pos_list)
        portfolio.increment_days_held()
        portfolio.save()
        logger.info(f"Exit checks done. Exited {len([s for s in exit_signals if not s.get('partial')])} positions")
    except Exception as e:
        logger.error(f"Exit checks failed: {e}", exc_info=True)


def run_eod_snapshot(_ctx=None):
    logger.info("EOD snapshot (4:30 PM ET)")
    try:
        from data import get_current_price
        from strategy import get_regime
        from portfolio import Portfolio

        portfolio = Portfolio()
        prices = {}
        for sym in list(portfolio.positions.keys()):
            p = get_current_price(sym)
            if p:
                prices[sym] = p
        spy_price = get_current_price("SPY") or 0
        portfolio.save_equity_snapshot(spy_price, portfolio.regime, prices)
        portfolio.save()
        logger.info("EOD snapshot saved")
    except Exception as e:
        logger.error(f"EOD snapshot failed: {e}", exc_info=True)


def start_scheduler(_ctx=None):
    scheduler = BackgroundScheduler(timezone=ET)
    scheduler.add_job(run_morning_scan, CronTrigger(hour=9, minute=30, timezone=ET))
    scheduler.add_job(run_main_strategy, CronTrigger(hour=10, minute=30, timezone=ET))
    scheduler.add_job(run_exit_checks, CronTrigger(hour=15, minute=45, timezone=ET))
    scheduler.add_job(run_eod_snapshot, CronTrigger(hour=16, minute=30, timezone=ET))
    scheduler.start()
    logger.info("Scheduler started with ET timezone jobs")
    return scheduler
