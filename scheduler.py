import logging
import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

logger = logging.getLogger(__name__)
ET = pytz.timezone("America/New_York")


def run_morning_scan(app_context):
    logger.info("Morning scan starting (9:30 AM ET)")
    try:
        from data import fetch_multiple, compute_indicators, get_current_price, is_market_open, ALL_TICKERS, get_vix_data
        from strategy import detect_regime, strategy_earnings_drift
        from portfolio import Portfolio

        if not is_market_open():
            logger.info("Market closed, skipping morning scan")
            return

        portfolio = Portfolio()
        data_map = {}
        for sym in ALL_TICKERS:
            df = None
            from data import fetch_ticker_data
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

        drift_trades = strategy_earnings_drift(portfolio, data_map, regime, prices)
        for action, sym, strat, price, shares, stop, reason in drift_trades:
            pv = portfolio.total_value(prices)
            portfolio.open_position(sym, strat, price, shares, stop, pv, reason)

        portfolio.save()
        logger.info(f"Morning scan done. Regime={regime}, positions={len(portfolio.positions)}")
    except Exception as e:
        logger.error(f"Morning scan failed: {e}", exc_info=True)


def run_main_strategy(app_context):
    logger.info("Main strategy run starting (10:30 AM ET)")
    try:
        from data import fetch_multiple, compute_indicators, get_current_price, is_market_open, ALL_TICKERS, get_vix_data, fetch_ticker_data
        from strategy import detect_regime, strategy_trend, strategy_mean_reversion, strategy_vol_reversion, strategy_crypto_trend
        from portfolio import Portfolio

        if not is_market_open():
            logger.info("Market closed, skipping main strategy")
            return

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

        all_actions = []
        all_actions += strategy_trend(portfolio, data_map, regime, prices)
        all_actions += strategy_mean_reversion(portfolio, data_map, regime, prices)
        all_actions += strategy_vol_reversion(portfolio, data_map, vix_data, prices)
        all_actions += strategy_crypto_trend(portfolio, data_map, prices)

        pv = portfolio.total_value(prices)
        for action, sym, strat, price, shares, stop, reason in all_actions:
            if len(portfolio.positions) >= portfolio.max_positions():
                break
            if sym not in portfolio.positions:
                portfolio.open_position(sym, strat, price, shares, stop, pv, reason)

        portfolio.update_caution(pv)
        portfolio.save()
        logger.info(f"Main strategy done. Cash={portfolio.cash:.0f}, positions={len(portfolio.positions)}")
    except Exception as e:
        logger.error(f"Main strategy failed: {e}", exc_info=True)


def run_exit_checks(app_context):
    logger.info("Exit checks starting (3:45 PM ET)")
    try:
        from data import fetch_ticker_data, compute_indicators, get_current_price, is_market_open, ALL_TICKERS
        from strategy import detect_regime, check_exits
        from portfolio import Portfolio

        if not is_market_open():
            return

        portfolio = Portfolio()
        data_map = {}
        for sym in list(portfolio.positions.keys()):
            raw = fetch_ticker_data(sym)
            if raw is not None:
                df = compute_indicators(raw)
                if df is not None:
                    data_map[sym] = df

        spy_raw = fetch_ticker_data("SPY")
        spy_df = compute_indicators(spy_raw) if spy_raw is not None else None
        regime, _ = detect_regime(spy_df)

        prices = {}
        for sym in list(portfolio.positions.keys()):
            p = get_current_price(sym)
            if p:
                prices[sym] = p
            portfolio.update_trailing_stop(sym, prices.get(sym, portfolio.positions[sym]["entry_price"]))

        exits = check_exits(portfolio, data_map, regime, prices)
        pv = portfolio.total_value(prices)
        for sym, price, reason in exits:
            portfolio.close_position(sym, price, pv, reason)

        portfolio.increment_days_held()
        portfolio.save()
        logger.info(f"Exit checks done. Exited {len(exits)} positions")
    except Exception as e:
        logger.error(f"Exit checks failed: {e}", exc_info=True)


def run_eod_snapshot(app_context):
    logger.info("EOD snapshot starting (4:30 PM ET)")
    try:
        from data import fetch_ticker_data, compute_indicators, get_current_price, ALL_TICKERS
        from strategy import detect_regime
        from portfolio import Portfolio

        portfolio = Portfolio()
        spy_price = get_current_price("SPY") or 0

        prices = {}
        for sym in list(portfolio.positions.keys()):
            p = get_current_price(sym)
            if p:
                prices[sym] = p

        portfolio.save_equity_snapshot(spy_price, portfolio.regime, prices)
        portfolio.save()
        logger.info("EOD snapshot saved")
    except Exception as e:
        logger.error(f"EOD snapshot failed: {e}", exc_info=True)


def start_scheduler(app_context=None):
    scheduler = BackgroundScheduler(timezone=ET)

    scheduler.add_job(lambda: run_morning_scan(app_context), CronTrigger(hour=9, minute=30, timezone=ET))
    scheduler.add_job(lambda: run_main_strategy(app_context), CronTrigger(hour=10, minute=30, timezone=ET))
    scheduler.add_job(lambda: run_exit_checks(app_context), CronTrigger(hour=15, minute=45, timezone=ET))
    scheduler.add_job(lambda: run_eod_snapshot(app_context), CronTrigger(hour=16, minute=30, timezone=ET))

    scheduler.start()
    logger.info("Scheduler started with ET timezone jobs")
    return scheduler
