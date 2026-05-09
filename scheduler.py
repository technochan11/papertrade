import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from data import get_market_data, get_vix, get_current_price
from portfolio import (
    get_state, save_state, get_portfolio_value, calculate_position_size,
    check_exits, execute_trade, log_trade, log_equity, get_drawdown, init_db
)
from strategy import run_all_strategies, detect_market_regime, detect_gap_ups

logger = logging.getLogger(__name__)

EASTERN = pytz.timezone('US/Eastern')

TICKERS = [
    'SPY','QQQ','IWM','XLK','XLF','XLE','XLV','XLI','XLC','XLY','XLP','XLU','XLRE','XLB',
    'AAPL','MSFT','GOOGL','AMZN','NVDA','META','JPM','JNJ','UNH','XOM','CVX','BAC','WMT','PG','HD','V',
    'IEF','GLD','TLT','SHY','BTC-USD','ETH-USD','SOL-USD','^VIX'
]

MARKET_DATA = {}
REGIME = 'NEUTRAL'
GAP_UP_STOCKS = []
STRATEGY_WEIGHTS = {
    'trend': 0.2, 'mean_rev': 0.2, 'vol_rev': 0.2, 'earnings_drift': 0.2,
    'crash': 0.2, 'short': 0.2, 'tlt_hedge': 0.2, 'gld': 0.2, 'crypto_trend': 0.2
}


def morning_data_fetch():
    global MARKET_DATA, GAP_UP_STOCKS
    logger.info('Morning data fetch starting')
    try:
        MARKET_DATA = get_market_data(TICKERS)
        GAP_UP_STOCKS = detect_gap_ups(MARKET_DATA)
        logger.info(f'Fetched {len(MARKET_DATA)} tickers, gap-ups: {GAP_UP_STOCKS}')
    except Exception as e:
        logger.error(f'Morning fetch error: {e}')


def update_regime():
    global REGIME
    try:
        spy_df = MARKET_DATA.get('SPY')
        if spy_df is not None:
            REGIME = detect_market_regime(spy_df)
            logger.info(f'Regime updated: {REGIME}')
    except Exception as e:
        logger.error(f'Regime update error: {e}')


def run_strategies():
    try:
        state = get_state()
        positions = state.get('positions', [])
        cash = float(state.get('cash', 500000))

        vix = get_vix()
        vix_df = MARKET_DATA.get('^VIX')
        vix_20d_avg = float(vix_df['Close'].iloc[-20:].mean()) if vix_df is not None and len(vix_df) >= 20 else 20.0

        spy_df = MARKET_DATA.get('SPY')
        portfolio_value = get_portfolio_value(state, MARKET_DATA)

        has_gld = any(p['ticker'] == 'GLD' for p in positions)
        signals = run_all_strategies(MARKET_DATA, REGIME, vix, vix_20d_avg, spy_df, GAP_UP_STOCKS, has_gld)

        drawdown = get_drawdown(portfolio_value, state.get('starting_capital', 500000))
        max_pos = {'BULL': 5, 'NEUTRAL': 4, 'BEAR': 3}.get(REGIME, 4)

        for signal in signals:
            if len(positions) >= max_pos:
                break
            if any(p['ticker'] == signal['ticker'] for p in positions):
                continue

            strategy = signal['strategy']
            weight = STRATEGY_WEIGHTS.get(strategy, 0.2)
            entry = signal['price']
            stop = signal['stop_price']
            stop_pct = abs(entry - stop) / entry if entry else 0.05

            size = calculate_position_size(portfolio_value, weight, stop_pct, vix, strategy, REGIME, drawdown)
            shares = int(size / entry) if entry > 0 else 0
            if shares < 1 or size > cash:
                continue

            trade = {
                'date': datetime.now(EASTERN).isoformat(),
                'symbol': signal['ticker'],
                'action': signal['action'],
                'strategy': strategy,
                'price': entry,
                'shares': shares,
                'position_value': shares * entry,
                'portfolio_value': portfolio_value,
                'reason': signal['reason'],
                'profit_pct': 0.0,
            }
            log_trade(trade)
            cash -= shares * entry
            positions.append({
                'ticker': signal['ticker'],
                'strategy': strategy,
                'entry_price': entry,
                'shares': shares,
                'stop_price': stop,
                'entry_date': datetime.now(EASTERN).date().isoformat(),
                'days_held': 0,
                'reason': signal['reason'],
            })

        state['positions'] = positions
        state['cash'] = cash
        save_state(state)
        logger.info(f'Strategies run. Positions: {len(positions)}, Cash: ${cash:,.0f}')
    except Exception as e:
        logger.error(f'run_strategies error: {e}', exc_info=True)


def check_exits_job():
    try:
        state = get_state()
        positions = state.get('positions', [])
        cash = float(state.get('cash', 500000))

        exits = check_exits(positions, MARKET_DATA, REGIME)
        remaining = []
        portfolio_value = get_portfolio_value(state, MARKET_DATA)

        for pos in positions:
            exit_info = next((e for e in exits if e['ticker'] == pos['ticker']), None)
            if exit_info:
                price = get_current_price(pos['ticker']) or pos['entry_price']
                profit_pct = (price - pos['entry_price']) / pos['entry_price']
                action = 'BUY_TO_COVER' if pos.get('strategy') == 'short' else 'SELL'
                trade = {
                    'date': datetime.now(EASTERN).isoformat(),
                    'symbol': pos['ticker'],
                    'action': action,
                    'strategy': pos['strategy'],
                    'price': price,
                    'shares': pos['shares'],
                    'position_value': pos['shares'] * price,
                    'portfolio_value': portfolio_value,
                    'reason': exit_info.get('reason', 'exit'),
                    'profit_pct': profit_pct,
                }
                log_trade(trade)
                cash += pos['shares'] * price
            else:
                pos['days_held'] = pos.get('days_held', 0) + 1
                remaining.append(pos)

        state['positions'] = remaining
        state['cash'] = cash
        save_state(state)
        logger.info(f'Exits checked. Closed {len(exits)}, remaining: {len(remaining)}')
    except Exception as e:
        logger.error(f'check_exits error: {e}', exc_info=True)


def daily_snapshot():
    try:
        state = get_state()
        portfolio_value = get_portfolio_value(state, MARKET_DATA)
        spy_price = get_current_price('SPY') or 0
        spy_baseline = state.get('spy_baseline')
        if not spy_baseline and spy_price:
            state['spy_baseline'] = spy_price
            save_state(state)
            spy_baseline = spy_price
        spy_scaled = (spy_price / spy_baseline * 500000) if spy_baseline else 500000
        drawdown = get_drawdown(portfolio_value, state.get('starting_capital', 500000))
        log_equity(
            datetime.now(EASTERN).date().isoformat(),
            portfolio_value, spy_scaled, drawdown, REGIME,
            len(state.get('positions', []))
        )
        logger.info(f'Snapshot: ${portfolio_value:,.0f}, drawdown={drawdown:.1%}')
    except Exception as e:
        logger.error(f'daily_snapshot error: {e}', exc_info=True)


def start_scheduler():
    init_db()
    scheduler = BackgroundScheduler(timezone=EASTERN)
    weekday = 'mon-fri'
    scheduler.add_job(morning_data_fetch, CronTrigger(day_of_week=weekday, hour=9, minute=30, timezone=EASTERN))
    scheduler.add_job(update_regime, CronTrigger(day_of_week=weekday, hour=10, minute=0, timezone=EASTERN))
    scheduler.add_job(run_strategies, CronTrigger(day_of_week=weekday, hour=10, minute=30, timezone=EASTERN))
    scheduler.add_job(check_exits_job, CronTrigger(day_of_week=weekday, hour=15, minute=45, timezone=EASTERN))
    scheduler.add_job(daily_snapshot, CronTrigger(day_of_week=weekday, hour=16, minute=30, timezone=EASTERN))
    scheduler.start()
    logger.info('Scheduler started')
    return scheduler


def stop_scheduler(scheduler):
    scheduler.shutdown()
