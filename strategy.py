import pandas as pd
import numpy as np

ETF_TICKERS = {'SPY','QQQ','IWM','XLK','XLF','XLE','XLV','XLI','XLC','XLY','XLP','XLU','XLRE','XLB','IEF','GLD','TLT','SHY'}
DEFENSIVE = {'XLP','XLU','XLV','GLD','TLT'}
AGGRESSIVE = {'XLK','XLY','XLC'}
CRYPTO = {'BTC-USD','ETH-USD','SOL-USD'}


def _latest(df, col):
    try:
        return float(df[col].dropna().iloc[-1])
    except Exception:
        return None


def _signal(ticker, action, strategy, price, stop_pct, reason, shares=0):
    return {
        'ticker': ticker, 'action': action, 'strategy': strategy,
        'price': price, 'shares': shares,
        'stop_price': round(price * (1 - stop_pct) if action == 'BUY' else price * (1 + stop_pct), 4),
        'reason': reason,
    }


def detect_market_regime(spy_df):
    try:
        price = _latest(spy_df, 'Close')
        ema50 = _latest(spy_df, 'EMA50')
        ema200 = _latest(spy_df, 'EMA200')
        if price and ema50 and ema200:
            if price > ema50 and ema50 > ema200 * 0.98:
                return 'BULL'
            if price < ema200 * 0.97:
                return 'BEAR'
    except Exception:
        pass
    return 'NEUTRAL'


def detect_gap_ups(market_data):
    gaps = []
    for ticker, df in market_data.items():
        if ticker in CRYPTO or ticker == '^VIX' or len(df) < 20:
            continue
        try:
            prev_close = float(df['Close'].iloc[-2])
            today_open = float(df['Open'].iloc[-1])
            today_vol = float(df['Volume'].iloc[-1])
            avg_vol = float(df['Volume'].iloc[-21:-1].mean())
            gap = (today_open - prev_close) / prev_close
            if gap >= 0.03 and avg_vol > 0 and today_vol >= avg_vol * 3:
                gaps.append(ticker)
        except Exception:
            continue
    return gaps


def trend_following(market_data, regime, vix, spy_5d_return):
    signals = []
    candidates = []
    for ticker, df in market_data.items():
        if ticker in CRYPTO or ticker == '^VIX' or len(df) < 50:
            continue
        try:
            price = _latest(df, 'Close')
            ema20 = _latest(df, 'EMA20')
            ema50 = _latest(df, 'EMA50')
            ema200 = _latest(df, 'EMA200')
            if not all([price, ema20, ema50, ema200]):
                continue
            if not (ema20 > ema50 > ema200):
                continue
            score = sum([ema20 > ema50, ema50 > ema200, price > ema200 * 1.05])
            if score < 2:
                continue
            if spy_5d_return is not None and spy_5d_return < 0:
                if ticker in DEFENSIVE:
                    score *= 1.8
                elif ticker in AGGRESSIVE:
                    score *= 0.3
            if ticker not in ETF_TICKERS:
                score *= 1.5
            candidates.append((ticker, price, score))
        except Exception:
            continue
    candidates.sort(key=lambda x: -x[2])
    for ticker, price, _ in candidates[:5]:
        signals.append(_signal(ticker, 'BUY', 'trend', price, 0.05, f'EMA trend confirmed'))
    return signals


def mean_reversion(market_data, regime):
    signals = []
    for ticker, df in market_data.items():
        if ticker in CRYPTO or ticker == '^VIX' or len(df) < 20:
            continue
        try:
            price = _latest(df, 'Close')
            rsi = _latest(df, 'RSI14')
            bb_lower = _latest(df, 'BB_lower')
            adx = _latest(df, 'ADX')
            if not all([price, rsi, bb_lower]):
                continue
            rsi_threshold = 22 if regime == 'BEAR' else 30
            if rsi < rsi_threshold and price < bb_lower:
                if regime == 'BULL' and adx and adx >= 25:
                    continue
                signals.append(_signal(ticker, 'BUY', 'mean_rev', price, 0.05, f'RSI={rsi:.1f} below BB lower'))
        except Exception:
            continue
    return signals


def volatility_reversion(market_data, vix, vix_20d_avg):
    signals = []
    if not vix or not vix_20d_avg or vix_20d_avg == 0:
        return signals
    try:
        spy_df = market_data.get('SPY')
        if spy_df is None or len(spy_df) < 5:
            return signals
        spy_rsi = _latest(spy_df, 'RSI14')
        if vix >= vix_20d_avg * 1.4 and vix > 25 and spy_rsi and spy_rsi < 35:
            price = _latest(spy_df, 'Close')
            amp = '3x' if vix > 50 else ('2x' if vix > 35 else '1x')
            signals.append(_signal('SPY', 'BUY', 'vol_rev', price, 0.05, f'VIX spike {vix:.1f} amp={amp}'))
    except Exception:
        pass
    return signals


def earnings_drift(market_data, gap_up_stocks):
    signals = []
    for ticker in gap_up_stocks:
        df = market_data.get(ticker)
        if df is None or len(df) < 2:
            continue
        try:
            price = _latest(df, 'Close')
            signals.append(_signal(ticker, 'BUY', 'earnings_drift', price, 0.03, 'Gap up on volume'))
        except Exception:
            continue
    return signals


def crash_scanner(market_data, spy_day_return):
    if spy_day_return is None or spy_day_return > -0.01:
        return []
    candidates = []
    for ticker, df in market_data.items():
        if ticker in CRYPTO or ticker == '^VIX' or len(df) < 20:
            continue
        try:
            price = _latest(df, 'Close')
            prev = float(df['Close'].iloc[-2])
            rsi = _latest(df, 'RSI14')
            vol = float(df['Volume'].iloc[-1])
            avg_vol = float(df['Volume'].iloc[-21:-1].mean())
            drop = (price - prev) / prev
            if drop <= -0.05 and rsi and rsi < 25 and avg_vol > 0 and vol >= avg_vol * 1.5:
                score = abs(drop) * (30 - rsi)
                candidates.append((ticker, price, score))
        except Exception:
            continue
    candidates.sort(key=lambda x: -x[2])
    return [_signal(t, 'BUY', 'crash', p, 0.05, 'Crash buy') for t, p, _ in candidates[:3]]


def short_selling(market_data, spy_rsi):
    if not spy_rsi or spy_rsi <= 70:
        return []
    candidates = []
    for ticker, df in market_data.items():
        if ticker in CRYPTO or ticker == '^VIX' or ticker in ETF_TICKERS or len(df) < 50:
            continue
        try:
            price = _latest(df, 'Close')
            ema50 = _latest(df, 'EMA50')
            ema200 = _latest(df, 'EMA200')
            rsi = _latest(df, 'RSI14')
            if price and ema50 and ema200 and rsi:
                if price < ema50 and rsi > 65:
                    weakness = (ema200 - price) / ema200
                    candidates.append((ticker, price, weakness))
        except Exception:
            continue
    candidates.sort(key=lambda x: -x[2])
    return [_signal(t, 'SELL_SHORT', 'short', p, 0.08, 'Weak stock in overbought market') for t, p, _ in candidates[:2]]


def tlt_hedge(market_data, spy_monthly_return):
    if spy_monthly_return is None or spy_monthly_return >= -0.05:
        return []
    try:
        df = market_data.get('TLT')
        if df is None:
            return []
        price = _latest(df, 'Close')
        return [_signal('TLT', 'BUY', 'tlt_hedge', price, 0.08, f'SPY monthly={spy_monthly_return:.1%}')]
    except Exception:
        return []


def gld_allocation(market_data, regime, has_gld_position):
    if has_gld_position:
        return []
    try:
        df = market_data.get('GLD')
        if df is None:
            return []
        price = _latest(df, 'Close')
        pct = {'BULL': 0.08, 'NEUTRAL': 0.10, 'BEAR': 0.15}.get(regime, 0.10)
        return [_signal('GLD', 'BUY', 'gld', price, 0.10, f'Gold allocation target={pct}')]
    except Exception:
        return []


def crypto_trend(market_data, btc_above_ema200):
    if not btc_above_ema200:
        return []
    signals = []
    for ticker in ['BTC-USD', 'ETH-USD', 'SOL-USD']:
        df = market_data.get(ticker)
        if df is None or len(df) < 50:
            continue
        try:
            price = _latest(df, 'Close')
            ema20 = _latest(df, 'EMA20')
            ema50 = _latest(df, 'EMA50')
            ema200 = _latest(df, 'EMA200')
            if all([price, ema20, ema50, ema200]) and ema20 > ema50 > ema200:
                atr = _latest(df, 'ATR')
                stop_pct = min(0.25, max(0.10, (atr / price) * 2)) if atr else 0.15
                signals.append(_signal(ticker, 'BUY', 'crypto_trend', price, stop_pct, 'Crypto EMA trend'))
        except Exception:
            continue
    return signals


def run_all_strategies(market_data, regime, vix, vix_20d_avg, spy_data, gap_up_stocks, has_gld_position):
    spy_rsi = _latest(spy_data, 'RSI14') if spy_data is not None else None
    try:
        spy_5d = (float(spy_data['Close'].iloc[-1]) - float(spy_data['Close'].iloc[-6])) / float(spy_data['Close'].iloc[-6])
    except Exception:
        spy_5d = None
    try:
        spy_22d = (float(spy_data['Close'].iloc[-1]) - float(spy_data['Close'].iloc[-23])) / float(spy_data['Close'].iloc[-23])
    except Exception:
        spy_22d = None
    try:
        spy_day = (float(spy_data['Close'].iloc[-1]) - float(spy_data['Close'].iloc[-2])) / float(spy_data['Close'].iloc[-2])
    except Exception:
        spy_day = None
    try:
        btc_df = market_data.get('BTC-USD')
        btc_above = btc_df is not None and _latest(btc_df, 'Close') > _latest(btc_df, 'EMA200')
    except Exception:
        btc_above = False

    all_signals = []
    all_signals += trend_following(market_data, regime, vix, spy_5d)
    all_signals += mean_reversion(market_data, regime)
    all_signals += volatility_reversion(market_data, vix, vix_20d_avg)
    all_signals += earnings_drift(market_data, gap_up_stocks)
    all_signals += crash_scanner(market_data, spy_day)
    all_signals += short_selling(market_data, spy_rsi)
    all_signals += tlt_hedge(market_data, spy_22d)
    all_signals += gld_allocation(market_data, regime, has_gld_position)
    all_signals += crypto_trend(market_data, btc_above)

    seen = {}
    for s in all_signals:
        t = s['ticker']
        if t not in seen:
            seen[t] = s
    return list(seen.values())
