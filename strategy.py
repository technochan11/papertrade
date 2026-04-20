import pandas as pd
import numpy as np
from ta.trend import EMAIndicator, ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands, AverageTrueRange
import logging

logger = logging.getLogger(__name__)

ETFS = [
    "SPY","QQQ","IWM","XLK","XLF","XLE","XLV",
    "XLI","XLC","XLY","XLP","XLU","XLRE","XLB",
    "IEF","GLD","TLT","SHY"
]

def get_regime(spy_data):
    if spy_data is None or len(spy_data) < 200:
        return "NEUTRAL"
    close = spy_data["Close"]
    ema50 = EMAIndicator(close, window=50).ema_indicator().iloc[-1]
    ema200 = EMAIndicator(close, window=200).ema_indicator().iloc[-1]
    price = close.iloc[-1]
    if price > ema50 and ema50 > ema200 * 0.98:
        return "BULL"
    elif price < ema200 * 0.97:
        return "BEAR"
    return "NEUTRAL"

def get_adx(spy_data):
    if spy_data is None or len(spy_data) < 20:
        return 0.0
    try:
        adx = ADXIndicator(
            spy_data["High"],
            spy_data["Low"],
            spy_data["Close"],
            window=14
        )
        val = adx.adx().iloc[-1]
        return float(val) if not pd.isna(val) else 0.0
    except Exception:
        return 0.0

def momentum_score(data):
    if data is None or len(data) < 200:
        return 0
    close = data["Close"]
    price = close.iloc[-1]
    ema20 = EMAIndicator(close, window=20).ema_indicator().iloc[-1]
    ema50 = EMAIndicator(close, window=50).ema_indicator().iloc[-1]
    ema200 = EMAIndicator(close, window=200).ema_indicator().iloc[-1]
    score = 0
    if ema20 > ema50:
        score += 1
    if ema50 > ema200:
        score += 1
    if price > ema200 * 1.05:
        score += 1
    return score

def get_atr(data, window=14):
    if data is None or len(data) < window + 1:
        return None
    try:
        atr = AverageTrueRange(
            data["High"], data["Low"],
            data["Close"], window=window
        )
        val = atr.average_true_range().iloc[-1]
        return float(val) if not pd.isna(val) else None
    except Exception:
        return None

def trend_following(
    tickers, market_data, portfolio,
    regime, adx, weights, max_pos
):
    if regime == "BEAR" and adx <= 20:
        return []
    signals = []
    open_count = portfolio.get_open_count()
    if open_count >= max_pos:
        return []
    candidates = []
    for ticker in tickers:
        data = market_data.get(ticker)
        if data is None or len(data) < 200:
            continue
        if portfolio.has_position(ticker):
            continue
        close = data["Close"]
        price = close.iloc[-1]
        if price <= 0:
            continue
        ema20 = EMAIndicator(
            close, window=20
        ).ema_indicator().iloc[-1]
        ema50 = EMAIndicator(
            close, window=50
        ).ema_indicator().iloc[-1]
        ema200 = EMAIndicator(
            close, window=200
        ).ema_indicator().iloc[-1]
        if not (ema20 > ema50 and ema50 > ema200 * 0.99):
            continue
        score = momentum_score(data)
        if score < 2:
            continue
        mom = (ema20 - ema200) / ema200
        if ticker not in ETFS:
            mom *= 1.5
        atr = get_atr(data)
        candidates.append({
            "ticker": ticker,
            "momentum": mom,
            "price": price,
            "atr": atr
        })
    candidates.sort(
        key=lambda x: x["momentum"], reverse=True
    )
    weight = weights.get("trend", 0.2)
    for c in candidates:
        if open_count >= max_pos:
            break
        ticker = c["ticker"]
        price = c["price"]
        atr = c["atr"]
        if atr is None or atr <= 0:
            stop_pct = 0.08
        else:
            stop_pct = max(0.06, min(0.20, (atr * 3) / price))
        size = portfolio.get_position_size(
            "trend", stop_pct, weight
        )
        shares = int(size / price)
        if shares <= 0:
            continue
        stop_price = price * (1 - stop_pct)
        signals.append({
            "ticker": ticker,
            "action": "BUY",
            "strategy": "trend",
            "price": price,
            "shares": shares,
            "stop_price": stop_price,
            "reason": f"TREND mom={c['momentum']*100:.1f}%"
        })
        open_count += 1
    return signals

def mean_reversion(
    tickers, market_data, portfolio,
    regime, adx, weights, max_pos
):
    if regime == "BULL" and adx >= 25:
        return []
    signals = []
    open_count = portfolio.get_open_count()
    if open_count >= max_pos:
        return []
    weight = weights.get("reversion", 0.2)
    for ticker in tickers:
        if open_count >= max_pos:
            break
        data = market_data.get(ticker)
        if data is None or len(data) < 20:
            continue
        if portfolio.has_position(ticker):
            continue
        close = data["Close"]
        price = close.iloc[-1]
        if price <= 0:
            continue
        rsi = RSIIndicator(
            close, window=14
        ).rsi().iloc[-1]
        bb = BollingerBands(close, window=20, window_dev=2)
        lower = bb.bollinger_lband().iloc[-1]
        if pd.isna(rsi) or pd.isna(lower):
            continue
        if regime == "BEAR" and rsi > 22:
            continue
        if not (rsi < 30 and price < lower):
            continue
        atr = get_atr(data)
        if atr is None or atr <= 0:
            stop_pct = 0.05
        else:
            stop_pct = max(0.03, min(0.08, (atr * 2) / price))
        size = portfolio.get_position_size(
            "reversion", stop_pct, weight
        )
        shares = int(size / price)
        if shares <= 0:
            continue
        stop_price = price * (1 - stop_pct)
        signals.append({
            "ticker": ticker,
            "action": "BUY",
            "strategy": "reversion",
            "price": price,
            "shares": shares,
            "stop_price": stop_price,
            "reason": f"REV RSI={rsi:.1f}"
        })
        open_count += 1
    return signals

def volatility_reversion(
    spy_data, vix_data, portfolio,
    weights, max_pos
):
    signals = []
    if portfolio.has_position("SPY"):
        return signals
    if portfolio.get_open_count() >= max_pos:
        return signals
    if vix_data is None or len(vix_data) < 20:
        return signals
    if spy_data is None or len(spy_data) < 14:
        return signals
    vix_close = vix_data["Close"]
    vix_now = vix_close.iloc[-1]
    vix_avg = vix_close.tail(20).mean()
    if vix_avg <= 0:
        return signals
    if vix_now < vix_avg * 1.40:
        return signals
    if vix_now < 25:
        return signals
    spy_rsi = RSIIndicator(
        spy_data["Close"], window=14
    ).rsi().iloc[-1]
    if pd.isna(spy_rsi) or spy_rsi >= 35:
        return signals
    price = spy_data["Close"].iloc[-1]
    weight = weights.get("vol_rev", 0.2)
    size = portfolio.get_position_size(
        "vol_rev", 0.07, weight
    )
    shares = int(size / price)
    if shares <= 0:
        return signals
    signals.append({
        "ticker": "SPY",
        "action": "BUY",
        "strategy": "vol_rev",
        "price": price,
        "shares": shares,
        "stop_price": price * 0.93,
        "reason": f"VOL_REV VIX={vix_now:.1f}"
    })
    return signals

def earnings_drift(
    drift_candidates, market_data,
    portfolio, regime, weights, max_pos
):
    if regime == "BEAR":
        return []
    signals = []
    open_count = portfolio.get_open_count()
    if open_count >= max_pos:
        return []
    weight = weights.get("drift", 0.2)
    for ticker in drift_candidates:
        if open_count >= max_pos:
            break
        data = market_data.get(ticker)
        if data is None or len(data) < 31:
            continue
        if portfolio.has_position(ticker):
            continue
        close = data["Close"]
        volume = data["Volume"]
        today_open = data["Open"].iloc[-1]
        prev_close = close.iloc[-2]
        if prev_close <= 0:
            continue
        gap = (today_open - prev_close) / prev_close
        if gap < 0.03:
            continue
        avg_vol = volume.tail(30).mean()
        today_vol = volume.iloc[-1]
        if avg_vol <= 0 or today_vol < avg_vol * 3:
            continue
        price = today_open
        stop_pct = 0.08
        size = portfolio.get_position_size(
            "drift", stop_pct, weight
        )
        shares = int(size / price)
        if shares <= 0:
            continue
        signals.append({
            "ticker": ticker,
            "action": "BUY",
            "strategy": "drift",
            "price": price,
            "shares": shares,
            "stop_price": price * 0.92,
            "reason": f"DRIFT gap={gap*100:.1f}%"
        })
        open_count += 1
    return signals

def crypto_trend(
    crypto_tickers, crypto_data, btc_data,
    portfolio, regime, weights, max_pos
):
    signals = []
    open_count = portfolio.get_open_count()
    if open_count >= max_pos:
        return signals
    if btc_data is None or len(btc_data) < 200:
        return signals
    btc_close = btc_data["Close"]
    btc_price = btc_close.iloc[-1]
    btc_ema200 = EMAIndicator(
        btc_close, window=200
    ).ema_indicator().iloc[-1]
    if btc_price < btc_ema200 * 0.95:
        logger.info("CRYPTO BEAR - skipping all crypto")
        return signals
    weight = weights.get("crypto", 0.2)
    for ticker in crypto_tickers:
        if open_count >= max_pos:
            break
        data = crypto_data.get(ticker)
        if data is None or len(data) < 200:
            continue
        if portfolio.has_position(ticker):
            continue
        close = data["Close"]
        price = close.iloc[-1]
        if price <= 0:
            continue
        ema20 = EMAIndicator(
            close, window=20
        ).ema_indicator().iloc[-1]
        ema50 = EMAIndicator(
            close, window=50
        ).ema_indicator().iloc[-1]
        ema200 = EMAIndicator(
            close, window=200
        ).ema_indicator().iloc[-1]
        if not (ema20 > ema50 and ema50 > ema200 * 0.98):
            continue
        score = momentum_score(data)
        if score < 2:
            continue
        atr = get_atr(data)
        if atr is None or atr <= 0:
            stop_pct = 0.15
        else:
            stop_pct = max(0.10, min(0.25, (atr * 2) / price))
        portfolio_value = portfolio.get_portfolio_value()
        if portfolio.in_caution:
            pos_val = portfolio_value * 0.025 / stop_pct * 0.5
        else:
            pos_val = portfolio_value * 0.025 / stop_pct
        max_val = portfolio_value * 0.20
        pos_val = min(pos_val, max_val)
        quantity = round(pos_val / price, 6)
        if quantity <= 0:
            continue
        signals.append({
            "ticker": ticker,
            "action": "BUY",
            "strategy": "crypto",
            "price": price,
            "shares": quantity,
            "stop_price": price * (1 - stop_pct),
            "reason": f"CRYPTO mom score={score}"
        })
        open_count += 1
    return signals

def check_exits(positions, market_data, crypto_data, regime):
    exits = []
    for pos in positions:
        ticker = pos["ticker"]
        strategy = pos["strategy"]
        entry_price = pos["entry_price"]
        shares = pos["shares"]
        days_held = pos.get("days_held", 0)
        stop_price = pos.get("stop_price", 0)
        data = market_data.get(ticker)
        if data is None:
            data = crypto_data.get(ticker)
        if data is None:
            continue
        price = data["Close"].iloc[-1]
        if price <= 0:
            continue
        profit_pct = (price - entry_price) / entry_price
        new_stop = update_trailing_stop(
            price, entry_price, stop_price
        )
        pos["stop_price"] = new_stop
        should_exit = False
        exit_reason = ""
        if price <= new_stop:
            should_exit = True
            exit_reason = f"TRAILING STOP profit={profit_pct*100:.1f}%"
        if not should_exit:
            if strategy == "trend":
                if days_held >= 5:
                    close = data["Close"]
                    if len(close) >= 50:
                        ema20 = EMAIndicator(
                            close, window=20
                        ).ema_indicator().iloc[-1]
                        ema50 = EMAIndicator(
                            close, window=50
                        ).ema_indicator().iloc[-1]
                        if ema20 < ema50 * 0.95:
                            should_exit = True
                            exit_reason = "TREND EXIT MA"
                    if regime == "BEAR" and ticker not in [
                        "BTC-USD", "ETH-USD", "SOL-USD"
                    ]:
                        should_exit = True
                        exit_reason = "TREND EXIT BEAR REGIME"
            elif strategy == "reversion":
                if days_held >= 2:
                    close = data["Close"]
                    if len(close) >= 20:
                        rsi = RSIIndicator(
                            close, window=14
                        ).rsi().iloc[-1]
                        bb = BollingerBands(
                            close, window=20, window_dev=2
                        )
                        mid = bb.bollinger_mavg().iloc[-1]
                        if not pd.isna(rsi) and not pd.isna(mid):
                            if rsi > 50 or price > mid:
                                should_exit = True
                                exit_reason = f"REV EXIT RSI={rsi:.1f}"
            elif strategy == "drift":
                gap_open = pos.get("gap_open", entry_price)
                if days_held >= 10:
                    should_exit = True
                    exit_reason = "DRIFT 10d"
                elif price < gap_open * 0.97:
                    should_exit = True
                    exit_reason = "DRIFT GAP FILL"
            elif strategy == "vol_rev":
                if days_held >= 15:
                    should_exit = True
                    exit_reason = "VOL_REV 15d"
            elif strategy == "crypto":
                if days_held >= 5:
                    close = data["Close"]
                    if len(close) >= 50:
                        ema20 = EMAIndicator(
                            close, window=20
                        ).ema_indicator().iloc[-1]
                        ema50 = EMAIndicator(
                            close, window=50
                        ).ema_indicator().iloc[-1]
                        if ema20 < ema50 * 0.95:
                            should_exit = True
                            exit_reason = "CRYPTO EXIT MA"
        if not should_exit and profit_pct > 1.0:
            harvested = pos.get("harvested", False)
            if not harvested:
                harvest_shares = int(shares * 0.25)
                if harvest_shares > 0:
                    exits.append({
                        "ticker": ticker,
                        "action": "SELL",
                        "strategy": strategy,
                        "price": price,
                        "shares": harvest_shares,
                        "reason": f"HARVEST 25% profit={profit_pct*100:.0f}%",
                        "partial": True
                    })
                    pos["shares"] = shares - harvest_shares
                    pos["harvested"] = True
            continue
        if should_exit:
            exits.append({
                "ticker": ticker,
                "action": "SELL",
                "strategy": strategy,
                "price": price,
                "shares": shares,
                "reason": exit_reason,
                "profit_pct": profit_pct,
                "partial": False
            })
    return exits

def update_trailing_stop(price, entry_price, current_stop):
    profit_pct = (price - entry_price) / entry_price
    if profit_pct > 1.00:
        stop_mult = 0.93
    elif profit_pct > 0.50:
        stop_mult = 0.90
    elif profit_pct > 0.25:
        stop_mult = 0.87
    else:
        stop_mult = 0.78
    new_stop = price * stop_mult
    return max(new_stop, current_stop)

def detect_gap_ups(tickers, market_data):
    candidates = []
    for ticker in tickers:
        data = market_data.get(ticker)
        if data is None or len(data) < 31:
            continue
        try:
            today_open = data["Open"].iloc[-1]
            prev_close = data["Close"].iloc[-2]
            volume = data["Volume"]
            today_vol = volume.iloc[-1]
            avg_vol = volume.tail(30).mean()
            if prev_close <= 0 or avg_vol <= 0:
                continue
            gap = (today_open - prev_close) / prev_close
            if gap >= 0.03 and today_vol >= avg_vol * 3:
                candidates.append(ticker)
        except Exception:
            continue
    return candidates

def calculate_weights(trade_history):
    strategies = [
        "trend", "reversion", "vol_rev",
        "drift", "crypto"
    ]
    weights = {}
    for strat in strategies:
        trades = [
            t for t in trade_history
            if t.get("strategy") == strat
            and t.get("profit_pct") is not None
        ]
        if len(trades) < 10:
            weights[strat] = 0.2
            continue
        returns = [t["profit_pct"] for t in trades[-30:]]
        win_rate = sum(1 for r in returns if r > 0) / len(returns)
        avg_return = sum(returns) / len(returns)
        if avg_return < 0 and win_rate < 0.35:
            w = 0.05
        elif win_rate > 0.60 and avg_return > 0:
            w = 0.35
        else:
            w = max(
                0.05,
                win_rate * 0.6 + max(0, avg_return) * 5 * 0.4
            )
        weights[strat] = max(0.05, min(0.40, w))
    total = sum(weights.values())
    if total > 0:
        for s in weights:
            weights[s] /= total
    total = sum(weights.values())
    if total > 0:
        for s in weights:
            weights[s] = max(0.05, min(0.40, weights[s]))
    return weights
