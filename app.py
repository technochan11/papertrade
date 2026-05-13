import os
import json
import logging
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from portfolio import init_db, get_state, get_portfolio_value, get_drawdown
from data import get_market_data, get_vix, get_current_price
from scheduler import (
    start_scheduler, MARKET_DATA, REGIME, STRATEGY_WEIGHTS,
    morning_data_fetch, update_regime, run_strategies, check_exits_job, daily_snapshot
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='.')
CORS(app)

with app.app_context():
    init_db()
    start_scheduler()


@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/health')
def health():
    return jsonify({'status': 'ok'})


def _get_portfolio_metrics(state, market_data):
    portfolio_value = get_portfolio_value(state, market_data)
    starting = state.get('starting_capital', 500000)
    drawdown = get_drawdown(portfolio_value, starting)
    total_return = (portfolio_value - starting) / starting

    spy_price = get_current_price('SPY')
    spy_baseline = state.get('spy_baseline') or spy_price
    spy_return = ((spy_price / spy_baseline) - 1) if spy_baseline and spy_price else 0
    alpha = total_return - spy_return

    try:
        from portfolio import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('SELECT portfolio_value FROM equity_history ORDER BY id DESC LIMIT 2')
        eq_rows = cur.fetchall()
        conn.close()
        daily_pnl = round(float(eq_rows[0][0]) - float(eq_rows[1][0]), 2) if len(eq_rows) >= 2 else 0.0
    except Exception:
        daily_pnl = 0.0

    return {
        'portfolio_value': round(portfolio_value, 2),
        'total_return': round(total_return * 100, 2),
        'vs_spy_alpha': round(alpha * 100, 2),
        'max_drawdown': round(-drawdown * 100, 2),
        'daily_pnl': daily_pnl,
        'open_positions': len(state.get('positions', [])),
        'max_positions': 5,
        'cash': round(state.get('cash', 0), 2),
    }


def _safe_last(df, col, default=0):
    try:
        s = df[col].dropna()
        return float(s.iloc[-1]) if len(s) > 0 else default
    except Exception:
        return default


@app.route('/api/portfolio')
def api_portfolio():
    try:
        state = get_state()
        md = MARKET_DATA if MARKET_DATA else get_market_data(['SPY'])
        metrics = _get_portfolio_metrics(state, md)

        spy_df = md.get('SPY')
        spy_price = get_current_price('SPY') or 0
        ema50 = _safe_last(spy_df, 'EMA50') if spy_df is not None else 0
        ema200 = _safe_last(spy_df, 'EMA200') if spy_df is not None else 0
        adx = _safe_last(spy_df, 'ADX') if spy_df is not None else 0
        try:
            close = spy_df['Close'].dropna() if spy_df is not None else None
            spy_1m = (float(close.iloc[-1]) - float(close.iloc[-23])) / float(close.iloc[-23]) if close is not None and len(close) >= 23 else 0
        except Exception:
            spy_1m = 0
        vix = get_vix()

        metrics.update({
            'regime': {
                'label':     REGIME,
                'spy_price': round(spy_price, 2),
                'ema50':     round(ema50, 2),
                'ema200':    round(ema200, 2),
                'return_1m': round(spy_1m * 100, 2),
                'adx':       round(adx, 2),
                'vix':       round(vix, 2),
            },
            'strategy_weights': STRATEGY_WEIGHTS,
            'beta': 1.0,
            'sharpe': 0.0,
            'correlation': 0.0,
        })
        return jsonify(metrics)
    except Exception as e:
        logger.error(f'Portfolio error: {e}', exc_info=True)
        return jsonify({
            'regime': {'label': 'NEUTRAL', 'spy_price': 0, 'ema50': 0, 'ema200': 0, 'return_1m': 0, 'adx': 0, 'vix': 0},
            'cash': 500000,
            'portfolio_value': 500000,
            'total_return': 0,
            'vs_spy_alpha': 0,
            'max_drawdown': 0,
            'daily_pnl': 0,
            'open_positions': 0,
            'max_positions': 5,
            'strategy_weights': {},
            'beta': 1, 'sharpe': 0, 'correlation': 0,
            'error': str(e),
        })


@app.route('/api/positions')
def api_positions():
    try:
        state = get_state()
        positions = state.get('positions', [])
        result = []
        for p in positions:
            price = get_current_price(p['ticker']) or p['entry_price']
            pnl = (price - p['entry_price']) / p['entry_price']
            result.append({
                'ticker': p['ticker'],
                'strategy': p['strategy'],
                'entry_price': p['entry_price'],
                'current_price': round(price, 4),
                'pnl_pct': round(pnl, 4),
                'stop_price': p['stop_price'],
                'position_value': round(price * p['shares'], 2),
                'days_held': p.get('days_held', 0),
            })
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/trades')
def api_trades():
    try:
        from portfolio import get_conn, PLACEHOLDER
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(f'SELECT date, symbol, action, strategy, price, shares, profit_pct FROM trades ORDER BY id DESC LIMIT 50')
        rows = cur.fetchall()
        conn.close()
        cols = ['date', 'symbol', 'action', 'strategy', 'price', 'shares', 'profit_pct']
        return jsonify([dict(zip(cols, r)) for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/equity-history')
def api_equity_history():
    try:
        from portfolio import get_conn, get_state as _get_state
        conn = get_conn()
        cur = conn.cursor()

        # Deduplicate by date — keep the latest entry per date
        cur.execute('''
            SELECT date, portfolio_value, spy_value
            FROM equity_history
            WHERE id IN (SELECT MAX(id) FROM equity_history GROUP BY date)
            ORDER BY date ASC LIMIT 90
        ''')
        rows = list(cur.fetchall())

        if len(rows) < 2:
            # Synthesize from trades: each trade row stores portfolio_value at that moment
            cur.execute('SELECT date, portfolio_value FROM trades ORDER BY id ASC')
            trade_rows = cur.fetchall()
            if trade_rows:
                seen = {}
                for date_str, pv in trade_rows:
                    seen[date_str[:10]] = float(pv)
                state = _get_state()
                spy_baseline = state.get('spy_baseline') or 0
                spy_now = get_current_price('SPY') or spy_baseline or 0
                dates = sorted(seen.keys())
                n = len(dates)
                synth = []
                for i, d in enumerate(dates):
                    pv = seen[d]
                    # linear interpolation of SPY from baseline → current
                    frac = i / max(n - 1, 1)
                    spy_val = round(spy_baseline + frac * (spy_now - spy_baseline) * 500000 / spy_now, 2) if spy_now else 500000
                    synth.append((d, pv, spy_val))
                rows = synth

        conn.close()

        # Always append the live current value as today's final point
        try:
            state = _get_state()
            md = MARKET_DATA if MARKET_DATA else {}
            live_pv = get_portfolio_value(state, md)
            spy_now = get_current_price('SPY') or 0
            state2 = _get_state()
            spy_baseline = state2.get('spy_baseline') or spy_now
            spy_scaled = round(spy_now / spy_baseline * 500000, 2) if spy_baseline else 500000
            today = datetime.now().strftime('%Y-%m-%d')
            if not rows or rows[-1][0] != today:
                rows.append((today, live_pv, spy_scaled))
            else:
                rows[-1] = (today, live_pv, spy_scaled)
        except Exception:
            pass

        return jsonify([{'date': r[0], 'portfolio_value': round(float(r[1]), 2),
                         'spy_value': round(float(r[2]), 2) if r[2] is not None else None}
                        for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics')
def api_metrics():
    try:
        from portfolio import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('SELECT portfolio_value, spy_value FROM equity_history ORDER BY date')
        rows = cur.fetchall()
        conn.close()

        if len(rows) < 10:
            return jsonify({'alpha_ann': 0, 'beta': 1, 'sharpe': 0, 'correlation': 0})

        import numpy as np
        port = [r[0] for r in rows]
        spy = [r[1] for r in rows]
        port_ret = np.diff(port) / np.array(port[:-1])
        spy_ret = np.diff(spy) / np.array(spy[:-1])

        cov = np.cov(port_ret, spy_ret)
        beta = float(cov[0, 1] / cov[1, 1]) if cov[1, 1] != 0 else 1.0
        ann_port = float(np.mean(port_ret) * 252)
        ann_spy = float(np.mean(spy_ret) * 252)
        alpha = ann_port - beta * ann_spy
        sharpe = float(np.mean(port_ret) / np.std(port_ret) * (252 ** 0.5)) if np.std(port_ret) > 0 else 0
        correlation = float(np.corrcoef(port_ret, spy_ret)[0, 1])

        return jsonify({'alpha_ann': round(alpha * 100, 2), 'beta': round(beta, 4),
                        'sharpe': round(sharpe, 4), 'correlation': round(correlation, 4)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/run-strategy', methods=['POST'])
def api_run_strategy():
    try:
        morning_data_fetch()
        update_regime()
        run_strategies()
        return jsonify({'status': 'ok', 'message': 'Strategy run complete', 'regime': REGIME})
    except Exception as e:
        logger.error(f'run-strategy error: {e}', exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/backup')
def api_backup():
    try:
        from portfolio import get_conn
        conn = get_conn()
        cur = conn.cursor()
        backup = {}
        for table in ['trades', 'equity_history', 'portfolio_state']:
            cur.execute(f'SELECT * FROM {table}')
            cols = [d[0] for d in cur.description]
            backup[table] = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        return jsonify(backup)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
