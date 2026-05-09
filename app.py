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

    return {
        'portfolio_value': round(portfolio_value, 2),
        'total_return': round(total_return, 4),
        'daily_pnl': 0.0,
        'drawdown': round(drawdown, 4),
        'alpha': round(alpha, 4),
        'open_positions': len(state.get('positions', [])),
        'cash': round(state.get('cash', 0), 2),
        'regime': REGIME,
    }


@app.route('/api/portfolio')
def api_portfolio():
    try:
        state = get_state()
        md = MARKET_DATA if MARKET_DATA else get_market_data(['SPY'])
        metrics = _get_portfolio_metrics(state, md)

        spy_df = md.get('SPY')
        spy_price = get_current_price('SPY') or 0
        ema50 = float(spy_df['EMA50'].dropna().iloc[-1]) if spy_df is not None else 0
        ema200 = float(spy_df['EMA200'].dropna().iloc[-1]) if spy_df is not None else 0
        try:
            spy_1m = (float(spy_df['Close'].iloc[-1]) - float(spy_df['Close'].iloc[-23])) / float(spy_df['Close'].iloc[-23])
        except Exception:
            spy_1m = 0
        adx = float(spy_df['ADX'].dropna().iloc[-1]) if spy_df is not None else 0
        vix = get_vix()

        metrics.update({
            'spy_price': round(spy_price, 2),
            'spy_ema50': round(ema50, 2),
            'spy_ema200': round(ema200, 2),
            'spy_1m_return': round(spy_1m, 4),
            'adx': round(adx, 2),
            'vix': round(vix, 2),
            'strategy_weights': STRATEGY_WEIGHTS,
            'beta': 1.0,
            'sharpe': 0.0,
            'correlation': 0.0,
        })
        return jsonify(metrics)
    except Exception as e:
        logger.error(f'/api/portfolio error: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


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
        from portfolio import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('SELECT date, portfolio_value, spy_value FROM equity_history ORDER BY date DESC LIMIT 90')
        rows = cur.fetchall()
        conn.close()
        rows.reverse()
        return jsonify([{'date': r[0], 'portfolio_value': r[1], 'spy_value': r[2]} for r in rows])
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
            return jsonify({'alpha': 0, 'beta': 1, 'sharpe': 0, 'correlation': 0})

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

        return jsonify({'alpha': round(alpha, 4), 'beta': round(beta, 4),
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
