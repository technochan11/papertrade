# PaperTrade Pro

A full paper trading dashboard running 5 automated strategies on $500,000 virtual capital.

## Local Setup

```bash
pip install -r requirements.txt
python app.py          # runs on http://localhost:5000
# Open index.html in browser (or serve it)
```

## Deploy Backend → Render.com (Free, 24/7)

1. Push this folder to a GitHub repo  
   ```bash
   git init && git add . && git commit -m "init"
   gh repo create papertrade --public --push --source=.
   ```
2. Go to [render.com](https://render.com) → **New** → **Web Service**
3. Connect your GitHub repo
4. Render auto-detects `render.yaml` — click **Deploy**
5. Wait ~3 min. Copy your URL: `https://papertrade-api.onrender.com`

## Deploy Frontend → GitHub Pages (Free)

1. Edit `index.html` line ~172 — replace the `YOUR-RENDER-APP` placeholder:
   ```js
   : 'https://papertrade-api.onrender.com'  // your real URL
   ```
2. Go to your GitHub repo → **Settings** → **Pages**
3. Source: **Deploy from branch** → `main` → `/` (root)
4. Your dashboard is live at: `https://yourusername.github.io/papertrade/`

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Health check |
| `/api/portfolio` | GET | Portfolio summary |
| `/api/positions` | GET | Open positions |
| `/api/trades` | GET | Last 20 trades |
| `/api/regime` | GET | Market regime |
| `/api/equity-curve` | GET | 90-day history |
| `/api/run-strategy` | POST | Manually trigger run |

## Strategies

1. **Trend Following** — EMA 20/50/200 crossover, momentum scoring
2. **Mean Reversion** — RSI + Bollinger Band oversold entries
3. **Volatility Reversion** — VIX spike + SPY RSI oversold (SPY only)
4. **Earnings Drift** — Gap-up on 3x volume
5. **Crypto Trend** — BTC-confirmed EMA trend for BTC/ETH/SOL

## Notes

- Free Render tier **sleeps after 15 min inactivity** — first request may take 30-60s to wake up
- Use [UptimeRobot](https://uptimerobot.com) (free) to ping `/health` every 5 min to keep it awake
- No real money involved — paper trading only
