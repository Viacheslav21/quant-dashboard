# Quant Dashboard

Read-only FastAPI dashboard for monitoring a prediction-market trading engine ("quant-engine") and arbitrage bot ("quant-arbitrage"). Connects to a shared PostgreSQL database. Includes on-demand AI analysis via Claude Sonnet.

## Stack

- **Python** (FastAPI + uvicorn)
- **asyncpg** for Postgres
- **Anthropic SDK** (Claude Sonnet for on-demand analysis)
- Deployed via `Procfile` (`web: python app.py`)

## Project structure

```
app.py           — Main FastAPI app: routes, HTML rendering, config
utils/db.py      — Database class (asyncpg pool, read-only queries)
requirements.txt — Python dependencies
Procfile         — Process runner entry point
```

## Pages

### Dashboard (`/`)
- Key metrics: bankroll, ROI, win rate (W/L), avg EV, avg Kelly, open position count
- Cumulative P&L line chart (Chart.js)
- Open positions table: question, side, entry/current price, unrealized P&L, EV, KL, stake, market link
- Recent signals table (10 latest): question, side, market price, p_final, EV, KL, source
- Closed positions history (paginated, 100/page): question, side, entry price, outcome, P&L, result, EV

### Analytics (`/analytics`)
- Summary: win rate, EV predicted vs actual, avg position lifetime (hours)
- Config A/B testing table: compare parameter sets by trades, win rate, P&L, EV, stake
- Breakdown tables: by theme, by source (math/news/claude), by side (YES/NO), by close reason (TP/SL/RESOLVED)
- Calibration table: predicted probability buckets (0-30%, 30-50%, 50-70%, 70-100%) vs actual win rates with bias
- Daily P&L table (14 days) and bar chart
- Charts: cumulative P&L, daily P&L, calibration (predicted vs actual), win rate by theme
- Signal backtest (last 50): execution status, direction accuracy, missed profit vs saved by rejection
- Market metrics (top 50 active): volatility (ATR), momentum, volume ratio
- AI analysis button: triggers Claude Sonnet analysis on all metrics

### Arbitrage (`/arbitrage`)
- Arb-specific stats: bankroll, ROI, P&L, win rate, open positions, avg hold time (minutes)
- Arb cumulative P&L chart
- Open arb positions and recent arb signals tables
- Analytics: by group name, by close reason, by side, daily P&L
- Closed arb history (paginated)

### API
- `GET /api` — JSON stats: bankroll, open/closed counts
- `POST /api/run-analysis` — Triggers Claude Sonnet-4.5 analysis, returns recommendations (max 500 words)

## AI Analysis

On-demand via button on analytics page. Sends to Claude Sonnet: overall stats, win rates by theme/source/side, calibration data, EV accuracy, close reasons, current config. Returns actionable recommendations: what's working, what's not, specific config changes, risks. Max 800 tokens, plain text.

## Environment variables

- `DATABASE_URL` — PostgreSQL connection string (required)
- `ANTHROPIC_API_KEY` — Anthropic API key (for AI analysis)
- `BANKROLL` — Starting bankroll for ROI calculation (default: 1000)
- `MIN_EV`, `MIN_KL`, `MAX_KELLY_FRAC`, `TAKE_PROFIT_PCT`, `STOP_LOSS_PCT` — Trading parameters displayed in config comparison

## Running locally

```sh
pip install -r requirements.txt
# Requires DATABASE_URL env var pointing to the shared quant-engine Postgres
python app.py
```
