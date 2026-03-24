# Quant Dashboard

Read-only FastAPI dashboard for monitoring a prediction-market trading engine ("quant-engine") and arbitrage bot ("quant-arbitrage"). Connects to a shared PostgreSQL database. Includes on-demand AI analysis via Claude Sonnet.

## Stack

- **Python** (FastAPI + uvicorn + Jinja2 templates)
- **asyncpg** for Postgres
- **Anthropic SDK** (Claude Sonnet for on-demand analysis)
- **Chart.js** for frontend charts
- Deployed via `Procfile` (`web: python app.py`)

## Project structure

```
app.py                — FastAPI routes and data orchestration (~250 lines)
utils/
  db.py               — Database class (asyncpg pool, read-only queries)
  helpers.py           — Shared helpers: pc(), wr_color(), to_json()
  metrics.py           — Pure Python metric computation: Sharpe, drawdown, streaks, equity curve, PnL distribution
templates/
  base.html            — Shared layout: CSS, nav, sort JS, footer
  dashboard.html       — Main dashboard page
  analytics.html       — Analytics page
  arbitrage.html       — Arbitrage page
requirements.txt
Procfile
```

## Pages

### Dashboard (`/`)
- Key metrics: bankroll, ROI, win rate, Sharpe ratio, max drawdown, win/loss streaks, avg EV, open position count
- Charts: cumulative P&L, equity curve, drawdown
- Open positions table with profit/loss count and total unrealized P&L
- Recent signals table (10 latest)
- Best/worst trade cards
- Rolling 7d/30d performance cards (P&L + win rate)
- Closed positions history (paginated with OFFSET/LIMIT, date filter, CSV export)

### Analytics (`/analytics`)
- Summary: win rate, EV predicted vs actual, avg lifetime, Sharpe, max drawdown
- Best/worst trade, rolling 7d/30d P&L
- Date range filter
- Config A/B testing table
- Breakdown tables: by theme, by source, by side, by close reason
- Calibration table and chart
- Charts: cumulative P&L, daily P&L, calibration, win rate by theme (pie), equity curve, drawdown, P&L distribution histogram
- Signal backtest (last 50)
- Market metrics (top 50 active)
- AI analysis button (Claude Sonnet)

### Arbitrage (`/arbitrage`)
- Arb-specific stats: bankroll, ROI, P&L, win rate, open positions, avg hold time
- Arb cumulative P&L chart
- Open arb positions with profit/loss count and total uPnL
- Recent arb signals
- Analytics: by group, by close reason, by side, daily P&L
- Closed arb history (paginated with OFFSET/LIMIT)

### API
- `GET /api` — JSON stats: bankroll, open/closed counts
- `GET /api/export/positions?date_from=&date_to=` — CSV export of closed positions
- `POST /api/run-analysis` — Triggers Claude Sonnet analysis, returns recommendations

## Key architecture decisions

- **Jinja2 templates** with base template inheritance — CSS/nav/sort JS defined once in `base.html`
- **Helper functions** (`pc`, `wr_color`) registered as Jinja2 globals, no duplication
- **Pagination** uses SQL `OFFSET/LIMIT` (not Python slicing)
- **Metrics computation** (Sharpe, drawdown, streaks) is pure Python in `utils/metrics.py`, separated from DB layer
- All tables have sortable column headers (client-side JS)

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
