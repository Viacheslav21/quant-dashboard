# Quant Dashboard

FastAPI dashboard for monitoring a prediction-market trading engine ("quant-engine") and micro bot ("quant-micro"). Connects to a shared PostgreSQL database. Writes to `trader_commands` (manual close), `patterns.blocked` (theme block/unblock), and `config_live` + `config_live_history` (live config editing).

## Stack

- **Python** (FastAPI + uvicorn + Jinja2 templates)
- **asyncpg** for Postgres
- **Chart.js** for frontend charts
- Deployed via `Procfile` (`web: python app.py`)

## Project structure

```
app.py                — Slim orchestrator: startup, auth, middleware (~180 lines)
routes/
  deps.py             — Shared dependencies: db, config, templates, helpers
  pages.py            — HTML pages: /, /analytics, /micro, /model, /config (~310 lines)
  api.py              — Core API: /api, commands, export, diagnostics, config (~160 lines)
  mobile.py           — Mobile API: /api/mobile/* (~140 lines)
  audit.py            — System + micro audit reports (~990 lines)
  ml_proxy.py         — ML service proxy: /api/ml/* (~50 lines)
utils/
  db.py               — Database class (asyncpg pool, queries + trader_commands write, config_live read/write, TTL cache) (~800 lines)
  helpers.py           — Shared helpers: pc(), wr_color(), to_json()
  metrics.py           — Pure Python metric computation: Sharpe, drawdown, streaks, equity curve, PnL distribution
templates/
  base.html            — Shared layout: CSS, nav, sort JS, footer
  dashboard.html       — Main dashboard page
  analytics.html       — Analytics page
  micro.html           — Micro bot page
  config.html          — Live config editor page
  model.html           — ML model health page
  login.html           — Login form
requirements.txt
Procfile
```

## Pages

### Dashboard (`/`)
- Key metrics: bankroll, ROI, win rate, Sharpe ratio, max drawdown, avg EV, open position count
- Charts: equity curve
- Open positions table with profit/loss count and total unrealized P&L
- Recent signals table (10 latest)
- Rolling 7d/30d performance cards (P&L + win rate)
- Closed positions history (paginated with OFFSET/LIMIT, date filter, CSV export)

### Analytics (`/analytics`)
- Summary: win rate, EV predicted vs actual, avg lifetime, Sharpe, max drawdown
- Best/worst trade, rolling 7d/30d P&L
- Date range filter
- Config A/B testing table
- Breakdown tables: by theme, by source, by side, by close reason
- Calibration table and chart, CLV analytics, DMA weights
- Theme block/unblock buttons with custom modal dialog (writes to patterns.blocked via API)
- Charts: cumulative P&L, daily P&L, calibration, win rate by theme (pie), equity curve, drawdown, P&L distribution histogram
- Signal backtest (last 50)
- Market metrics (top 50 active)

### Micro (`/micro`)
- Micro bot stats: bankroll (from config_live BANKROLL), P&L, win rate, open positions, staked capital
- Best/worst trade cards
- By Close Reason table | By Side pie chart
- By Theme + Theme Calibration merged table with block/unblock buttons
- Config A/B comparison table (by config_tag)
- Daily P&L bar chart

### Config (`/config`)
- Live config editor for all engine and micro parameters (42 total: 23 engine, 19 micro)
- Parameters grouped by section: signals, risk, sizing, capacity, timing, filters, claude, general
- Micro BANKROLL editable (bankroll read from config_live, not hardcoded)
- Per-parameter validation (type: float/int/bool/str, min/max bounds)
- Version tracking per key (incremented on each change)
- Change history log with old/new values and timestamps
- Writes to `config_live` table + sends `NOTIFY config_reload` for instant pickup by engine and micro
- Config A/B comparison table also shown on the micro page

### Model (`/model`)
- ML model health check (proxies to quant-ml service)
- Training metrics, feature importance
- Trigger training via dashboard

### API
- `GET /api` — JSON stats: bankroll, open/closed counts
- `GET /api/export/positions` — CSV export of closed positions (date filter)
- `POST /api/commands/close` — Insert close command + NOTIFY to engine
- `POST /api/commands/theme-block` — Block/unblock a theme (writes to patterns.blocked)
- `GET /api/themes` — List all themes with blocked status
- `GET /api/system-audit` — Comprehensive multi-section text audit report
- `GET /api/micro-audit` — Micro bot audit report (sections: WR by quality score, WR by days left, WR by config tag, theme calibration, SL blacklist)
- `GET /api/diagnostics` — Deep WR diagnostics in JSON
- `POST /api/ml/train`, `POST /api/ml/train-only` — Proxy ML training triggers
- `GET /api/ml/training-status`, `GET /api/ml/health` — ML service status
- `GET /api/config` — All live config parameters with current values, sections, types, min/max, versions
- `POST /api/config` — Update config parameters (validates type + min/max, increments version, writes history, sends NOTIFY config_reload)
- `GET /api/config/history` — Change history for config_live (key, old/new values, timestamps)
- **Mobile API**: `/api/mobile/overview`, `/api/mobile/positions`, `/api/mobile/analytics`, `/api/mobile/daily-pnl`, `/api/mobile/equity-curve`

### Auth
- `GET/POST /login` — Session cookie auth (30-day expiry)
- `GET /logout` — Clear session
- HMAC-SHA256 token hashing, Bearer token + query param support
- Optional `DASHBOARD_TOKEN` and `API_SECRET` for access control

## Key architecture decisions

- **Route modules** — app.py is a slim 180-line orchestrator; routes split into pages, api, mobile, audit, ml_proxy via `APIRouter`
- **asyncio.gather** — all page routes fetch independent DB data in parallel (50-80% latency reduction)
- **TTL cache** — `get_all_closed_trades()` cached for 30s (avoids repeated full-table scans on page loads)
- **Jinja2 templates** with base template inheritance + caching enabled — CSS/nav/sort JS defined once in `base.html`
- **Helper functions** (`pc`, `wr_color`) registered as Jinja2 globals, no duplication
- **Pagination** uses SQL `OFFSET/LIMIT` (not Python slicing)
- **Metrics computation** (Sharpe, drawdown, streaks) is pure Python in `utils/metrics.py`, separated from DB layer
- All tables have sortable column headers (client-side JS)

## Environment variables

- `DATABASE_URL` — PostgreSQL connection string (required)
- `BANKROLL` — Starting bankroll for ROI calculation (default: 1000)
- `DASHBOARD_TOKEN` — Optional password for web dashboard auth
- `API_SECRET` — Optional secret for expensive operations
- `ML_API_URL` — Optional ML service URL for health checks and training proxy
- `MIN_EV`, `MIN_KL`, `MAX_KELLY_FRAC`, `TAKE_PROFIT_PCT`, `STOP_LOSS_PCT` — Trading parameters displayed in config comparison
- `PORT` — Uvicorn listen port (default: 3000)

## Running locally

```sh
pip install -r requirements.txt
# Requires DATABASE_URL env var pointing to the shared quant-engine Postgres
python app.py
```
