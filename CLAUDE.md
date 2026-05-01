# Quant Dashboard

FastAPI dashboard for monitoring **quant-micro** (the resolution-harvester bot). Connects to the shared PostgreSQL database. Writes to `config_live` + `config_live_history` (live config editing) and `micro_theme_stats` (theme block/unblock).

The dashboard previously also served the `quant-engine` and `quant-ml` services. Both have been removed from the deployment — engine pages, ML proxy, and engine queries are deleted from this codebase. Only micro is shown.

## Stack

- **Python** (FastAPI + uvicorn + Jinja2 templates)
- **asyncpg** for Postgres
- **Chart.js** for frontend charts
- Deployed via `Procfile` (`web: python app.py`)

## Project structure

```
app.py                — Slim orchestrator: startup, auth, middleware (~170 lines)
routes/
  deps.py             — Shared dependencies: db, config, templates, helpers
  pages.py            — HTML pages: /, /micro, /config (~180 lines)
  api.py              — Core API: micro theme block, live config CRUD (~85 lines)
  mobile.py           — Mobile API: /api/mobile/micro/* (~120 lines)
  audit.py            — Micro audit report: /api/micro-audit (~610 lines)
utils/
  db.py               — Database class: micro queries + config_live read/write (~300 lines)
  helpers.py          — Shared helpers: pc(), wr_color(), to_json()
  metrics.py          — Pure Python metric computation: Sharpe, drawdown, streaks
templates/
  base.html           — Shared layout: CSS, nav (Micro/Config), footer
  micro.html          — Micro bot page
  config.html         — Live config editor page
  login.html          — Login form
requirements.txt
Procfile
```

## Pages

### `/` → redirects to `/micro`

### Micro (`/micro`)
- Stats: bankroll (from `config_live.BANKROLL`), P&L, win rate, open positions, staked capital
- P&L pace: avg/day, 7d/30d sums, YTD sum (next to Daily P&L chart)
- Best/worst trade cards
- By Close Reason table | By Side pie chart
- By Theme + Theme Calibration merged table with block/unblock buttons
- Config A/B comparison table (by `config_tag`)
- Daily P&L bar chart (daily / weekly / monthly toggle)
- Equity curve

### Config (`/config`)
- Live config editor for all 25 micro parameters
- Parameters grouped by section: signals, risk, sizing, capacity, timing, filters, sim, general
- BANKROLL editable (read from `config_live`, not env)
- Per-parameter validation (type: float/int/bool/str, min/max bounds)
- Version tracking per key (incremented on each change)
- Change history log with old/new values and timestamps
- Writes to `config_live` + sends `NOTIFY config_reload` for instant pickup by quant-micro

## API

- `POST /api/commands/micro-theme-block` — Block/unblock a theme (writes to `micro_theme_stats`)
- `GET /api/config` — All live config parameters (micro only)
- `POST /api/config` — Update config parameters (validates type + min/max, increments version, writes history, sends NOTIFY config_reload). Single `{service, key, value}` or batch `{updates: [...]}`. Only `service='micro'` accepted.
- `GET /api/config/history` — Change history (micro only)
- `GET /api/micro-audit` — Comprehensive text audit report (Health, Performance, Risk, Diagnostics, Efficiency, all closed positions). Includes `MAX_LOSS_BLOCKED` REST-lag diagnostic from `micro_price_history`.

### Mobile API (`/api/mobile/micro/*`)
- `/overview` — bankroll, P&L, WR, open positions summary
- `/positions?status=open|closed&page=&limit=` — paginated positions
- `/daily-pnl?days=30` — daily P&L for chart
- `/themes` — themes with stats and blocked status
- `POST /theme-block` — block/unblock theme

## Auth
- `GET/POST /login` — Session cookie auth (30-day expiry)
- `GET /logout` — Clear session
- HMAC-SHA256 token hashing, Bearer token + query param support
- Optional `DASHBOARD_TOKEN` and `API_SECRET` for access control

## Key architecture decisions

- **Route modules** — app.py is a slim orchestrator; routes split into pages, api, mobile, audit via `APIRouter`
- **asyncio.gather** — all page routes fetch independent DB data in parallel
- **Jinja2 templates** with base template inheritance + caching enabled
- **Helper functions** (`pc`, `wr_color`) registered as Jinja2 globals
- **Pagination** uses SQL `OFFSET/LIMIT` (not Python slicing)
- **Metrics computation** (Sharpe, drawdown, streaks) is pure Python in `utils/metrics.py`
- **No write to engine tables** — schema for `config_live` + `micro_*` is owned by quant-micro itself; dashboard is a read-mostly client (writes only to `config_live`/`micro_theme_stats`)

## Environment variables

- `DATABASE_URL` — PostgreSQL connection string (required)
- `BANKROLL` — Starting bankroll fallback (default: 1000). Real value comes from `config_live.BANKROLL`.
- `DASHBOARD_TOKEN` — Optional password for web dashboard auth
- `API_SECRET` — Optional secret for expensive operations
- `PORT` — Uvicorn listen port (default: 3000)

## Running locally

```sh
pip install -r requirements.txt
# Requires DATABASE_URL env var pointing to the shared Postgres
python app.py
```
