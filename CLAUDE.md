# Quant Dashboard

Read-only FastAPI dashboard for monitoring a prediction-market trading engine ("quant-engine"). Connects to a shared PostgreSQL database.

## Stack

- **Python** (FastAPI + uvicorn)
- **asyncpg** for Postgres
- **Anthropic SDK** (available but dashboard is primarily a viewer)
- Deployed via `Procfile` (`python app.py`)

## Project structure

```
app.py           — Main FastAPI app: routes, HTML rendering, config
utils/db.py      — Database class (asyncpg pool, read-only queries)
requirements.txt — Python dependencies
Procfile         — Process runner entry point
```

## Key concepts

- **Positions**: trades with open/closed status, PnL, side (YES/NO), stake amounts
- **Signals**: model-generated trading signals with EV, Kelly sizing, source
- **Markets**: prediction markets with yes_price, themes, active status
- **Stats**: aggregated bankroll, PnL, win/loss counts (single row, id=1)
- **Config history**: tracks parameter changes over time

## Running locally

```sh
pip install -r requirements.txt
# Requires DATABASE_URL env var pointing to the shared quant-engine Postgres
python app.py
```

## Environment variables

- `DATABASE_URL` — PostgreSQL connection string (required)
- `ANTHROPIC_API_KEY` — Anthropic API key
- `BANKROLL` — Starting bankroll (default: 1000)
- `MIN_EV`, `MIN_KL`, `MAX_KELLY_FRAC`, `TAKE_PROFIT_PCT`, `STOP_LOSS_PCT` — Trading parameters
