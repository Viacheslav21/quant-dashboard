# Mobile API Reference

Base URL: `https://<your-dashboard-url>/api/mobile`

## Authentication

All endpoints require authentication if `DASHBOARD_TOKEN` is set.

**Option 1 — Bearer token (recommended for mobile):**
```
Authorization: Bearer <DASHBOARD_TOKEN>
```

**Option 2 — Query parameter:**
```
GET /api/mobile/overview?token=<DASHBOARD_TOKEN>
```

**Option 3 — Session cookie:**
Login via `POST /login` with `token=<DASHBOARD_TOKEN>`, use returned `session_token` cookie.

If `DASHBOARD_TOKEN` is not set, all endpoints are open (no auth required).

**Error response (401):**
```json
{"error": "Unauthorized"}
```

---

## Engine Endpoints

### GET /api/mobile/overview

Main dashboard: bankroll, PnL, win rate, open positions.

**Response:**
```json
{
  "bankroll": 847.32,
  "start_bankroll": 1000,
  "total_pnl": 52.15,
  "roi_pct": -15.3,
  "wins": 142,
  "losses": 38,
  "wr_pct": 78.9,
  "open_count": 23,
  "open_upnl": -12.45,
  "open_staked": 285.00,
  "themes": {
    "crypto": {"count": 5, "staked": 62.50, "upnl": -3.20},
    "war": {"count": 3, "staked": 37.50, "upnl": 1.15},
    "other": {"count": 8, "staked": 100.00, "upnl": -5.40}
  }
}
```

---

### GET /api/mobile/positions

Open or closed positions list.

**Query params:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `status` | string | `open` | `open` or `closed` |
| `page` | int | 1 | Page number (closed only) |
| `limit` | int | 50 | Per page (closed only) |

**Response (open):**
```json
{
  "positions": [
    {
      "id": "pos_abc123",
      "market_id": "1817387",
      "question": "Will the price of Bitcoin be above $72,000 on April 8?",
      "theme": "crypto",
      "side": "NO",
      "entry_price": 0.885,
      "current_price": 0.365,
      "stake": 12.50,
      "upnl": -7.32,
      "pnl_pct": -58.8,
      "tp_pct": 0.15,
      "sl_pct": 0.25,
      "ev": 0.14,
      "opened_at": "2026-04-05T14:30:00+00:00"
    }
  ],
  "total": 23
}
```

**Response (closed):**
```json
{
  "positions": [
    {
      "id": "pos_def456",
      "question": "Will Ethereum reach $2,800 in April?",
      "theme": "crypto",
      "side": "NO",
      "entry_price": 0.92,
      "exit_price": 0.99,
      "stake": 10.00,
      "pnl": 0.76,
      "result": "WIN",
      "close_reason": "resolved",
      "opened_at": "2026-04-03T10:00:00+00:00",
      "closed_at": "2026-04-07T18:00:00+00:00"
    }
  ],
  "total": 180,
  "page": 1
}
```

---

### GET /api/mobile/analytics

Full analytics: breakdowns, calibration, DMA weights, CLV.

**Response:**
```json
{
  "by_theme": [
    {"theme": "crypto", "total": 45, "wins": 32, "avg_pnl": 0.15, "total_pnl": 6.75}
  ],
  "by_side": [
    {"side": "YES", "total": 80, "wins": 62, "avg_pnl": 0.12}
  ],
  "by_config": [
    {"config_tag": "v7", "total": 100, "wins": 78, "avg_pnl": 0.10}
  ],
  "daily_pnl": [
    {"day": "2026-04-07", "pnl": 3.45, "trades": 12, "wr": 83.3}
  ],
  "calibration": [
    {"agent": "math", "brier": 0.013, "bias": 0.001, "factor": 1.0}
  ],
  "ev_predicted": 0.145,
  "ev_actual": 0.128,
  "clv": [
    {"market_id": "123", "clv": 0.03, "question": "..."}
  ],
  "dma_weights": [
    {"source": "volume", "weight": 2.0, "hits": 105, "misses": 38}
  ],
  "sharpe": 1.85,
  "max_drawdown_pct": 12.3,
  "streaks": {"current_win": 5, "current_loss": 0, "max_win": 18, "max_loss": 3}
}
```

---

### GET /api/mobile/daily-pnl

Daily PnL for chart rendering.

**Query params:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `days` | int | 30 | Number of days |

**Response:**
```json
{
  "daily": [
    {"day": "2026-04-01", "pnl": 2.15, "trades": 8, "wr": 87.5},
    {"day": "2026-04-02", "pnl": -1.30, "trades": 5, "wr": 60.0}
  ]
}
```

---

### GET /api/mobile/equity-curve

Equity curve data points for chart.

**Response:**
```json
{
  "equity": [
    {"date": "2026-04-01", "equity": 1002.15},
    {"date": "2026-04-02", "equity": 1000.85}
  ]
}
```

---

## Micro Endpoints

### GET /api/mobile/micro/overview

Micro bot dashboard: bankroll, PnL, win rate, open positions.

**Response:**
```json
{
  "bankroll": 537.65,
  "total_pnl": 14.46,
  "wins": 91,
  "losses": 4,
  "wr_pct": 95.8,
  "open_count": 36,
  "open_upnl": -2.18,
  "open_staked": 456.81,
  "themes": {
    "crypto": {"count": 5, "staked": 100.00, "upnl": -0.06},
    "musk": {"count": 4, "staked": 65.00, "upnl": 0.36},
    "sports": {"count": 5, "staked": 64.53, "upnl": -0.40},
    "politics": {"count": 3, "staked": 34.08, "upnl": -0.51}
  }
}
```

---

### GET /api/mobile/micro/positions

Micro open or closed positions.

**Query params:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `status` | string | `open` | `open` or `closed` |
| `page` | int | 1 | Page number (closed only) |
| `limit` | int | 50 | Per page (closed only) |

**Response (open):**
```json
{
  "positions": [
    {
      "id": "mic_1817387_1712345678",
      "market_id": "1817387",
      "question": "Will Bitcoin dip to $64,000 April 6-12?",
      "theme": "crypto",
      "side": "NO",
      "entry_price": 0.96,
      "current_price": 0.94,
      "stake": 20.00,
      "upnl": -0.42,
      "pnl_pct": -2.1,
      "end_date": "2026-04-12T23:59:00+00:00",
      "opened_at": "2026-04-06T14:30:00+00:00"
    }
  ],
  "total": 36
}
```

**Response (closed):**
```json
{
  "positions": [
    {
      "id": "mic_abc123_1712000000",
      "question": "Will Bitcoin dip to $60,000 April 6-12?",
      "theme": "crypto",
      "side": "NO",
      "entry_price": 0.96,
      "current_price": 0.99,
      "stake": 20.00,
      "pnl": 0.52,
      "result": "WIN",
      "close_reason": "resolved",
      "opened_at": "2026-04-06T10:00:00+00:00",
      "closed_at": "2026-04-08T09:25:43+00:00"
    }
  ],
  "total": 95,
  "page": 1
}
```

---

### GET /api/mobile/micro/daily-pnl

Micro daily PnL for chart.

**Query params:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `days` | int | 30 | Number of days |

**Response:**
```json
{
  "daily": [
    {"day": "2026-04-04", "pnl": 0.99, "trades": 2, "wr": 100.0},
    {"day": "2026-04-05", "pnl": 3.34, "trades": 10, "wr": 90.0},
    {"day": "2026-04-06", "pnl": 2.61, "trades": 20, "wr": 100.0},
    {"day": "2026-04-07", "pnl": -0.66, "trades": 42, "wr": 92.9},
    {"day": "2026-04-08", "pnl": 7.67, "trades": 20, "wr": 100.0}
  ]
}
```

---

## Error Handling

All endpoints return the same error format:

```json
{"error": "description of error"}
```

HTTP status codes:
- `200` — success
- `401` — unauthorized (missing/invalid token)
- `500` — server error

---

## Rate Limits

No explicit rate limiting. Dashboard connects to PostgreSQL via connection pool (max 15 connections). Heavy polling (>1 req/sec) is discouraged — data updates every 30 seconds (WS position price writes are throttled).

Recommended polling intervals:
- Overview: every 30-60 seconds
- Positions: every 30-60 seconds
- Daily PnL: every 5 minutes
- Analytics: every 5 minutes
- Equity curve: every 5 minutes
