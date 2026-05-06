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
GET /api/mobile/micro/overview?token=<DASHBOARD_TOKEN>
```

**Option 3 — Session cookie:**
Login via `POST /login` with `token=<DASHBOARD_TOKEN>`, use returned `session_token` cookie.

If `DASHBOARD_TOKEN` is not set, all endpoints are open (no auth required).

**Error response (401):**
```json
{"error": "Unauthorized"}
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

Micro open or closed positions. Returns a **uniform position object** regardless
of `status` — fields not relevant for the current status are returned as `null`,
so clients can rely on a single schema.

**Query params:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `status` | string | `open` | `open` or `closed` |
| `page` | int | 1 | Page number (closed only) |
| `limit` | int | 50 | Per page (closed only) |

**Position schema (always the same shape):**
| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Position id |
| `market_id` | string | Polymarket market id |
| `question` | string | Market question |
| `theme` | string | Theme bucket |
| `side` | string | `YES` or `NO` |
| `entry_price` | float | Entry price |
| `current_price` | float | Latest known price (= entry if unknown) |
| `stake` | float | Stake amount in USD |
| `pnl` | float | Unrealized PnL when `status=open`, realized PnL when `status=closed` |
| `pnl_pct` | float | Percent change between `entry_price` and `current_price` |
| `status` | string | `open` or `closed` |
| `result` | string\|null | `WIN`/`LOSS` for closed, `null` for open |
| `close_reason` | string\|null | Reason for the close (see values below). `null` for open. |
| `end_date` | string\|null | Market end date for open, `null` for closed |
| `opened_at` | string | Position open timestamp |
| `closed_at` | string\|null | Close timestamp for closed, `null` for open |

**`close_reason` values** (only set when `status=closed`):
| Value | Result | Meaning |
|-------|--------|---------|
| `resolved` | `WIN` | Market resolved in our favor — bid ≥ 99.5¢ on WS or ≥ 99¢ on REST. Payout $1.00, no exit fees. |
| `resolved_loss` | `LOSS` | Market resolved against us — bid ≤ 1¢ confirmed by REST. Stake fully lost. |
| `take_profit` | `WIN` | Early take-profit — bid ≥ `TAKE_PROFIT_PRICE` (default 98¢) with > `TAKE_PROFIT_MIN_DAYS` (default 1d) remaining. Exit fees apply. |
| `max_loss` | `LOSS` | Hard loss cap (`MAX_LOSS_PER_POS`, default $3) hit. REST-verified via CLOB book (with bypass after N consecutive blocks). Real PnL recorded at exit price. |
| `rapid_drop` | `LOSS` | Bid dropped > `RAPID_DROP_PCT` (default 7¢) from entry. REST + 24h-volume confirmed. |
| `expired` | `WIN` or `LOSS` | Position auto-closed 72h past `end_date` because market never produced a clean resolution signal. |

> Both `max_loss` and `rapid_drop` add the market+side to the SL blacklist — quant-micro
> will not re-enter that market+side again.

**Response:**
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
      "pnl": -0.42,
      "pnl_pct": -2.1,
      "status": "open",
      "result": null,
      "close_reason": null,
      "end_date": "2026-04-12T23:59:00+00:00",
      "opened_at": "2026-04-06T14:30:00+00:00",
      "closed_at": null
    }
  ],
  "total": 36
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

### GET /api/mobile/micro/themes

Per-theme stats with blocked flag.

**Response:**
```json
{
  "themes": [
    {"theme": "crypto", "trades": 28, "wins": 27, "wr": 96.4, "pnl": 8.42, "blocked": false},
    {"theme": "sports", "trades": 15, "wins": 14, "wr": 93.3, "pnl": 3.10, "blocked": false},
    {"theme": "politics", "trades": 6, "wins": 2, "wr": 33.3, "pnl": -2.40, "blocked": true}
  ]
}
```

> Field shape mirrors `db.get_micro_themes()`. `blocked` reflects either a manual
> block from the dashboard or a Bayesian auto-block (WR<40% after ≥5 trades).

---

### POST /api/mobile/micro/theme-block

Block or unblock a theme. Blocked themes are skipped by the entry filter in
quant-micro until unblocked.

**Body:**
```json
{
  "theme": "politics",
  "blocked": true
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `theme` | string | yes | Theme name (e.g. `crypto`, `sports`, `politics`, `musk`, `other`) |
| `blocked` | bool | no (default `true`) | `true` to block, `false` to unblock |

**Response (200):**
```json
{
  "ok": true,
  "theme": "politics",
  "blocked": true
}
```

**Errors:**
- `400` — `{"error": "theme required"}` if `theme` is missing
- `500` — `{"error": "<message>"}` on DB failure

**Example (curl):**
```sh
curl -X POST https://<your-dashboard-url>/api/mobile/micro/theme-block \
  -H "Authorization: Bearer $DASHBOARD_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"theme":"politics","blocked":true}'
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

No explicit rate limiting. Dashboard connects to PostgreSQL via connection pool (max 15 connections). Heavy polling (>1 req/sec) is discouraged — data updates every 30 seconds.

Recommended polling intervals:
- Overview: every 30-60 seconds
- Positions: every 30-60 seconds
- Daily PnL: every 5 minutes
