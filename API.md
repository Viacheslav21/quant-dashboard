# Quant Dashboard — Mobile API

Base URL: `https://<your-railway-url>`

## Auth

All endpoints require authentication. Use the same `DASHBOARD_TOKEN` as the web dashboard.

```
Authorization: Bearer <DASHBOARD_TOKEN>
```

Unauthorized requests return `401`:
```json
{"error": "Unauthorized"}
```

---

## Endpoints

### 1. GET `/api/mobile/overview`

Main screen data: bankroll, PnL, win rate, open positions grouped by theme.

**Request:**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
  https://your-app.railway.app/api/mobile/overview
```

**Response:**
```json
{
  "bankroll": 436.12,
  "start_bankroll": 1000.0,
  "total_pnl": 2.15,
  "roi_pct": -56.4,
  "wins": 289,
  "losses": 266,
  "wr_pct": 52.1,
  "open_count": 52,
  "open_upnl": 12.35,
  "open_staked": 625.80,
  "themes": {
    "iran": {"count": 11, "staked": 107.0, "upnl": 2.35},
    "crypto": {"count": 5, "staked": 75.0, "upnl": 2.96},
    "social": {"count": 6, "staked": 67.0, "upnl": -3.52},
    "oil": {"count": 3, "staked": 59.0, "upnl": 5.91},
    "election": {"count": 6, "staked": 52.0, "upnl": -0.45}
  }
}
```

---

### 2. GET `/api/mobile/positions`

List of positions (open or closed).

**Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `status` | string | `open` | `open` or `closed` |
| `page` | int | `1` | Page number (closed only) |
| `limit` | int | `50` | Items per page (closed only) |

**Request (open):**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
  "https://your-app.railway.app/api/mobile/positions?status=open"
```

**Response (open):**
```json
{
  "positions": [
    {
      "id": "pos_1472026_1711800000",
      "market_id": "1472026",
      "question": "Will Saudi Arabia strike Iran by March 31?",
      "theme": "iran",
      "side": "NO",
      "entry_price": 0.84,
      "current_price": 0.99,
      "stake": 5.0,
      "upnl": 0.82,
      "pnl_pct": 17.9,
      "tp_pct": 0.20,
      "sl_pct": 0.30,
      "ev": 0.156,
      "opened_at": "2026-03-28T14:30:00+00:00"
    }
  ],
  "total": 52
}
```

**Request (closed):**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
  "https://your-app.railway.app/api/mobile/positions?status=closed&page=1&limit=20"
```

**Response (closed):**
```json
{
  "positions": [
    {
      "id": "pos_1455604_1711790000",
      "question": "Will Trump talk to Xi Jinping in March?",
      "theme": "china",
      "side": "YES",
      "entry_price": 0.78,
      "exit_price": 0.96,
      "stake": 1.98,
      "pnl": 0.46,
      "result": "WIN",
      "close_reason": "TAKE_PROFIT",
      "opened_at": "2026-03-30T10:15:00+00:00",
      "closed_at": "2026-03-30T10:20:00+00:00"
    }
  ],
  "total": 555,
  "page": 1
}
```

---

### 3. GET `/api/mobile/analytics`

Full analytics: themes, sides, config A/B, calibration, DMA weights, CLV, risk metrics.

**Request:**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
  https://your-app.railway.app/api/mobile/analytics
```

**Response:**
```json
{
  "by_theme": [
    {"theme": "crypto", "wins": 94, "total": 153, "wr": 61.4, "avg_pnl": 0.25, "total_pnl": 38.25},
    {"theme": "oil", "wins": 20, "total": 54, "wr": 37.0, "avg_pnl": -0.34, "total_pnl": -18.36}
  ],
  "by_side": [
    {"side": "YES", "wins": 76, "total": 157, "wr": 48.4, "avg_pnl": -0.06},
    {"side": "NO", "wins": 213, "total": 398, "wr": 53.5, "avg_pnl": 0.03}
  ],
  "by_config": [
    {"config_tag": "v12", "wins": 11, "total": 17, "wr": 64.7, "total_pnl": -8.0, "avg_pnl": -0.47, "avg_stake": 10.0}
  ],
  "daily_pnl": [
    {"date": "2026-03-31", "pnl": -10.13, "trades": 10, "wr": 40.0},
    {"date": "2026-03-30", "pnl": 23.68, "trades": 43, "wr": 72.1}
  ],
  "calibration": [
    {"bucket": "0-30%", "count": 58, "predicted": 2.8, "actual": 3.4},
    {"bucket": "70-100%", "count": 16, "predicted": 97.1, "actual": 100.0}
  ],
  "ev_predicted": 0.181,
  "ev_actual": -0.020,
  "clv": {
    "1h": 0.54,
    "4h": 0.61,
    "24h": 0.66,
    "close": 0.48,
    "positive_pct": 81.2
  },
  "dma_weights": {
    "book": 1.75,
    "volume": 1.69,
    "crowd": 1.33,
    "long_momentum": 1.07,
    "contrarian": 0.80,
    "momentum": 0.57,
    "arb": 0.51,
    "history": 0.30
  },
  "sharpe": -2.06,
  "max_drawdown_pct": 10.4,
  "streaks": {"current_win": 0, "current_loss": 1, "max_win": 19, "max_loss": 14}
}
```

---

### 4. GET `/api/mobile/daily-pnl`

Daily PnL for chart rendering.

**Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `days` | int | `30` | Number of days |

**Request:**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
  "https://your-app.railway.app/api/mobile/daily-pnl?days=14"
```

**Response:**
```json
{
  "daily": [
    {"date": "2026-03-31", "pnl": -10.13, "trades": 10, "wr": 40.0},
    {"date": "2026-03-30", "pnl": 23.68, "trades": 43, "wr": 72.1},
    {"date": "2026-03-29", "pnl": 9.69, "trades": 40, "wr": 72.5}
  ]
}
```

---

### 5. GET `/api/mobile/equity-curve`

Equity curve data points for chart.

**Request:**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
  https://your-app.railway.app/api/mobile/equity-curve
```

**Response:**
```json
{
  "equity": [
    {"date": "2026-03-19", "equity": 1000.0},
    {"date": "2026-03-20", "equity": 1028.08},
    {"date": "2026-03-21", "equity": 1032.12},
    {"date": "2026-03-30", "equity": 436.12}
  ]
}
```

---

### 6. POST `/api/commands/close` (existing)

Manually close a position from the app.

**Request:**
```bash
curl -X POST -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"position_id": "pos_1472026_1711800000"}' \
  https://your-app.railway.app/api/commands/close
```

**Response:**
```json
{"ok": true, "command_id": 42}
```

---

### 7. GET `/api/diagnostics` (existing)

WR by EV range, lifetime, stake size, TP/SL combos.

**Request:**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
  https://your-app.railway.app/api/diagnostics
```

---

### 8. GET `/api/system-audit` (existing)

Full system audit (same as 1.log). Returns plain text.

**Request:**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
  https://your-app.railway.app/api/system-audit
```

---

## Error Handling

All endpoints return `500` on server errors:
```json
{"error": "error description"}
```

## Notes

- All monetary values are in USD
- Prices are in range 0.0–1.0 (multiply by 100 for cents)
- Dates are ISO 8601 with timezone (`2026-03-30T14:30:00+00:00`)
- `wr_pct` is win rate as percentage (52.1 = 52.1%)
- `pnl_pct` is PnL as percentage relative to entry price
- `tp_pct` / `sl_pct` are take-profit/stop-loss thresholds (0.20 = 20%)
- `ev` is expected value as decimal (0.156 = 15.6%)
