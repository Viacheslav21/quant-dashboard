import os
import logging
from decimal import Decimal
from datetime import datetime, date
import asyncpg

log = logging.getLogger("db")


def _clean(row) -> dict:
    """Convert asyncpg Record to dict with JSON-safe types.
    Prevents 'unhashable type: dict' from JSONB columns and Decimal issues."""
    if row is None:
        return {}
    d = {}
    for k, v in dict(row).items():
        if isinstance(v, Decimal):
            d[k] = float(v)
        elif isinstance(v, (dict, list)):
            d[k] = v  # JSONB columns — keep as-is (JSON-serializable)
        else:
            d[k] = v
    return d


def _clean_list(rows) -> list:
    return [_clean(r) for r in rows]


class Database:
    """Read-only database layer for dashboard. Connects to shared quant-engine PostgreSQL."""

    def __init__(self, url: str):
        self.url = url
        self.pool = None

    async def init(self):
        self.pool = await asyncpg.create_pool(self.url, min_size=2, max_size=10, command_timeout=30)
        # Ensure trader_commands table exists (dashboard writes to it)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trader_commands (
                    id BIGSERIAL PRIMARY KEY,
                    command TEXT NOT NULL,
                    position_id TEXT,
                    params JSONB DEFAULT '{}',
                    status TEXT DEFAULT 'pending',
                    result JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    executed_at TIMESTAMPTZ
                );
                CREATE INDEX IF NOT EXISTS idx_trader_commands_status
                    ON trader_commands(status) WHERE status='pending';
                CREATE TABLE IF NOT EXISTS config_live (
                    id BIGSERIAL PRIMARY KEY,
                    service TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    value_type TEXT NOT NULL DEFAULT 'str',
                    description TEXT DEFAULT '',
                    min_val REAL,
                    max_val REAL,
                    section TEXT DEFAULT 'general',
                    version INTEGER DEFAULT 1,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(service, key)
                );
                CREATE TABLE IF NOT EXISTS config_live_history (
                    id BIGSERIAL PRIMARY KEY,
                    service TEXT NOT NULL,
                    key TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    changed_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
        log.info("[DB] Dashboard connected to shared database")

    async def get_stats(self) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT bankroll, total_pnl, wins, losses, avg_ev, avg_kelly FROM stats WHERE id=1")
            if row:
                r = _clean(row)
                return {
                    "bankroll": float(r.get("bankroll") or 0),
                    "total_pnl": float(r.get("total_pnl") or 0),
                    "wins": int(r.get("wins") or 0),
                    "losses": int(r.get("losses") or 0),
                    "avg_ev": float(r.get("avg_ev") or 0),
                    "avg_kelly": float(r.get("avg_kelly") or 0),
                }
            return {"bankroll": float(os.getenv("BANKROLL", "1000")), "total_pnl": 0.0, "wins": 0, "losses": 0, "avg_ev": 0.0, "avg_kelly": 0.0}

    async def get_open_positions(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, market_id, question, side, side_price, current_price,
                       unrealized_pnl, ev, kl, stake_amt, url, opened_at, theme, config_tag
                FROM positions WHERE status='open' ORDER BY opened_at DESC
            """)
            return _clean_list(rows)

    async def get_closed_positions(self, limit: int = 100, offset: int = 0, date_from=None, date_to=None) -> list:
        async with self.pool.acquire() as conn:
            query = """SELECT id, market_id, question, side, side_price, current_price, outcome,
                              pnl, result, ev, kl, stake_amt, url, opened_at, closed_at, theme, config_tag
                       FROM positions WHERE status='closed'"""
            params = []
            idx = 1
            if date_from:
                query += f" AND closed_at >= ${idx}::timestamptz"
                params.append(date_from)
                idx += 1
            if date_to:
                query += f" AND closed_at <= ${idx}::timestamptz"
                params.append(date_to)
                idx += 1
            query += f" ORDER BY closed_at DESC LIMIT ${idx} OFFSET ${idx + 1}"
            params.extend([limit, offset])
            rows = await conn.fetch(query, *params)
            return _clean_list(rows)

    async def get_closed_positions_count(self, date_from=None, date_to=None) -> int:
        async with self.pool.acquire() as conn:
            query = "SELECT COUNT(*) FROM positions WHERE status='closed'"
            params = []
            idx = 1
            if date_from:
                query += f" AND closed_at >= ${idx}::timestamptz"
                params.append(date_from)
                idx += 1
            if date_to:
                query += f" AND closed_at <= ${idx}::timestamptz"
                params.append(date_to)
                idx += 1
            val = await conn.fetchval(query, *params)
            return int(val or 0)

    async def get_recent_signals(self, limit: int = 20) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, market_id, question, side, side_price, p_market, p_final,
                       p_claude as p_ml, ev, kl, kelly, source, executed, created_at
                FROM signals ORDER BY created_at DESC LIMIT $1
            """, limit)
            return _clean_list(rows)

    async def get_cumulative_pnl(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT closed_at, pnl,
                    SUM(pnl) OVER (ORDER BY closed_at) as cumulative
                FROM positions
                WHERE status='closed' AND closed_at IS NOT NULL
                ORDER BY closed_at ASC
            """)
            return [{"t": r["closed_at"].isoformat(), "pnl": float(r["pnl"]), "cum": float(r["cumulative"])} for r in rows]

    async def get_signal_outcomes(self, limit: int = 50) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT s.id, s.question, s.side, s.side_price, s.p_market, s.p_final,
                    s.ev, s.kelly, s.source, s.executed, s.created_at,
                    COALESCE(m.yes_price, CASE WHEN p.result='WIN' THEN
                        CASE WHEN s.side='YES' THEN 0.95 ELSE 0.05 END
                        ELSE CASE WHEN s.side='YES' THEN 0.05 ELSE 0.95 END
                    END) as current_price,
                    COALESCE(m.is_active, FALSE) as is_active,
                    CASE WHEN m.id IS NOT NULL THEN
                        CASE WHEN s.side = 'YES' THEN m.yes_price - s.side_price
                             ELSE (1 - m.yes_price) - s.side_price END
                    WHEN p.id IS NOT NULL THEN
                        CASE WHEN p.result = 'WIN' THEN ABS(s.side_price)
                             ELSE -s.side_price END
                    END as price_move
                FROM signals s
                LEFT JOIN markets m ON s.market_id = m.id
                LEFT JOIN positions p ON s.id = p.signal_id
                ORDER BY s.created_at DESC
                LIMIT $1
            """, limit)
            return _clean_list(rows)

    async def get_all_market_metrics(self, limit: int = 50) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT mm.market_id, mm.volatility, mm.momentum, mm.vol_ratio, mm.updated_at,
                       m.question, m.yes_price, m.theme
                FROM market_metrics mm
                JOIN markets m ON mm.market_id = m.id
                WHERE m.is_active = TRUE
                ORDER BY mm.updated_at DESC
                LIMIT $1
            """, limit)
            return _clean_list(rows)

    async def get_config_history(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT tag, params, created_at FROM config_history ORDER BY created_at DESC")
            # Keep params (JSONB) as-is for config comparison
            return [dict(r) for r in rows]

    async def get_analytics(self) -> dict:
        async with self.pool.acquire() as conn:
            by_theme = await conn.fetch("""
                SELECT theme, COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                    ROUND(SUM(pnl)::numeric, 2) as total_pnl
                FROM positions WHERE status='closed' AND theme IS NOT NULL
                GROUP BY theme ORDER BY total DESC
            """)
            by_side = await conn.fetch("""
                SELECT side, COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl
                FROM positions WHERE status='closed'
                GROUP BY side
            """)
            by_reason = await conn.fetch("""
                SELECT
                    CASE
                        WHEN outcome LIKE '%@%' AND pnl > 0 THEN 'TAKE_PROFIT'
                        WHEN outcome LIKE '%@%' AND pnl <= 0 THEN 'STOP_LOSS'
                        ELSE 'RESOLVED'
                    END as reason,
                    COUNT(*) as total,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl
                FROM positions WHERE status='closed'
                GROUP BY reason ORDER BY total DESC
            """)
            by_config = await conn.fetch("""
                SELECT COALESCE(config_tag, 'v0') as config_tag, COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(SUM(pnl)::numeric, 2) as total_pnl,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                    ROUND(AVG(ev)::numeric, 4) as avg_ev,
                    ROUND(AVG(stake_amt)::numeric, 2) as avg_stake
                FROM positions WHERE status='closed'
                GROUP BY COALESCE(config_tag, 'v0') ORDER BY config_tag
            """)
            calibration = await conn.fetch("""
                SELECT
                    CASE
                        WHEN p_final < 0.3 THEN '0-30%'
                        WHEN p_final < 0.5 THEN '30-50%'
                        WHEN p_final < 0.7 THEN '50-70%'
                        ELSE '70-100%'
                    END as bucket,
                    COUNT(*) as total,
                    ROUND(AVG(p_final)::numeric, 3) as avg_predicted,
                    ROUND(AVG(CASE
                        WHEN (side='YES' AND result='WIN') OR (side='NO' AND result='LOSS')
                        THEN 1.0 ELSE 0.0
                    END)::numeric, 3) as actual_wr
                FROM positions WHERE status='closed' AND outcome IN ('YES', 'NO')
                GROUP BY bucket ORDER BY bucket
            """)
            avg_lifetime = await conn.fetchrow("""
                SELECT ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600)::numeric, 1) as avg_hours
                FROM positions WHERE status='closed' AND closed_at IS NOT NULL
            """)
            daily_pnl = await conn.fetch("""
                SELECT DATE(closed_at) as day,
                    ROUND(SUM(pnl)::numeric, 2) as pnl,
                    COUNT(*) as trades,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins
                FROM positions WHERE status='closed' AND closed_at IS NOT NULL
                GROUP BY day ORDER BY day ASC
            """)
            ev_accuracy = await conn.fetchrow("""
                SELECT
                    ROUND(AVG(ev)::numeric, 4) as avg_predicted_ev,
                    ROUND(AVG(pnl / NULLIF(stake_amt, 0))::numeric, 4) as avg_actual_return
                FROM positions WHERE status='closed' AND stake_amt > 0
            """)

        return {
            "by_config": _clean_list(by_config),
            "by_theme": _clean_list(by_theme),
            "by_side": _clean_list(by_side),
            "by_reason": _clean_list(by_reason),
            "calibration": _clean_list(calibration),
            "avg_lifetime_hours": float(avg_lifetime["avg_hours"] or 0) if avg_lifetime else 0.0,
            "daily_pnl": _clean_list(daily_pnl),
            "ev_predicted": float(ev_accuracy["avg_predicted_ev"] or 0) if ev_accuracy else 0.0,
            "ev_actual": float(ev_accuracy["avg_actual_return"] or 0) if ev_accuracy else 0.0,
        }

    # ── New metric queries ──

    _trades_cache = None
    _trades_cache_at = 0

    async def get_all_closed_trades(self) -> list:
        """All closed trades ordered chronologically for metric computation.
        Cached for 30 seconds — data only changes when a trade closes."""
        import time
        now = time.time()
        if self._trades_cache is not None and now - self._trades_cache_at < 30:
            return self._trades_cache
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT pnl, stake_amt, result, closed_at, question
                FROM positions WHERE status='closed' AND closed_at IS NOT NULL
                ORDER BY closed_at ASC
            """)
            self._trades_cache = _clean_list(rows)
            self._trades_cache_at = now
            return self._trades_cache

    async def get_best_worst_trades(self) -> dict:
        async with self.pool.acquire() as conn:
            best = await conn.fetchrow(
                "SELECT question, pnl, side, closed_at FROM positions WHERE status='closed' ORDER BY pnl DESC LIMIT 1")
            worst = await conn.fetchrow(
                "SELECT question, pnl, side, closed_at FROM positions WHERE status='closed' ORDER BY pnl ASC LIMIT 1")
            return {
                "best": _clean(best) if best else None,
                "worst": _clean(worst) if worst else None,
            }

    async def get_rolling_performance(self) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    COALESCE(SUM(CASE WHEN closed_at >= NOW() - INTERVAL '7 days' THEN pnl END), 0) as pnl_7d,
                    COALESCE(COUNT(CASE WHEN closed_at >= NOW() - INTERVAL '7 days' THEN 1 END), 0) as trades_7d,
                    COALESCE(SUM(CASE WHEN closed_at >= NOW() - INTERVAL '7 days' AND result='WIN' THEN 1 ELSE 0 END), 0) as wins_7d,
                    COALESCE(SUM(CASE WHEN closed_at >= NOW() - INTERVAL '30 days' THEN pnl END), 0) as pnl_30d,
                    COALESCE(COUNT(CASE WHEN closed_at >= NOW() - INTERVAL '30 days' THEN 1 END), 0) as trades_30d,
                    COALESCE(SUM(CASE WHEN closed_at >= NOW() - INTERVAL '30 days' AND result='WIN' THEN 1 ELSE 0 END), 0) as wins_30d
                FROM positions WHERE status='closed'
            """)
            r = _clean(row) if row else {}
            trades_7d = int(r.get("trades_7d") or 0)
            trades_30d = int(r.get("trades_30d") or 0)
            wins_7d = int(r.get("wins_7d") or 0)
            wins_30d = int(r.get("wins_30d") or 0)
            return {
                "pnl_7d": float(r.get("pnl_7d") or 0),
                "trades_7d": trades_7d,
                "wins_7d": wins_7d,
                "wr_7d": round(wins_7d / trades_7d * 100, 1) if trades_7d > 0 else 0.0,
                "pnl_30d": float(r.get("pnl_30d") or 0),
                "trades_30d": trades_30d,
                "wins_30d": wins_30d,
                "wr_30d": round(wins_30d / trades_30d * 100, 1) if trades_30d > 0 else 0.0,
            }

    async def get_positions_for_export(self, date_from=None, date_to=None) -> list:
        async with self.pool.acquire() as conn:
            query = """SELECT question, side, side_price, outcome, pnl, result, ev, kl,
                              stake_amt, opened_at, closed_at, theme, config_tag
                       FROM positions WHERE status='closed'"""
            params = []
            idx = 1
            if date_from:
                query += f" AND closed_at >= ${idx}::timestamptz"
                params.append(date_from)
                idx += 1
            if date_to:
                query += f" AND closed_at <= ${idx}::timestamptz"
                params.append(date_to)
                idx += 1
            query += " ORDER BY closed_at DESC"
            rows = await conn.fetch(query, *params)
            return _clean_list(rows)

    # ── Win Rate Diagnostics ──

    async def get_wr_diagnostics(self) -> dict:
        """Deep WR diagnostics: close reasons from trade_log, avg win/loss, EV buckets, trailing analysis."""
        async with self.pool.acquire() as conn:
            # 1. Exact close reasons from trade_log (CLOSE_TP, CLOSE_SL, CLOSE_TRAILING_TP, CLOSE_RESOLVED, CLOSE_MANUAL)
            close_reasons = await conn.fetch("""
                SELECT event_type, COUNT(*) as total,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                    ROUND(SUM(pnl)::numeric, 2) as total_pnl,
                    ROUND(AVG(stake_amt)::numeric, 2) as avg_stake,
                    ROUND(AVG(pnl_pct)::numeric, 4) as avg_pnl_pct
                FROM trade_log
                WHERE event_type IN ('CLOSE_TP', 'CLOSE_SL', 'CLOSE_TRAILING_TP', 'CLOSE_RESOLVED', 'CLOSE_MANUAL')
                GROUP BY event_type ORDER BY total DESC
            """)

            # 2. Average win size vs average loss size
            win_loss_size = await conn.fetchrow("""
                SELECT
                    ROUND(AVG(CASE WHEN result='WIN' THEN pnl END)::numeric, 2) as avg_win,
                    ROUND(AVG(CASE WHEN result='LOSS' THEN pnl END)::numeric, 2) as avg_loss,
                    ROUND(AVG(CASE WHEN result='WIN' THEN pnl/NULLIF(stake_amt,0) END)::numeric, 4) as avg_win_pct,
                    ROUND(AVG(CASE WHEN result='LOSS' THEN pnl/NULLIF(stake_amt,0) END)::numeric, 4) as avg_loss_pct,
                    COUNT(CASE WHEN result='WIN' THEN 1 END) as wins,
                    COUNT(CASE WHEN result='LOSS' THEN 1 END) as losses
                FROM positions WHERE status='closed' AND stake_amt > 0
            """)

            # 3. WR by EV bucket at entry
            ev_buckets = await conn.fetch("""
                SELECT
                    CASE
                        WHEN ev < 0.15 THEN '12-15%'
                        WHEN ev < 0.20 THEN '15-20%'
                        WHEN ev < 0.30 THEN '20-30%'
                        WHEN ev < 0.50 THEN '30-50%'
                        ELSE '50%+'
                    END as ev_bucket,
                    COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                    ROUND(SUM(pnl)::numeric, 2) as total_pnl
                FROM positions WHERE status='closed' AND ev IS NOT NULL
                GROUP BY ev_bucket ORDER BY ev_bucket
            """)

            # 4. WR by Kelly bucket at entry
            kelly_buckets = await conn.fetch("""
                SELECT
                    CASE
                        WHEN kelly < 0.02 THEN '1-2%'
                        WHEN kelly < 0.04 THEN '2-4%'
                        WHEN kelly < 0.06 THEN '4-6%'
                        ELSE '6%+'
                    END as kelly_bucket,
                    COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                    ROUND(SUM(pnl)::numeric, 2) as total_pnl
                FROM positions WHERE status='closed' AND kelly IS NOT NULL
                GROUP BY kelly_bucket ORDER BY kelly_bucket
            """)

            # 5. Trailing TP analysis — positions that had high pnl_pct but closed via SL
            missed_tp = await conn.fetchrow("""
                SELECT COUNT(*) as count,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl
                FROM trade_log
                WHERE event_type = 'CLOSE_SL'
            """)

            # 6. WR by position lifetime bucket
            lifetime_wr = await conn.fetch("""
                SELECT
                    CASE
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600 < 1 THEN '<1h'
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600 < 6 THEN '1-6h'
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600 < 24 THEN '6-24h'
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600 < 72 THEN '1-3d'
                        ELSE '3d+'
                    END as lifetime,
                    COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl
                FROM positions WHERE status='closed' AND closed_at IS NOT NULL AND opened_at IS NOT NULL
                GROUP BY lifetime ORDER BY MIN(EXTRACT(EPOCH FROM (closed_at - opened_at)))
            """)

            # 7. WR by stake size bucket
            stake_wr = await conn.fetch("""
                SELECT
                    CASE
                        WHEN stake_amt < 5 THEN '<$5'
                        WHEN stake_amt < 10 THEN '$5-10'
                        WHEN stake_amt < 20 THEN '$10-20'
                        WHEN stake_amt < 50 THEN '$20-50'
                        ELSE '$50+'
                    END as stake_bucket,
                    COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                    ROUND(SUM(pnl)::numeric, 2) as total_pnl
                FROM positions WHERE status='closed'
                GROUP BY stake_bucket ORDER BY MIN(stake_amt)
            """)

            # 8. WR trend — last 7 days daily WR
            daily_wr = await conn.fetch("""
                SELECT DATE(closed_at) as day,
                    COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(SUM(pnl)::numeric, 2) as pnl,
                    ROUND(AVG(pnl/NULLIF(stake_amt,0))::numeric, 4) as avg_return_pct
                FROM positions WHERE status='closed' AND closed_at >= NOW() - INTERVAL '14 days'
                GROUP BY day ORDER BY day DESC
            """)

            # 9. TP/SL settings distribution on closed positions
            tp_sl_dist = await conn.fetch("""
                SELECT
                    ROUND(tp_pct::numeric, 2) as tp,
                    ROUND(sl_pct::numeric, 2) as sl,
                    COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl
                FROM positions WHERE status='closed' AND tp_pct IS NOT NULL AND sl_pct IS NOT NULL
                GROUP BY tp, sl ORDER BY total DESC LIMIT 10
            """)

            # 10. Breakeven WR needed for current avg win/loss
            wl = _clean(win_loss_size) if win_loss_size else {}
            avg_win_pct = abs(float(wl.get("avg_win_pct") or 0))
            avg_loss_pct = abs(float(wl.get("avg_loss_pct") or 0))
            breakeven_wr = round(avg_loss_pct / (avg_win_pct + avg_loss_pct) * 100, 1) if (avg_win_pct + avg_loss_pct) > 0 else 50.0

            return {
                "close_reasons": _clean_list(close_reasons),
                "win_loss_size": _clean(win_loss_size) if win_loss_size else {},
                "breakeven_wr": breakeven_wr,
                "ev_buckets": _clean_list(ev_buckets),
                "kelly_buckets": _clean_list(kelly_buckets),
                "missed_tp": _clean(missed_tp) if missed_tp else {},
                "lifetime_wr": _clean_list(lifetime_wr),
                "stake_wr": _clean_list(stake_wr),
                "daily_wr": _clean_list(daily_wr),
                "tp_sl_dist": _clean_list(tp_sl_dist),
            }

    # ── CLV Analytics ──

    async def get_clv_analytics(self) -> dict:
        """CLV = did price move in our direction after entry? Positive = good entry."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT side, side_price, clv_1h, clv_4h, clv_24h, clv_close,
                       result, theme, config_tag
                FROM positions
                WHERE status = 'closed' AND side_price > 0
                ORDER BY closed_at DESC LIMIT 500
            """)
            if not rows:
                return {"avg_clv_1h": 0, "avg_clv_4h": 0, "avg_clv_24h": 0, "avg_clv_close": 0,
                        "total": 0, "positive_clv_pct": 0, "by_theme": [], "by_tag": []}

            def clv_val(row, col):
                v = row.get(col)
                if v is None:
                    return None
                entry = row["side_price"]
                if row["side"] == "YES":
                    return (v - entry) / entry
                else:
                    return (entry - v) / entry

            clvs = {"1h": [], "4h": [], "24h": [], "close": []}
            by_theme, by_tag = {}, {}
            for r in rows:
                for label, col in [("1h","clv_1h"),("4h","clv_4h"),("24h","clv_24h"),("close","clv_close")]:
                    v = clv_val(r, col)
                    if v is not None:
                        clvs[label].append(v)
                cv = clv_val(r, "clv_close")
                if cv is not None:
                    by_theme.setdefault(r.get("theme") or "other", []).append(cv)
                    by_tag.setdefault(r.get("config_tag") or "?", []).append(cv)

            def avg(lst): return round(sum(lst)/len(lst)*100, 2) if lst else 0
            def pos_pct(lst): return round(sum(1 for v in lst if v > 0)/len(lst)*100, 1) if lst else 0

            return {
                "avg_clv_1h": avg(clvs["1h"]),
                "avg_clv_4h": avg(clvs["4h"]),
                "avg_clv_24h": avg(clvs["24h"]),
                "avg_clv_close": avg(clvs["close"]),
                "positive_clv_pct": pos_pct(clvs["close"]),
                "total": len(rows),
                "n_with_clv": len(clvs["close"]),
                "by_theme": [{"theme": t, "avg_clv": avg(v), "positive_pct": pos_pct(v), "n": len(v)}
                             for t, v in sorted(by_theme.items(), key=lambda x: -len(x[1]))],
                "by_tag": [{"tag": t, "avg_clv": avg(v), "positive_pct": pos_pct(v), "n": len(v)}
                           for t, v in sorted(by_tag.items(), key=lambda x: -len(x[1]))],
            }

    # ── DMA Weights ──

    async def get_dma_weights(self) -> list:
        """Get current DMA weights for dashboard display."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT source, weight, hits, misses, avg_likelihood, updated_at
                FROM dma_weights ORDER BY weight DESC
            """)
            return _clean_list(rows)

    # ── Micro (scalping) tables (read-only) ──

    async def get_micro_stats(self) -> dict:
        """Compute micro stats from positions. Reads BANKROLL from config_live."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    COALESCE(SUM(pnl), 0) as total_pnl,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
                    COUNT(*) as total_trades
                FROM micro_positions WHERE status='closed'
            """)
            open_staked = await conn.fetchval(
                "SELECT COALESCE(SUM(stake_amt), 0) FROM micro_positions WHERE status='open'"
            )
            br_row = await conn.fetchval(
                "SELECT value FROM config_live WHERE service='micro' AND key='BANKROLL'"
            )
            starting_bankroll = float(br_row) if br_row else 500.0
        total_pnl = float(row["total_pnl"]) if row else 0
        return {
            "bankroll": round(starting_bankroll + total_pnl - float(open_staked or 0), 2),
            "total_pnl": round(total_pnl, 2),
            "wins": int(row["wins"] or 0) if row else 0,
            "losses": int(row["losses"] or 0) if row else 0,
            "total_trades": int(row["total_trades"] or 0) if row else 0,
            "peak_equity": 0.0,
        }

    async def get_micro_open_positions(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT mp.id, mp.market_id, mp.question, mp.theme, mp.side, mp.entry_price,
                       mp.current_price, mp.unrealized_pnl, mp.stake_amt,
                       mp.end_date, mp.opened_at, m.url
                FROM micro_positions mp
                LEFT JOIN markets m ON m.id = mp.market_id
                WHERE mp.status='open' ORDER BY mp.opened_at DESC
            """)
            return _clean_list(rows)

    async def get_micro_closed_positions(self, limit: int = 100, offset: int = 0) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT mp.id, mp.market_id, mp.question, mp.theme, mp.side, mp.entry_price,
                       mp.current_price, mp.pnl, mp.result, mp.close_reason, mp.stake_amt,
                       mp.opened_at, mp.closed_at, m.url
                FROM micro_positions mp
                LEFT JOIN markets m ON m.id = mp.market_id
                WHERE mp.status='closed' ORDER BY mp.closed_at DESC LIMIT $1 OFFSET $2
            """, limit, offset)
            return _clean_list(rows)

    async def get_micro_closed_count(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM micro_positions WHERE status='closed'"
            ) or 0

    async def get_micro_cumulative_pnl(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT closed_at, pnl,
                    SUM(pnl) OVER (ORDER BY closed_at) as cumulative
                FROM micro_positions
                WHERE status='closed' AND closed_at IS NOT NULL
                ORDER BY closed_at ASC
            """)
            return [{"t": r["closed_at"].isoformat(), "pnl": float(r["pnl"]), "cum": float(r["cumulative"])} for r in rows]

    async def get_micro_analytics(self) -> dict:
        async with self.pool.acquire() as conn:
            by_theme = await conn.fetch("""
                SELECT theme, COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                    ROUND(SUM(pnl)::numeric, 2) as total_pnl
                FROM micro_positions WHERE status='closed' AND theme IS NOT NULL
                GROUP BY theme ORDER BY total DESC
            """)
            by_reason = await conn.fetch("""
                SELECT close_reason as reason, COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                    ROUND(SUM(pnl)::numeric, 2) as total_pnl
                FROM micro_positions WHERE status='closed' AND close_reason IS NOT NULL
                GROUP BY close_reason ORDER BY total DESC
            """)
            by_side = await conn.fetch("""
                SELECT side, COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl
                FROM micro_positions WHERE status='closed'
                GROUP BY side
            """)
            avg_lifetime = await conn.fetchrow("""
                SELECT ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600)::numeric, 1) as avg_hours
                FROM micro_positions WHERE status='closed' AND closed_at IS NOT NULL
            """)
            daily_pnl = await conn.fetch("""
                SELECT DATE(closed_at) as day,
                    ROUND(SUM(pnl)::numeric, 2) as pnl,
                    COUNT(*) as trades,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins
                FROM micro_positions WHERE status='closed' AND closed_at IS NOT NULL
                GROUP BY day ORDER BY day ASC
            """)
            by_config = await conn.fetch("""
                SELECT config_tag, COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(SUM(pnl)::numeric, 2) as total_pnl,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                    ROUND(AVG(stake_amt)::numeric, 2) as avg_stake
                FROM micro_positions WHERE status='closed' AND config_tag IS NOT NULL
                GROUP BY config_tag ORDER BY total DESC
            """)
        return {
            "by_theme": _clean_list(by_theme),
            "by_reason": _clean_list(by_reason),
            "by_side": _clean_list(by_side),
            "by_config": _clean_list(by_config),
            "avg_lifetime_hours": float(avg_lifetime["avg_hours"] or 0) if avg_lifetime else 0.0,
            "daily_pnl": _clean_list(daily_pnl),
        }

    async def get_theme_patterns(self) -> list:
        """Get all themes with calibration data and blocked status."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT category, trade_n, trade_wr, trade_roi, kelly_mult, ev_mult,
                       COALESCE(blocked, FALSE) as blocked
                FROM patterns ORDER BY COALESCE(trade_n, 0) DESC
            """)
            return _clean_list(rows)

    async def get_micro_price_paths(self, limit: int = 10) -> list:
        """Fetch price history for the most recent closed positions that have recorded ticks.
        Returns list of {position, ticks} — ticks include delta from previous tick."""
        async with self.pool.acquire() as conn:
            # Get recent closed positions that are losses OR had significant price movement
            positions = await conn.fetch("""
                SELECT p.market_id, p.side, p.question, p.entry_price, p.current_price,
                       p.pnl, p.result, p.close_reason, p.opened_at, p.closed_at,
                       p.stake_amt
                FROM micro_positions p
                WHERE p.status = 'closed'
                  AND (
                      p.result = 'LOSS'
                      OR (p.entry_price - p.current_price) > 0.05
                  )
                  AND EXISTS (
                      SELECT 1 FROM micro_price_history h
                      WHERE h.market_id = p.market_id AND h.side = p.side
                        AND h.ts >= p.opened_at AND h.ts <= p.closed_at + interval '2 seconds'
                  )
                ORDER BY p.closed_at DESC NULLS LAST
                LIMIT $1
            """, limit)

            result = []
            for pos in positions:
                ticks = await conn.fetch("""
                    SELECT price, source, ts
                    FROM micro_price_history
                    WHERE market_id = $1 AND side = $2
                      AND ts >= $3 AND ts <= $4::timestamptz + interval '2 seconds'
                    ORDER BY ts
                """, pos["market_id"], pos["side"],
                    pos["opened_at"], pos["closed_at"])
                result.append({
                    "pos": dict(pos),
                    "ticks": [{"price": float(t["price"]), "source": t["source"],
                               "ts": t["ts"]} for t in ticks],
                })
            return result

    async def set_theme_blocked(self, theme: str, blocked: bool):
        """Block or unblock a theme for engine trading."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO patterns (category, base_rate, sample_size, blocked)
                VALUES ($1, 0.5, 0, $2)
                ON CONFLICT (category) DO UPDATE SET blocked = $2, updated_at = NOW()
            """, theme, blocked)
        log.info(f"[DB] Engine theme '{theme}' {'BLOCKED' if blocked else 'UNBLOCKED'}")

    async def set_micro_theme_blocked(self, theme: str, blocked: bool):
        """Block or unblock a theme for micro trading."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO micro_theme_stats (theme, blocked)
                VALUES ($1, $2)
                ON CONFLICT (theme) DO UPDATE SET blocked = $2, updated_at = NOW()
            """, theme, blocked)
        log.info(f"[DB] Micro theme '{theme}' {'BLOCKED' if blocked else 'UNBLOCKED'}")

    async def get_micro_themes(self) -> list:
        """Get all micro themes with trade stats and blocked status."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT p.theme,
                    COUNT(*) as trades,
                    SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(SUM(p.pnl)::numeric, 2) as total_pnl,
                    COALESCE(t.blocked, FALSE) as blocked
                FROM micro_positions p
                LEFT JOIN micro_theme_stats t ON p.theme = t.theme
                WHERE p.status = 'closed' AND p.theme IS NOT NULL
                GROUP BY p.theme, t.blocked
                ORDER BY COUNT(*) DESC
            """)
            return [dict(r) for r in rows]

    # ── Live Config ──

    async def get_all_config(self) -> list:
        """Get all live config for both services."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM config_live ORDER BY service, section, key"
            )
        return _clean_list(rows)

    async def update_config(self, service: str, key: str, value: str) -> dict:
        """Update a single config key with validation and history."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM config_live WHERE service=$1 AND key=$2", service, key
            )
            if not row:
                raise ValueError(f"Config key {service}/{key} not found")
            # Type validation
            vtype = row["value_type"]
            if vtype == "float":
                num = float(value)
                if row["min_val"] is not None and num < float(row["min_val"]):
                    raise ValueError(f"Value {num} below minimum {row['min_val']}")
                if row["max_val"] is not None and num > float(row["max_val"]):
                    raise ValueError(f"Value {num} above maximum {row['max_val']}")
            elif vtype == "int":
                num = int(float(value))
                if row["min_val"] is not None and num < int(row["min_val"]):
                    raise ValueError(f"Value {num} below minimum {int(row['min_val'])}")
                if row["max_val"] is not None and num > int(row["max_val"]):
                    raise ValueError(f"Value {num} above maximum {int(row['max_val'])}")
            elif vtype == "bool":
                if value.lower() not in ("true", "false", "1", "0", "yes", "no"):
                    raise ValueError(f"Invalid bool value: {value}")

            old_value = row["value"]
            new_version = (row["version"] or 0) + 1
            async with conn.transaction():
                await conn.execute("""
                    UPDATE config_live SET value=$1, version=$2, updated_at=NOW()
                    WHERE service=$3 AND key=$4
                """, value, new_version, service, key)
                await conn.execute("""
                    INSERT INTO config_live_history (service, key, old_value, new_value, version)
                    VALUES ($1, $2, $3, $4, $5)
                """, service, key, old_value, value, new_version)
            # Notify engine/micro to reload config instantly
            await conn.execute(f"NOTIFY config_reload, '{service}/{key}'")
            log.info(f"[CONFIG] {service}/{key}: {old_value}→{value} (v{new_version})")
            return {"ok": True, "version": new_version}

    async def get_config_live_history(self, limit: int = 50) -> list:
        """Recent config_live changes."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT service, key, old_value, new_value, version, changed_at
                FROM config_live_history
                ORDER BY changed_at DESC LIMIT $1
            """, limit)
        return _clean_list(rows)

    async def close(self):
        if self.pool:
            await self.pool.close()
