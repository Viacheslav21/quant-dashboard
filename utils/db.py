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
            # JSONB columns — skip to avoid unhashable type errors
            continue
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
                       unrealized_pnl, ev, kl, stake_amt, url, opened_at, theme
                FROM positions WHERE status='open' ORDER BY opened_at DESC
            """)
            return _clean_list(rows)

    async def get_closed_positions(self, limit: int = 100, offset: int = 0, date_from=None, date_to=None) -> list:
        async with self.pool.acquire() as conn:
            query = """SELECT id, question, side, side_price, current_price, outcome,
                              pnl, result, ev, kl, stake_amt, opened_at, closed_at, theme, config_tag
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
                       ev, kl, kelly, source, executed, created_at
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
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl
                FROM positions WHERE status='closed' AND theme IS NOT NULL
                GROUP BY theme ORDER BY total DESC
            """)
            by_source = await conn.fetch("""
                SELECT COALESCE(s.source, 'math') as source, COUNT(*) as total,
                    SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(p.pnl)::numeric, 2) as avg_pnl
                FROM positions p
                LEFT JOIN signals s ON p.signal_id = s.id
                WHERE p.status='closed'
                GROUP BY COALESCE(s.source, 'math') ORDER BY total DESC
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
                FROM positions WHERE status='closed'
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
                GROUP BY day ORDER BY day DESC LIMIT 14
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
            "by_source": _clean_list(by_source),
            "by_side": _clean_list(by_side),
            "by_reason": _clean_list(by_reason),
            "calibration": _clean_list(calibration),
            "avg_lifetime_hours": float(avg_lifetime["avg_hours"] or 0) if avg_lifetime else 0.0,
            "daily_pnl": _clean_list(daily_pnl),
            "ev_predicted": float(ev_accuracy["avg_predicted_ev"] or 0) if ev_accuracy else 0.0,
            "ev_actual": float(ev_accuracy["avg_actual_return"] or 0) if ev_accuracy else 0.0,
        }

    # ── New metric queries ──

    async def get_all_closed_trades(self) -> list:
        """All closed trades ordered chronologically for metric computation."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT pnl, stake_amt, result, closed_at, question
                FROM positions WHERE status='closed' AND closed_at IS NOT NULL
                ORDER BY closed_at ASC
            """)
            return _clean_list(rows)

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

    # ── Arbitrage tables (read-only) ──

    async def get_arb_stats(self) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT bankroll, total_pnl, wins, losses FROM arb_stats WHERE id=1")
            if row:
                r = _clean(row)
                return {
                    "bankroll": float(r.get("bankroll") or 0),
                    "total_pnl": float(r.get("total_pnl") or 0),
                    "wins": int(r.get("wins") or 0),
                    "losses": int(r.get("losses") or 0),
                }
            return {"bankroll": 0.0, "total_pnl": 0.0, "wins": 0, "losses": 0}

    async def get_arb_open_positions(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, market_id, question, side, side_price, current_price,
                       unrealized_pnl, ev, stake_amt, group_name, opened_at
                FROM arb_positions WHERE status='open' ORDER BY opened_at DESC
            """)
            return _clean_list(rows)

    async def get_arb_closed_positions(self, limit: int = 100, offset: int = 0) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, question, side, side_price, current_price, pnl, result,
                       close_reason, group_name, opened_at, closed_at
                FROM arb_positions WHERE status='closed' ORDER BY closed_at DESC LIMIT $1 OFFSET $2
            """, limit, offset)
            return _clean_list(rows)

    async def get_arb_signals(self, limit: int = 50) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, market_id, question, side, side_price, ev, group_name,
                       leader_question, leader_move, signal_type, executed, created_at
                FROM arb_signals ORDER BY created_at DESC LIMIT $1
            """, limit)
            return _clean_list(rows)

    async def get_arb_cumulative_pnl(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT closed_at, pnl,
                    SUM(pnl) OVER (ORDER BY closed_at) as cumulative
                FROM arb_positions
                WHERE status='closed' AND closed_at IS NOT NULL
                ORDER BY closed_at ASC
            """)
            return [{"t": r["closed_at"].isoformat(), "pnl": float(r["pnl"]), "cum": float(r["cumulative"])} for r in rows]

    async def get_arb_analytics(self) -> dict:
        async with self.pool.acquire() as conn:
            by_group = await conn.fetch("""
                SELECT group_name, COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                    ROUND(SUM(pnl)::numeric, 2) as total_pnl
                FROM arb_positions WHERE status='closed' AND group_name IS NOT NULL
                GROUP BY group_name ORDER BY total DESC
            """)
            by_reason = await conn.fetch("""
                SELECT close_reason as reason, COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl
                FROM arb_positions WHERE status='closed' AND close_reason IS NOT NULL
                GROUP BY close_reason ORDER BY total DESC
            """)
            by_side = await conn.fetch("""
                SELECT side, COUNT(*) as total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(pnl)::numeric, 2) as avg_pnl
                FROM arb_positions WHERE status='closed'
                GROUP BY side
            """)
            avg_lifetime = await conn.fetchrow("""
                SELECT ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at)) / 60)::numeric, 1) as avg_min
                FROM arb_positions WHERE status='closed' AND closed_at IS NOT NULL
            """)
            daily_pnl = await conn.fetch("""
                SELECT DATE(closed_at) as day,
                    ROUND(SUM(pnl)::numeric, 2) as pnl,
                    COUNT(*) as trades,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins
                FROM arb_positions WHERE status='closed' AND closed_at IS NOT NULL
                GROUP BY day ORDER BY day DESC LIMIT 14
            """)
        return {
            "by_group": _clean_list(by_group),
            "by_reason": _clean_list(by_reason),
            "by_side": _clean_list(by_side),
            "avg_lifetime_min": float(avg_lifetime["avg_min"] or 0) if avg_lifetime else 0.0,
            "daily_pnl": _clean_list(daily_pnl),
        }

    async def close(self):
        if self.pool:
            await self.pool.close()
