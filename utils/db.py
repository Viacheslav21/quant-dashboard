import os
import logging
import asyncpg

log = logging.getLogger("db")


class Database:
    """Read-only database layer for dashboard. Connects to shared quant-engine PostgreSQL."""

    def __init__(self, url: str):
        self.url = url
        self.pool = None

    async def init(self):
        self.pool = await asyncpg.create_pool(self.url, min_size=2, max_size=5, command_timeout=30)
        log.info("[DB] Dashboard connected to shared database")

    async def get_stats(self) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM stats WHERE id=1")
            return dict(row) if row else {"bankroll": float(os.getenv("BANKROLL","1000")), "total_pnl":0,"total_bets":0,"wins":0,"losses":0,"avg_ev":0,"avg_kelly":0}

    async def get_open_positions(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM positions WHERE status='open' ORDER BY opened_at DESC")
            return [dict(r) for r in rows]

    async def get_closed_positions(self, limit: int = 100) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM positions WHERE status='closed' ORDER BY closed_at DESC LIMIT $1", limit)
            return [dict(r) for r in rows]

    async def get_recent_signals(self, limit: int = 20) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM signals ORDER BY created_at DESC LIMIT $1", limit)
            return [dict(r) for r in rows]

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

    async def get_signal_outcomes(self, limit: int = 200) -> list:
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
            return [dict(r) for r in rows]

    async def get_all_market_metrics(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT mm.*, m.question, m.yes_price, m.theme
                FROM market_metrics mm
                JOIN markets m ON mm.market_id = m.id
                WHERE m.is_active = TRUE
                ORDER BY mm.updated_at DESC
            """)
            return [dict(r) for r in rows]

    async def get_config_history(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM config_history ORDER BY created_at DESC")
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
            "by_config": [dict(r) for r in by_config],
            "by_theme": [dict(r) for r in by_theme],
            "by_source": [dict(r) for r in by_source],
            "by_side": [dict(r) for r in by_side],
            "by_reason": [dict(r) for r in by_reason],
            "calibration": [dict(r) for r in calibration],
            "avg_lifetime_hours": float(avg_lifetime["avg_hours"] or 0) if avg_lifetime else 0,
            "daily_pnl": [dict(r) for r in daily_pnl],
            "ev_predicted": float(ev_accuracy["avg_predicted_ev"] or 0) if ev_accuracy else 0,
            "ev_actual": float(ev_accuracy["avg_actual_return"] or 0) if ev_accuracy else 0,
        }

    # ── Arbitrage tables (read-only) ──

    async def get_arb_stats(self) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM arb_stats WHERE id=1")
            return dict(row) if row else {"bankroll": 0, "total_pnl": 0, "total_bets": 0, "wins": 0, "losses": 0}

    async def get_arb_open_positions(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM arb_positions WHERE status='open' ORDER BY opened_at DESC")
            return [dict(r) for r in rows]

    async def get_arb_closed_positions(self, limit: int = 100) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM arb_positions WHERE status='closed' ORDER BY closed_at DESC LIMIT $1", limit)
            return [dict(r) for r in rows]

    async def get_arb_signals(self, limit: int = 50) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM arb_signals ORDER BY created_at DESC LIMIT $1", limit)
            return [dict(r) for r in rows]

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
            "by_group": [dict(r) for r in by_group],
            "by_reason": [dict(r) for r in by_reason],
            "by_side": [dict(r) for r in by_side],
            "avg_lifetime_min": float(avg_lifetime["avg_min"] or 0) if avg_lifetime else 0,
            "daily_pnl": [dict(r) for r in daily_pnl],
        }

    async def close(self):
        if self.pool:
            await self.pool.close()
