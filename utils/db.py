import os
import logging
from decimal import Decimal
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
    """Read-only dashboard DB layer for the micro bot.
    Connects to the shared Postgres. Schema for micro tables is owned by quant-micro;
    config_live tables are also created by quant-micro on startup. Dashboard only
    READS positions/themes/config and WRITES to config_live (edit) + micro_theme_stats."""

    def __init__(self, url: str):
        self.url = url
        self.pool = None

    async def init(self):
        self.pool = await asyncpg.create_pool(self.url, min_size=1, max_size=5, command_timeout=30)
        log.info("[DB] Dashboard connected to shared database")

    async def close(self):
        if self.pool:
            await self.pool.close()

    # ── Micro positions / stats ──

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
            starting_bankroll = float(br_row) if br_row else 1000.0
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

    async def get_micro_price_paths(self, limit: int = 10) -> list:
        """Fetch price history for the most recent closed positions that have recorded ticks."""
        async with self.pool.acquire() as conn:
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

    # ── Themes ──

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
        """Get all live config rows."""
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
