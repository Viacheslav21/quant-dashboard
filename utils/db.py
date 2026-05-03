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
        """Compute micro stats from positions. Reads BANKROLL from config_live.
        peak_equity is the historical max of (starting_bankroll + cumulative pnl)
        across the closed-position timeline — derived in SQL so it's actually
        meaningful (the in-memory copy in micro/main.py is process-local and resets
        on every deploy, so we can't read it from here)."""
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
            peak = await conn.fetchval("""
                SELECT COALESCE(MAX(cum), 0) FROM (
                    SELECT SUM(pnl) OVER (ORDER BY closed_at) AS cum
                    FROM micro_positions
                    WHERE status='closed' AND closed_at IS NOT NULL
                ) t
            """)
        total_pnl = float(row["total_pnl"]) if row else 0
        return {
            "bankroll": round(starting_bankroll + total_pnl - float(open_staked or 0), 2),
            "total_pnl": round(total_pnl, 2),
            "wins": int(row["wins"] or 0) if row else 0,
            "losses": int(row["losses"] or 0) if row else 0,
            "total_trades": int(row["total_trades"] or 0) if row else 0,
            "peak_equity": round(starting_bankroll + float(peak or 0), 2),
        }

    async def get_micro_open_positions(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT mp.id, mp.market_id, mp.question, mp.theme, mp.side, mp.entry_price,
                       mp.current_price, mp.unrealized_pnl, mp.stake_amt,
                       mp.end_date, mp.opened_at, mp.slug,
                       'https://polymarket.com/event/' || COALESCE(mp.slug, mp.market_id) AS url
                FROM micro_positions mp
                WHERE mp.status='open' ORDER BY mp.opened_at DESC
            """)
            return _clean_list(rows)

    async def get_micro_closed_positions(self, limit: int = 100, offset: int = 0) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT mp.id, mp.market_id, mp.question, mp.theme, mp.side, mp.entry_price,
                       mp.current_price, mp.pnl, mp.result, mp.close_reason, mp.stake_amt,
                       mp.opened_at, mp.closed_at, mp.slug, mp.quality, mp.entry_days_left,
                       'https://polymarket.com/event/' || COALESCE(mp.slug, mp.market_id) AS url
                FROM micro_positions mp
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

    async def get_micro_recent_config_changes(self, days: int = 7) -> list:
        """Recent config_live edits — surfaces tuning history alongside performance
        so the audit reader can correlate 'what changed' with 'what happened'."""
        from datetime import timedelta
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT key, old_value, new_value, version, changed_at
                FROM config_live_history
                WHERE service = 'micro' AND changed_at > NOW() - $1::interval
                ORDER BY changed_at DESC
                LIMIT 30
            """, timedelta(days=days))
            return _clean_list(rows)

    async def get_micro_pnl_by_hour(self) -> list:
        """PnL bucketed by hour-of-day (UTC). Surfaces time-of-day patterns —
        e.g. esports markets resolving badly during Asia/Europe trading hours."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT EXTRACT(HOUR FROM closed_at)::int AS hour,
                    COUNT(*) AS total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) AS wins,
                    ROUND(SUM(pnl)::numeric, 2) AS total_pnl
                FROM micro_positions
                WHERE status='closed' AND result IS NOT NULL AND closed_at IS NOT NULL
                GROUP BY hour ORDER BY hour
            """)
            return _clean_list(rows)

    async def get_micro_theme_adj_wr(self) -> list:
        """Bayesian-shrunk WR per theme. Same shrinkage (k=20) as quant-micro's
        recalibrate_theme — lets the audit show which themes are close to the
        auto-block threshold (40%)."""
        async with self.pool.acquire() as conn:
            g = await conn.fetchrow("""
                SELECT COUNT(*) AS n,
                       SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) AS wins
                FROM micro_positions WHERE status='closed' AND result IS NOT NULL
            """)
            global_wr = (float(g["wins"] or 0) / max(int(g["n"] or 0), 1)) if g else 0.5
            rows = await conn.fetch("""
                SELECT theme, COUNT(*) AS n,
                       SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) AS wins
                FROM micro_positions
                WHERE status='closed' AND result IS NOT NULL AND theme IS NOT NULL
                GROUP BY theme ORDER BY COUNT(*) DESC
            """)
            k = 20
            out = []
            for r in rows:
                n = int(r["n"] or 0)
                wins = int(r["wins"] or 0)
                raw = wins / n if n else 0
                adj = (n * raw + k * global_wr) / (n + k) if n else global_wr
                out.append({"theme": r["theme"], "n": n, "wins": wins,
                            "raw_wr": round(raw, 3), "adj_wr": round(adj, 3)})
            return out

    async def get_micro_worst_per_reason(self, per_reason: int = 3) -> list:
        """Top-N worst PnL trades per close_reason — fastest way to spot
        recurring failure patterns (e.g. crypto rapid_drops, sports max_loss)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT close_reason, side, theme, question,
                       ROUND(pnl::numeric, 2) AS pnl,
                       ROUND(stake_amt::numeric, 2) AS stake,
                       ROUND((entry_price * 100)::numeric, 1) AS entry_c,
                       ROUND((current_price * 100)::numeric, 1) AS exit_c,
                       closed_at
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY close_reason ORDER BY pnl ASC) AS rn
                    FROM micro_positions
                    WHERE status='closed' AND result IS NOT NULL
                ) t
                WHERE rn <= $1
                ORDER BY close_reason, pnl ASC
            """, per_reason)
            return _clean_list(rows)

    async def get_micro_rapid_drop_blocks(self) -> dict:
        """Mirror of the MAX_LOSS REST-block diagnostic but for rapid_drop. We
        don't currently log these to micro_price_history (only max_loss_blocked
        gets a tick), so this returns counts of actual rapid_drop closes vs. how
        many were preceded by REST blocks recorded in monitor.py logs.
        For now we approximate using the rapid_drop close events themselves."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE closed_at > NOW() - INTERVAL '24 hours') AS d1,
                    COUNT(*) FILTER (WHERE closed_at > NOW() - INTERVAL '7 days')   AS d7,
                    COUNT(*) AS total
                FROM micro_positions
                WHERE close_reason = 'rapid_drop' AND status='closed'
            """)
        return _clean(row) if row else {}

    async def get_micro_audit_aggregates(self) -> dict:
        """Aggregates for the audit report computed in SQL — replaces the Python
        loops over a 9999-row closed_all snapshot which silently truncated stats
        once the table grew past the limit.
        Single roundtrip; everything is bucket counts/sums over `status='closed'`.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                WITH closed AS (
                    SELECT pnl, result, close_reason, opened_at, closed_at,
                           stake_amt, theme,
                           EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600 AS hold_h
                    FROM micro_positions
                    WHERE status = 'closed' AND result IS NOT NULL
                )
                SELECT
                    -- Win/loss sizes
                    COALESCE(AVG(pnl) FILTER (WHERE result='WIN'),  0) AS avg_win,
                    COALESCE(AVG(pnl) FILTER (WHERE result='LOSS'), 0) AS avg_loss,
                    COALESCE(MAX(pnl), 0) AS best_pnl,
                    COALESCE(MIN(pnl), 0) AS worst_pnl,
                    -- Recent windows
                    COALESCE(SUM(pnl) FILTER (WHERE closed_at > NOW() - INTERVAL '7 days'),  0) AS pnl_7d,
                    COUNT(*)              FILTER (WHERE closed_at > NOW() - INTERVAL '7 days')     AS trades_7d,
                    COUNT(*) FILTER (WHERE closed_at > NOW() - INTERVAL '7 days' AND result='WIN') AS wins_7d,
                    COALESCE(SUM(pnl) FILTER (WHERE closed_at > NOW() - INTERVAL '30 days'), 0) AS pnl_30d,
                    COUNT(*)              FILTER (WHERE closed_at > NOW() - INTERVAL '30 days')    AS trades_30d,
                    -- Hold time split
                    COALESCE(AVG(hold_h) FILTER (WHERE result='WIN'),  0) AS hold_h_win,
                    COALESCE(AVG(hold_h) FILTER (WHERE result='LOSS'), 0) AS hold_h_loss,
                    -- Close reason buckets
                    COUNT(*) FILTER (WHERE close_reason = 'resolved')      AS n_resolved,
                    COUNT(*) FILTER (WHERE close_reason = 'resolved_loss') AS n_resolved_loss,
                    COUNT(*) FILTER (WHERE close_reason = 'take_profit')   AS n_take_profit,
                    COUNT(*) FILTER (WHERE close_reason = 'expired')       AS n_expired,
                    COUNT(*) FILTER (WHERE close_reason = 'max_loss')      AS n_max_loss,
                    COUNT(*) FILTER (WHERE close_reason = 'rapid_drop')    AS n_rapid_drop,
                    COUNT(*) AS total
                FROM closed
            """)
            best = await conn.fetchrow("""
                SELECT pnl, question
                FROM micro_positions
                WHERE status='closed' AND result IS NOT NULL
                ORDER BY pnl DESC NULLS LAST LIMIT 1
            """)
            worst = await conn.fetchrow("""
                SELECT pnl, question
                FROM micro_positions
                WHERE status='closed' AND result IS NOT NULL
                ORDER BY pnl ASC NULLS LAST LIMIT 1
            """)
            theme_roi = await conn.fetch("""
                SELECT theme,
                    COUNT(*) AS total,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) AS wins,
                    COALESCE(SUM(pnl), 0) AS total_pnl,
                    COALESCE(SUM(stake_amt), 0) AS total_stake,
                    COALESCE(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600), 0) AS avg_hold_h
                FROM micro_positions
                WHERE status='closed' AND result IS NOT NULL AND theme IS NOT NULL
                GROUP BY theme
                ORDER BY COUNT(*) DESC
            """)
        return {
            **{k: float(v) if isinstance(v, Decimal) else (int(v) if isinstance(v, int) else v)
               for k, v in dict(row).items()},
            "best":  {"pnl": float(best["pnl"]) if best else 0,
                      "question": best["question"] if best else ""},
            "worst": {"pnl": float(worst["pnl"]) if worst else 0,
                      "question": worst["question"] if worst else ""},
            "theme_roi": _clean_list(theme_roi),
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
