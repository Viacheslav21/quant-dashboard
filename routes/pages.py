"""HTML page routes: /, /micro, /config."""

import asyncio
from datetime import date, datetime, timedelta
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

import routes.deps as deps
from routes.deps import ctx, log, to_json

router = APIRouter()


def _compute_pnl_pace(daily_pnl_rows) -> dict:
    """Aggregate {day, pnl} rows into pace metrics shown next to the Daily P&L chart.
    avg/day uses available days; 7d/30d are rolling-window sums; YTD is sum since Jan 1.
    Handles `day` as date / datetime / ISO string."""
    today = date.today()
    week_cutoff = today - timedelta(days=7)
    month_cutoff = today - timedelta(days=30)
    year_start = date(today.year, 1, 1)

    pnls: list[float] = []
    pnl_7d = pnl_30d = pnl_ytd = 0.0
    for r in (daily_pnl_rows or []):
        p = float(r.get("pnl", 0) or 0)
        pnls.append(p)
        d = r.get("day")
        if isinstance(d, datetime):
            d = d.date()
        elif isinstance(d, str):
            try:
                d = date.fromisoformat(d[:10])
            except Exception:
                continue
        elif not isinstance(d, date):
            continue
        if d >= week_cutoff:
            pnl_7d += p
        if d >= month_cutoff:
            pnl_30d += p
        if d >= year_start:
            pnl_ytd += p

    n_days = len(pnls)
    return {
        "n_days": n_days,
        "avg_day": (sum(pnls) / n_days) if n_days > 0 else 0.0,
        "pnl_7d": pnl_7d,
        "pnl_30d": pnl_30d,
        "pnl_ytd": pnl_ytd,
    }


@router.get("/", response_class=RedirectResponse)
async def root_redirect():
    return RedirectResponse(url="/micro")


@router.get("/micro", response_class=HTMLResponse)
async def micro(request: Request, page: int = 1):
    try:
        per_page = 20
        stats, open_, pnl_data, data, all_closed = await asyncio.gather(
            deps.db.get_micro_stats(),
            deps.db.get_micro_open_positions(),
            deps.db.get_micro_cumulative_pnl(),
            deps.db.get_micro_analytics(),
            deps.db.get_micro_closed_positions(limit=9999, offset=0),
        )
        total_closed = stats["wins"] + stats["losses"]
        total_pages = max(1, (total_closed + per_page - 1) // per_page)
        closed = await deps.db.get_micro_closed_positions(limit=per_page, offset=(page - 1) * per_page)

        # Read BANKROLL from config_live, fallback to 1000
        try:
            async with deps.db.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT value FROM config_live WHERE service='micro' AND key='BANKROLL'"
                )
                micro_bankroll = float(row["value"]) if row else 1000.0
        except Exception:
            micro_bankroll = 1000.0
        roi = ((stats["bankroll"] - micro_bankroll) / micro_bankroll * 100) if micro_bankroll > 0 else 0
        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0

        open_in_profit = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) >= 0)
        open_in_loss = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) < 0)
        open_total_upnl = sum((p.get("unrealized_pnl") or 0) for p in open_)
        open_staked = sum(p.get("stake_amt", 0) for p in open_)

        # Best/worst trades
        best_trade = max(all_closed, key=lambda t: float(t.get("pnl") or 0)) if all_closed else None
        worst_trade = min(all_closed, key=lambda t: float(t.get("pnl") or 0)) if all_closed else None

        pace = _compute_pnl_pace(data.get("daily_pnl"))

        # Theme calibration computed from positions + blocked flag
        theme_cal = []
        try:
            async with deps.db.pool.acquire() as conn:
                theme_cal = [dict(r) for r in await conn.fetch("""
                    SELECT p.theme,
                        COUNT(*) as trades,
                        SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN p.result='LOSS' THEN 1 ELSE 0 END) as losses,
                        ROUND(SUM(p.pnl)::numeric, 2) as total_pnl,
                        COALESCE(t.blocked, false) as blocked
                    FROM micro_positions p
                    LEFT JOIN micro_theme_stats t ON p.theme = t.theme
                    WHERE p.status = 'closed' AND p.theme IS NOT NULL
                    GROUP BY p.theme, t.blocked
                    UNION ALL
                    SELECT t.theme, 0, 0, 0, 0.0, t.blocked
                    FROM micro_theme_stats t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM micro_positions p
                        WHERE p.theme = t.theme AND p.status = 'closed'
                    )
                    ORDER BY trades DESC
                """)]
        except Exception:
            pass

        return deps.templates.TemplateResponse(request, "micro.html", ctx(
            active_page="micro",
            stats=stats, roi=roi, wr=wr, total=total,
            micro_bankroll=micro_bankroll,
            open_positions=open_, closed=closed,
            data=data,
            open_in_profit=open_in_profit,
            open_in_loss=open_in_loss,
            open_total_upnl=open_total_upnl,
            open_staked=open_staked,
            total_closed=total_closed, page=page, total_pages=total_pages,
            pnl_data=to_json(pnl_data),
            daily_data=to_json(data["daily_pnl"]),
            best_trade=best_trade, worst_trade=worst_trade,
            theme_cal=theme_cal,
            pnl_n_days=pace["n_days"],
            pnl_avg_day=pace["avg_day"],
            pnl_7d=pace["pnl_7d"],
            pnl_30d=pace["pnl_30d"],
            pnl_ytd=pace["pnl_ytd"],
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Micro error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Micro Error</h1><pre>{e}</pre>", status_code=500)


@router.get("/config", response_class=HTMLResponse)
async def config_page(request: Request):
    try:
        configs, history = await asyncio.gather(
            deps.db.get_all_config(),
            deps.db.get_config_live_history(limit=30),
        )
        # Group by service then section. Engine service hidden — only micro shown.
        grouped = {}
        for c in configs:
            svc = c.get("service", "unknown")
            if svc != "micro":
                continue
            sec = c.get("section", "general")
            grouped.setdefault(svc, {}).setdefault(sec, []).append(c)

        max_version = max((c.get("version", 0) for c in configs if c.get("service") == "micro"), default=0)

        return deps.templates.TemplateResponse(request, "config.html", ctx(
            active_page="config",
            grouped=grouped,
            history=[h for h in history if h.get("service") == "micro"],
            max_version=max_version,
            engine_count=0,
            micro_count=sum(len(v) for v in grouped.get("micro", {}).values()),
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Config page error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Config Error</h1><pre>{e}</pre>", status_code=500)
