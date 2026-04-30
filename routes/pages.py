"""HTML page routes: /, /analytics, /micro, /model."""

import os
import json
import asyncio
from datetime import date, datetime, timedelta
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

import routes.deps as deps
from routes.deps import (
    ctx, parse_date, log, to_json,
    compute_sharpe_ratio, compute_max_drawdown,
    compute_streaks, compute_equity_curve, compute_pnl_distribution,
)

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


# ENGINE DISABLED ↓
# @router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, page: int = 1, date_from: str = None, date_to: str = None):
    try:
        per_page = 20
        df = parse_date(date_from)
        dt = parse_date(date_to)

        stats, open_, total_closed, signals, pnl_data, all_trades, rolling, best_worst = await asyncio.gather(
            deps.db.get_stats(),
            deps.db.get_open_positions(),
            deps.db.get_closed_positions_count(df, dt),
            deps.db.get_recent_signals(limit=10),
            deps.db.get_cumulative_pnl(),
            deps.db.get_all_closed_trades(),
            deps.db.get_rolling_performance(),
            deps.db.get_best_worst_trades(),
        )
        total_pages = max(1, (total_closed + per_page - 1) // per_page)
        closed = await deps.db.get_closed_positions(limit=per_page, offset=(page - 1) * per_page, date_from=df, date_to=dt)

        start = deps.config["BANKROLL"]
        # ROI on starting capital — uses realized total_pnl, NOT (bankroll - start).
        # bankroll nets open stakes, so it would flag a profitable bot as -ROI whenever
        # a lot of capital is tied up in open positions.
        roi = (stats["total_pnl"] / start * 100) if start > 0 else 0
        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0
        mode = "Simulation" if (deps.config or {}).get("SIMULATION", True) else "Live"

        sharpe = compute_sharpe_ratio(all_trades)
        drawdown = compute_max_drawdown(all_trades, start)
        streaks = compute_streaks(all_trades)
        equity = compute_equity_curve(all_trades, start)

        open_in_profit = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) >= 0)
        open_in_loss = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) < 0)
        open_total_upnl = sum((p.get("unrealized_pnl") or 0) for p in open_)

        return deps.templates.TemplateResponse(request, "dashboard.html", ctx(
            active_page="dashboard",
            stats=stats, start=start, roi=roi, wr=wr, mode=mode,
            open_positions=open_, closed=closed, signals=signals,
            open_in_profit=open_in_profit, open_in_loss=open_in_loss,
            open_total_upnl=open_total_upnl,
            total_closed=total_closed, page=page, total_pages=total_pages,
            pnl_data=to_json(pnl_data),
            equity_data=to_json(equity),
            drawdown_data=to_json(drawdown["series"]),
            sharpe=sharpe, drawdown=drawdown, streaks=streaks,
            rolling=rolling, best_worst=best_worst,
            max_open=os.getenv("MAX_OPEN", "5"),
            date_from=date_from, date_to=date_to,
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Render error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Dashboard Error</h1><pre>{e}</pre>", status_code=500)


# ENGINE DISABLED ↓
# @router.get("/analytics", response_class=HTMLResponse)
async def analytics(request: Request, date_from: str = None, date_to: str = None):
    try:
        (data, pnl_data, sig_outcomes, market_metrics, config_hist,
         stats, all_trades, rolling, best_worst, clv, dma_weights, theme_patterns) = await asyncio.gather(
            deps.db.get_analytics(),
            deps.db.get_cumulative_pnl(),
            deps.db.get_signal_outcomes(limit=50),
            deps.db.get_all_market_metrics(limit=50),
            deps.db.get_config_history(),
            deps.db.get_stats(),
            deps.db.get_all_closed_trades(),
            deps.db.get_rolling_performance(),
            deps.db.get_best_worst_trades(),
            deps.db.get_clv_analytics(),
            deps.db.get_dma_weights(),
            deps.db.get_theme_patterns(),
        )
        blocked_themes = {p["category"] for p in theme_patterns if p.get("blocked")}
        # Add blocked themes with 0 trades so they show in PnL by Theme table
        existing_themes = {r["theme"] for r in data["by_theme"]}
        for bt in blocked_themes:
            if bt not in existing_themes:
                data["by_theme"].append({"theme": bt, "total": 0, "wins": 0, "avg_pnl": 0, "total_pnl": 0})
        config_map = {c["tag"]: c["params"] for c in config_hist}

        start = deps.config["BANKROLL"]
        # ROI on starting capital — uses realized total_pnl, NOT (bankroll - start).
        # bankroll nets open stakes, so it would flag a profitable bot as -ROI whenever
        # a lot of capital is tied up in open positions.
        roi = (stats["total_pnl"] / start * 100) if start > 0 else 0
        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0

        sharpe = compute_sharpe_ratio(all_trades)
        drawdown = compute_max_drawdown(all_trades, start)
        equity = compute_equity_curve(all_trades, start)
        pnl_dist = compute_pnl_distribution(all_trades)

        ev_pred = data["ev_predicted"] * 100
        ev_act = data["ev_actual"] * 100

        config_rows = []
        for r in data["by_config"]:
            tag = r["config_tag"]
            params = config_map.get(tag, {})
            if isinstance(params, str):
                try:
                    params = json.loads(params)
                except Exception:
                    params = {}
            param_str = (
                f"EV≥{params.get('MIN_EV', '')} KL≥{params.get('MIN_KL', '')} Edge≥{params.get('MIN_EDGE', '')} "
                f"Kelly:{params.get('MIN_KELLY_FRAC', '')}–{params.get('MAX_KELLY_FRAC', '')} "
                f"TP:{params.get('TAKE_PROFIT_PCT', '')} SL:{params.get('STOP_LOSS_PCT', '')} "
                f"Trail:{params.get('TRAILING_TP', '')}/{params.get('TRAILING_PULLBACK', '')} "
                f"MaxOpen:{params.get('MAX_OPEN', '')} MaxTheme:{params.get('MAX_PER_THEME', '')} "
                f"Prospect:{'Y' if params.get('USE_PROSPECT') else 'N'}"
            ) if params else "—"
            config_rows.append({**r, "param_str": param_str})

        pace = _compute_pnl_pace(data.get("daily_pnl"))

        valid_sigs = [s for s in sig_outcomes if s.get("price_move") is not None]
        exec_sigs = [s for s in valid_sigs if s["executed"]]
        rej_sigs = [s for s in valid_sigs if not s["executed"]]
        exec_right = sum(1 for s in exec_sigs if s.get("price_move") and s["price_move"] > 0)
        rej_saved = sum(1 for s in rej_sigs if not (s.get("price_move") and s["price_move"] > 0))

        return deps.templates.TemplateResponse(request, "analytics.html", ctx(
            active_page="analytics",
            stats=stats, wr=wr, ev_pred=ev_pred, ev_act=ev_act,
            data=data, config_rows=config_rows,
            sig_outcomes=sig_outcomes[:50], market_metrics=market_metrics,
            pnl_data=to_json(pnl_data),
            daily_data=to_json(data["daily_pnl"]),
            cal_data=to_json(data["calibration"]),
            theme_data=to_json(data["by_theme"]),
            side_data=to_json(data["by_side"]),
            equity_data=to_json(equity),
            drawdown_data=to_json(drawdown["series"]),
            dist_data=to_json(pnl_dist),
            sharpe=sharpe, drawdown=drawdown,
            rolling=rolling, best_worst=best_worst,
            exec_right=exec_right, exec_total=len(exec_sigs),
            rej_right=sum(1 for s in rej_sigs if s.get("price_move") and s["price_move"] > 0),
            rej_saved=rej_saved, clv=clv, dma_weights=dma_weights, blocked_themes=blocked_themes,
            date_from=date_from, date_to=date_to,
            pnl_n_days=pace["n_days"],
            pnl_avg_day=pace["avg_day"],
            pnl_7d=pace["pnl_7d"],
            pnl_30d=pace["pnl_30d"],
            pnl_ytd=pace["pnl_ytd"],
            has_api_secret=True,  # simplified — auth checked by middleware
            api_secret_required=bool(os.getenv("API_SECRET", "")),
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Analytics error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Analytics Error</h1><pre>{e}</pre>", status_code=500)


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

        # Read BANKROLL from config_live, fallback to 500
        try:
            async with deps.db.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT value FROM config_live WHERE service='micro' AND key='BANKROLL'"
                )
                micro_bankroll = float(row["value"]) if row else 500.0
        except Exception:
            micro_bankroll = 500.0
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


@router.get("/model", response_class=HTMLResponse)
async def model_page(request: Request):
    try:
        import httpx
        ml_url = os.getenv("ML_API_URL", "")
        health_data = {}
        metrics = {}

        if ml_url:
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    r = await client.get(f"{ml_url}/health")
                    health_data = r.json()
            except Exception as e:
                health_data = {"status": "offline", "error": str(e)}

            try:
                async with deps.db.pool.acquire() as conn:
                    row = await conn.fetchrow("SELECT metrics FROM ml_models WHERE id='main'")
                    if row and row["metrics"]:
                        m = row["metrics"]
                        if isinstance(m, str):
                            metrics = json.loads(m)
                        else:
                            metrics = dict(m)
            except Exception:
                pass

        return deps.templates.TemplateResponse(request, "model.html", ctx(
            active_page="model",
            health=health_data,
            metrics=metrics,
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Model page error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Model Error</h1><pre>{e}</pre>", status_code=500)


@router.get("/config", response_class=HTMLResponse)
async def config_page(request: Request):
    try:
        configs, history = await asyncio.gather(
            deps.db.get_all_config(),
            deps.db.get_config_live_history(limit=30),
        )
        # Group by service then section
        grouped = {}
        for c in configs:
            svc = c.get("service", "unknown")
            sec = c.get("section", "general")
            grouped.setdefault(svc, {}).setdefault(sec, []).append(c)

        max_version = max((c.get("version", 0) for c in configs), default=0) if configs else 0

        return deps.templates.TemplateResponse(request, "config.html", ctx(
            active_page="config",
            grouped=grouped,
            history=history,
            max_version=max_version,
            engine_count=sum(len(v) for v in grouped.get("engine", {}).values()),
            micro_count=sum(len(v) for v in grouped.get("micro", {}).values()),
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Config page error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Config Error</h1><pre>{e}</pre>", status_code=500)
