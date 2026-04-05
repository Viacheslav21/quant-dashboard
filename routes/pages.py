"""HTML page routes: /, /analytics, /scalping, /model."""

import os
import json
import asyncio
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

import routes.deps as deps
from routes.deps import (
    ctx, parse_date, log, to_json,
    compute_sharpe_ratio, compute_max_drawdown,
    compute_streaks, compute_equity_curve, compute_pnl_distribution,
)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
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
        roi = ((stats["bankroll"] - start) / start * 100) if start > 0 else 0
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


@router.get("/analytics", response_class=HTMLResponse)
async def analytics(request: Request, date_from: str = None, date_to: str = None):
    try:
        (data, pnl_data, sig_outcomes, market_metrics, config_hist,
         stats, all_trades, rolling, best_worst, clv, dma_weights) = await asyncio.gather(
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
        )
        config_map = {c["tag"]: c["params"] for c in config_hist}

        start = deps.config["BANKROLL"]
        roi = ((stats["bankroll"] - start) / start * 100) if start > 0 else 0
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
            rej_saved=rej_saved, clv=clv, dma_weights=dma_weights,
            date_from=date_from, date_to=date_to,
            has_api_secret=True,  # simplified — auth checked by middleware
            api_secret_required=bool(os.getenv("API_SECRET", "")),
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Analytics error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Analytics Error</h1><pre>{e}</pre>", status_code=500)


@router.get("/scalping", response_class=HTMLResponse)
async def scalping(request: Request, page: int = 1):
    try:
        per_page = 20
        stats, open_, pnl_data, data = await asyncio.gather(
            deps.db.get_micro_stats(),
            deps.db.get_micro_open_positions(),
            deps.db.get_micro_cumulative_pnl(),
            deps.db.get_micro_analytics(),
        )
        total_closed = stats["wins"] + stats["losses"]
        total_pages = max(1, (total_closed + per_page - 1) // per_page)
        closed = await deps.db.get_micro_closed_positions(limit=per_page, offset=(page - 1) * per_page)

        micro_bankroll = 500.0
        roi = ((stats["bankroll"] - micro_bankroll) / micro_bankroll * 100) if micro_bankroll > 0 else 0
        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0

        open_in_profit = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) >= 0)
        open_in_loss = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) < 0)
        open_total_upnl = sum((p.get("unrealized_pnl") or 0) for p in open_)

        return deps.templates.TemplateResponse(request, "scalping.html", ctx(
            active_page="scalping",
            stats=stats, roi=roi, wr=wr, total=total,
            micro_bankroll=micro_bankroll,
            open_positions=open_, closed=closed,
            data=data,
            open_in_profit=open_in_profit,
            open_in_loss=open_in_loss,
            open_total_upnl=open_total_upnl,
            total_closed=total_closed, page=page, total_pages=total_pages,
            pnl_data=to_json(pnl_data),
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Scalping error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Scalping Error</h1><pre>{e}</pre>", status_code=500)


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
