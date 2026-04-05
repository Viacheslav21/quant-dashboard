
"""Mobile API routes: /api/mobile/*."""

import asyncio
from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response

from routes.deps import db, config, log, to_json
from routes.deps import compute_sharpe_ratio, compute_max_drawdown, compute_streaks, compute_equity_curve

router = APIRouter(prefix="/api/mobile")


@router.get("/overview")
async def mobile_overview():
    """Main screen: bankroll, PnL, WR, open positions summary."""
    try:
        stats, open_pos = await asyncio.gather(
            db.get_stats(),
            db.get_open_positions(),
        )
        start = config["BANKROLL"]
        total = stats["wins"] + stats["losses"]

        themes = {}
        for p in open_pos:
            t = p.get("theme", "other")
            if t not in themes:
                themes[t] = {"count": 0, "staked": 0, "upnl": 0}
            themes[t]["count"] += 1
            themes[t]["staked"] += p.get("stake_amt", 0)
            themes[t]["upnl"] += p.get("unrealized_pnl", 0) or 0

        return Response(to_json({
            "bankroll": stats["bankroll"],
            "start_bankroll": start,
            "total_pnl": stats["total_pnl"],
            "roi_pct": round((stats["bankroll"] - start) / start * 100, 1) if start > 0 else 0,
            "wins": stats["wins"],
            "losses": stats["losses"],
            "wr_pct": round(stats["wins"] / total * 100, 1) if total > 0 else 0,
            "open_count": len(open_pos),
            "open_upnl": round(sum((p.get("unrealized_pnl") or 0) for p in open_pos), 2),
            "open_staked": round(sum(p.get("stake_amt", 0) for p in open_pos), 2),
            "themes": themes,
        }), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/positions")
async def mobile_positions(status: str = "open", page: int = 1, limit: int = 50):
    """Open or closed positions list."""
    try:
        if status == "open":
            positions = await db.get_open_positions()
            result = []
            for p in positions:
                upnl = p.get("unrealized_pnl") or 0
                entry = p.get("side_price", 0)
                pnl_pct = (p.get("current_price", entry) - entry) / entry * 100 if entry > 0 else 0
                result.append({
                    "id": p["id"], "market_id": p["market_id"],
                    "question": p.get("question", ""), "theme": p.get("theme", "other"),
                    "side": p["side"], "entry_price": entry,
                    "current_price": p.get("current_price", entry),
                    "stake": p.get("stake_amt", 0), "upnl": round(upnl, 2),
                    "pnl_pct": round(pnl_pct, 1),
                    "tp_pct": p.get("tp_pct"), "sl_pct": p.get("sl_pct"),
                    "ev": p.get("ev"), "opened_at": p.get("created_at"),
                })
            return Response(to_json({"positions": result, "total": len(result)}), media_type="application/json")
        else:
            offset = (page - 1) * limit
            positions, total = await asyncio.gather(
                db.get_closed_positions(limit=limit, offset=offset),
                db.get_closed_positions_count(),
            )
            result = [{
                "id": p["id"], "question": p.get("question", ""),
                "theme": p.get("theme", "other"), "side": p["side"],
                "entry_price": p.get("side_price", 0),
                "exit_price": p.get("current_price", 0),
                "stake": p.get("stake_amt", 0), "pnl": p.get("pnl", 0),
                "result": p.get("result", ""), "close_reason": p.get("close_reason", ""),
                "opened_at": p.get("created_at"), "closed_at": p.get("closed_at"),
            } for p in positions]
            return Response(to_json({"positions": result, "total": total, "page": page}), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/analytics")
async def mobile_analytics():
    """Analytics: by theme, by side, daily PnL, calibration, DMA, CLV."""
    try:
        data, clv, dma_weights, all_trades, stats = await asyncio.gather(
            db.get_analytics(),
            db.get_clv_analytics(),
            db.get_dma_weights(),
            db.get_all_closed_trades(),
            db.get_stats(),
        )
        start = config["BANKROLL"]

        sharpe = compute_sharpe_ratio(all_trades)
        drawdown = compute_max_drawdown(all_trades, start)
        streaks = compute_streaks(all_trades)

        return Response(to_json({
            "by_theme": data["by_theme"], "by_side": data["by_side"],
            "by_config": data["by_config"], "daily_pnl": data["daily_pnl"],
            "calibration": data["calibration"],
            "ev_predicted": data["ev_predicted"], "ev_actual": data["ev_actual"],
            "clv": clv, "dma_weights": dma_weights,
            "sharpe": sharpe, "max_drawdown_pct": drawdown["max_dd_pct"],
            "streaks": streaks,
        }), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/daily-pnl")
async def mobile_daily_pnl(days: int = 30):
    """Daily PnL for chart."""
    try:
        data = await db.get_analytics()
        daily = data.get("daily_pnl", [])[:days]
        return Response(to_json({"daily": daily}), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/equity-curve")
async def mobile_equity_curve():
    """Equity curve data for chart."""
    try:
        all_trades = await db.get_all_closed_trades()
        start = config["BANKROLL"]
        equity = compute_equity_curve(all_trades, start)
        return Response(to_json({"equity": equity}), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
