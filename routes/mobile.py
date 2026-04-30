"""Mobile API routes: /api/mobile/micro/*"""

import asyncio
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

import routes.deps as deps
from routes.deps import log, to_json

router = APIRouter(prefix="/api/mobile")


@router.get("/micro/overview")
async def mobile_micro_overview():
    """Micro bot: bankroll, PnL, WR, open positions summary."""
    try:
        stats, open_pos = await asyncio.gather(
            deps.db.get_micro_stats(),
            deps.db.get_micro_open_positions(),
        )
        total = stats["wins"] + stats["losses"]

        themes = {}
        for p in open_pos:
            t = p.get("theme", "other")
            if t not in themes:
                themes[t] = {"count": 0, "staked": 0, "upnl": 0}
            themes[t]["count"] += 1
            themes[t]["staked"] += p.get("stake_amt", 0)
            themes[t]["upnl"] += (p.get("unrealized_pnl") or 0)

        return Response(to_json({
            "bankroll": stats["bankroll"],
            "total_pnl": stats["total_pnl"],
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


@router.get("/micro/positions")
async def mobile_micro_positions(status: str = "open", page: int = 1, limit: int = 50):
    """Micro open or closed positions."""
    try:
        if status == "open":
            positions = await deps.db.get_micro_open_positions()
            result = []
            for p in positions:
                upnl = p.get("unrealized_pnl") or 0
                entry = p.get("entry_price", 0)
                current = p.get("current_price") or entry
                pnl_pct = (current - entry) / entry * 100 if entry > 0 else 0
                result.append({
                    "id": p["id"], "market_id": p["market_id"],
                    "question": p.get("question", ""), "theme": p.get("theme", "other"),
                    "side": p["side"], "entry_price": entry,
                    "current_price": current,
                    "stake": p.get("stake_amt", 0), "upnl": round(upnl, 2),
                    "pnl_pct": round(pnl_pct, 1),
                    "end_date": p.get("end_date"),
                    "opened_at": p.get("opened_at"),
                })
            return Response(to_json({"positions": result, "total": len(result)}), media_type="application/json")
        else:
            offset = (page - 1) * limit
            positions = await deps.db.get_micro_closed_positions(limit=limit, offset=offset)
            total_count = await deps.db.get_micro_closed_count()
            result = [{
                "id": p["id"], "question": p.get("question", ""),
                "theme": p.get("theme", "other"), "side": p["side"],
                "entry_price": p.get("entry_price", 0),
                "current_price": p.get("current_price", 0),
                "stake": p.get("stake_amt", 0), "pnl": p.get("pnl", 0),
                "result": p.get("result", ""), "close_reason": p.get("close_reason", ""),
                "opened_at": p.get("opened_at"), "closed_at": p.get("closed_at"),
            } for p in positions]
            return Response(to_json({"positions": result, "total": total_count, "page": page}), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/micro/daily-pnl")
async def mobile_micro_daily_pnl(days: int = 30):
    """Micro daily PnL for chart."""
    try:
        data = await deps.db.get_micro_analytics()
        daily = data.get("daily_pnl", [])[:days]
        return Response(to_json({"daily": daily}), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/micro/themes")
async def mobile_micro_themes():
    """Micro themes with trade stats and blocked status."""
    try:
        themes = await deps.db.get_micro_themes()
        return Response(to_json({"themes": themes}), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/micro/theme-block")
async def mobile_micro_theme_block(request: Request):
    """Block or unblock a micro theme. Body: {theme, blocked}."""
    try:
        body = await request.json()
        theme = body.get("theme")
        blocked = body.get("blocked", True)
        if not theme:
            return JSONResponse({"error": "theme required"}, status_code=400)
        await deps.db.set_micro_theme_blocked(theme, blocked)
        return JSONResponse({"ok": True, "theme": theme, "blocked": blocked})
    except Exception as e:
        log.error(f"[CMD] micro theme block failed: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
