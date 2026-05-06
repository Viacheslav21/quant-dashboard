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


def _serialize_position(p: dict, status: str) -> dict:
    """Uniform shape for open and closed positions. Fields not applicable to a
    given status are returned as null so clients can rely on a single schema."""
    entry = p.get("entry_price", 0) or 0
    current = p.get("current_price") if p.get("current_price") is not None else entry
    pnl_pct = (current - entry) / entry * 100 if entry > 0 else 0
    if status == "open":
        pnl = p.get("unrealized_pnl") or 0
    else:
        pnl = p.get("pnl") or 0
    return {
        "id": p["id"],
        "market_id": p.get("market_id"),
        "question": p.get("question", ""),
        "theme": p.get("theme", "other"),
        "side": p["side"],
        "entry_price": entry,
        "current_price": current,
        "stake": p.get("stake_amt", 0),
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl_pct, 1),
        "status": status,
        "result": p.get("result") if status == "closed" else None,
        "close_reason": p.get("close_reason") if status == "closed" else None,
        "end_date": p.get("end_date") if status == "open" else None,
        "opened_at": p.get("opened_at"),
        "closed_at": p.get("closed_at") if status == "closed" else None,
    }


@router.get("/micro/positions")
async def mobile_micro_positions(status: str = "open", page: int = 1, limit: int = 50):
    """Micro open or closed positions. Open and closed share the same shape:
    uniform position objects (irrelevant fields nulled), `page`/`limit`
    pagination, and a response envelope of {positions, total, page, limit}.
    Past the last page `positions` is empty — clients paginate until empty."""
    try:
        page = max(1, page)
        limit = max(1, min(limit, 500))
        offset = (page - 1) * limit

        if status == "open":
            positions, total_count = await asyncio.gather(
                deps.db.get_micro_open_positions(limit=limit, offset=offset),
                deps.db.get_micro_open_count(),
            )
            result = [_serialize_position(p, "open") for p in positions]
        else:
            positions, total_count = await asyncio.gather(
                deps.db.get_micro_closed_positions(limit=limit, offset=offset),
                deps.db.get_micro_closed_count(),
            )
            result = [_serialize_position(p, "closed") for p in positions]

        return Response(to_json({
            "positions": result,
            "total": total_count,
            "page": page,
            "limit": limit,
        }), media_type="application/json")
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
