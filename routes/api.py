"""Core API routes: /api, /api/commands/close, /api/export, /api/diagnostics."""

import io
import csv
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

import routes.deps as deps
from routes.deps import parse_date, log, to_json
from utils.helpers import _json_serial

router = APIRouter(prefix="/api")


@router.get("")
async def api_stats():
    try:
        stats = await deps.db.get_stats()
        open_ = await deps.db.get_open_positions()
        closed = await deps.db.get_closed_positions(limit=5)
        return Response(to_json({"stats": stats, "open": len(open_), "recent": len(closed)}), media_type="application/json")
    except Exception as e:
        log.warning(f"[DASHBOARD] API error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/commands/close", response_class=JSONResponse)
async def cmd_close_position(request: Request):
    """Insert a close_position command into trader_commands table."""
    try:
        body = await request.json()
        position_id = body.get("position_id")
        if not position_id:
            return JSONResponse({"error": "position_id required"}, status_code=400)
        async with deps.db.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO trader_commands (command, position_id, params)
                VALUES ('close_position', $1, '{}')
                RETURNING id
            """, position_id)
            await conn.execute(f"NOTIFY trader_commands, '{row['id']}'")

        return JSONResponse({"ok": True, "command_id": row["id"]})
    except Exception as e:
        log.error(f"[CMD] close command failed: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/export/positions")
async def export_positions(date_from: str = None, date_to: str = None):
    """CSV export of closed positions."""
    df = parse_date(date_from)
    dt = parse_date(date_to)
    rows = await deps.db.get_positions_for_export(df, dt)
    output = io.StringIO()
    if rows:
        fields = list(rows[0].keys())
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: _json_serial(v) if not isinstance(v, (str, int, float, type(None))) else v for k, v in r.items()})
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=positions.csv"},
    )


@router.get("/diagnostics")
async def api_diagnostics():
    """Deep WR diagnostics."""
    try:
        diag = await deps.db.get_wr_diagnostics()
        return Response(to_json(diag), media_type="application/json")
    except Exception as e:
        log.warning(f"[DASHBOARD] Diagnostics error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/themes")
async def api_themes():
    """Theme calibration data with blocked status."""
    try:
        themes = await deps.db.get_theme_patterns()
        return Response(to_json(themes), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/commands/theme-block", response_class=JSONResponse)
async def cmd_theme_block(request: Request):
    """Block or unblock a theme for engine trading."""
    try:
        body = await request.json()
        theme = body.get("theme")
        blocked = body.get("blocked", True)
        if not theme:
            return JSONResponse({"error": "theme required"}, status_code=400)
        await deps.db.set_theme_blocked(theme, blocked)
        return JSONResponse({"ok": True, "theme": theme, "blocked": blocked})
    except Exception as e:
        log.error(f"[CMD] theme block failed: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/commands/micro-theme-block", response_class=JSONResponse)
async def cmd_micro_theme_block(request: Request):
    """Block or unblock a theme for micro trading."""
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


@router.get("/config")
async def api_config():
    """Get all live config for both services."""
    try:
        configs = await deps.db.get_all_config()
        return Response(to_json(configs), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/config", response_class=JSONResponse)
async def api_config_update(request: Request):
    """Update a single config key."""
    try:
        body = await request.json()
        service = body.get("service")
        key = body.get("key")
        value = body.get("value")
        if not all([service, key, value is not None]):
            return JSONResponse({"error": "service, key, value required"}, status_code=400)
        result = await deps.db.update_config(service, key, str(value))
        return JSONResponse(result)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        log.error(f"[CONFIG] Update failed: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/config/history")
async def api_config_history():
    """Recent config changes."""
    try:
        history = await deps.db.get_config_history(limit=50)
        return Response(to_json(history), media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
