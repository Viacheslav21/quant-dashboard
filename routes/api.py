"""Core API routes: micro theme blocking + live config CRUD."""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

import routes.deps as deps
from routes.deps import log, to_json

router = APIRouter(prefix="/api")


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
    """Get all live config for micro service."""
    try:
        configs = await deps.db.get_all_config()
        return Response(to_json([c for c in configs if c.get("service") == "micro"]),
                        media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/config", response_class=JSONResponse)
async def api_config_update(request: Request):
    """Update a single config key or a batch of keys.
    Single: {service, key, value}
    Batch:  {updates: [{service, key, value}, ...]}
    Only `service='micro'` updates are accepted (engine has been removed)."""
    try:
        body = await request.json()
        if "updates" in body:
            updates = body["updates"]
            if not updates:
                return JSONResponse({"error": "empty updates"}, status_code=400)
            results = []
            for u in updates:
                service, key, value = u.get("service"), u.get("key"), u.get("value")
                if service != "micro":
                    return JSONResponse({"error": f"only 'micro' service is supported"}, status_code=400)
                if not all([service, key, value is not None]):
                    return JSONResponse({"error": "service, key, value required in each update"}, status_code=400)
                result = await deps.db.update_config(service, key, str(value))
                results.append({"key": key, **result})
            return JSONResponse({"ok": True, "updated": len(results), "results": results})
        else:
            service = body.get("service")
            key = body.get("key")
            value = body.get("value")
            if service != "micro":
                return JSONResponse({"error": "only 'micro' service is supported"}, status_code=400)
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
    """Recent config changes (micro only)."""
    try:
        history = await deps.db.get_config_live_history(limit=50)
        return Response(to_json([h for h in history if h.get("service") == "micro"]),
                        media_type="application/json")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
