"""Quant Dashboard — FastAPI application.

Slim orchestrator: startup, middleware, auth, template config.
Routes are in routes/ package."""

import os
import hmac
import hashlib
import logging
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
import jinja2 as _jinja2
import uvicorn

from utils.db import Database
from utils.helpers import pc, wr_color, to_json

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("dashboard")

# ── Config ──

_config = {
    "BANKROLL":         float(os.getenv("BANKROLL", "1000")),
    "MIN_EV":           float(os.getenv("MIN_EV", "0.12")),
    "MIN_KL":           float(os.getenv("MIN_KL", "0.08")),
    "MAX_KELLY_FRAC":   float(os.getenv("MAX_KELLY_FRAC", "0.20")),
    "TAKE_PROFIT_PCT":  float(os.getenv("TAKE_PROFIT_PCT", "0.15")),
    "STOP_LOSS_PCT":    float(os.getenv("STOP_LOSS_PCT", "0.25")),
}

DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")
API_SECRET = os.getenv("API_SECRET", "")


# ── App + Templates ──

@asynccontextmanager
async def lifespan(application):
    db = Database(os.getenv("DATABASE_URL"))
    await db.init()
    log.info(f"[DASHBOARD] Ready on port {os.getenv('PORT', '3000')}")

    # Init shared deps for all route modules
    import routes.deps as deps
    deps.init(db, _config, templates)

    yield
    await db.close()

app = FastAPI(lifespan=lifespan)

_env = _jinja2.Environment(
    loader=_jinja2.FileSystemLoader("templates"),
    autoescape=True,
    cache_size=400,
)
templates = Jinja2Templates(directory="templates")
templates.env = _env


# ── Auth ──

def _hash_token(token: str) -> str:
    return hmac.new(b"quant-dash", token.encode(), hashlib.sha256).hexdigest()


def _check_auth(request: Request) -> bool:
    if not DASHBOARD_TOKEN:
        return True
    cookie = request.cookies.get("session_token", "")
    if cookie and hmac.compare_digest(cookie, _hash_token(DASHBOARD_TOKEN)):
        return True
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer ") and hmac.compare_digest(auth[7:], DASHBOARD_TOKEN):
        return True
    if request.query_params.get("token") == DASHBOARD_TOKEN:
        return True
    return False


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in ("/login", "/favicon.ico"):
            return await call_next(request)
        if not _check_auth(request):
            if path.startswith("/api"):
                return JSONResponse({"error": "Unauthorized"}, status_code=401)
            return RedirectResponse(url="/login", status_code=302)
        return await call_next(request)


app.add_middleware(AuthMiddleware)


# ── Auth routes (kept here — they use app-level state) ──

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = None):
    if not DASHBOARD_TOKEN:
        return RedirectResponse(url="/", status_code=302)
    if _check_auth(request):
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(request, "login.html", {"error": error})


@app.post("/login")
async def login_submit(request: Request):
    try:
        form = await request.form()
        token = str(form.get("token", "") or "")
        if token and hmac.compare_digest(token, DASHBOARD_TOKEN):
            response = RedirectResponse(url="/", status_code=302)
            response.set_cookie(
                "session_token", _hash_token(DASHBOARD_TOKEN),
                max_age=30 * 24 * 3600, httponly=True, samesite="lax",
            )
            log.info("[AUTH] Login successful")
            return response
    except Exception as e:
        log.error(f"[AUTH] Login error: {e}")
    log.warning(f"[AUTH] Failed login attempt from {request.client.host}")
    return RedirectResponse(url="/login?error=invalid", status_code=302)


@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("session_token")
    response.delete_cookie("api_secret")
    return response


@app.post("/api/auth/api-secret")
async def verify_api_secret(request: Request):
    body = await request.json()
    secret = body.get("secret", "")
    if not API_SECRET:
        return JSONResponse({"ok": True})
    if hmac.compare_digest(secret, API_SECRET):
        response = JSONResponse({"ok": True})
        response.set_cookie(
            "api_secret", _hash_token(API_SECRET),
            max_age=30 * 24 * 3600, httponly=True, samesite="lax",
        )
        return response
    return JSONResponse({"error": "Invalid API secret"}, status_code=403)


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


# ── Register route modules ──

from routes.pages import router as pages_router
from routes.api import router as api_router
from routes.mobile import router as mobile_router
from routes.audit import router as audit_router
from routes.ml_proxy import router as ml_proxy_router

app.include_router(pages_router)
app.include_router(api_router)
app.include_router(mobile_router)
app.include_router(audit_router)
app.include_router(ml_proxy_router)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "3000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
