import os
import io
import csv
import json
import hmac
import hashlib
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, Cookie
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
import uvicorn

from utils.db import Database
from utils.helpers import pc, wr_color, to_json, _json_serial
from utils.metrics import (
    compute_sharpe_ratio, compute_max_drawdown,
    compute_streaks, compute_equity_curve, compute_pnl_distribution,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("dashboard")

@asynccontextmanager
async def lifespan(application):
    global _db
    _db = Database(os.getenv("DATABASE_URL"))
    await _db.init()
    log.info(f"[DASHBOARD] Ready on port {os.getenv('PORT', '3000')}")
    yield
    await _db.close()

app = FastAPI(lifespan=lifespan)
import jinja2 as _jinja2
_env = _jinja2.Environment(
    loader=_jinja2.FileSystemLoader("templates"),
    autoescape=True,
    cache_size=0,
)
templates = Jinja2Templates(directory="templates")
templates.env = _env


def _ctx(**kwargs) -> dict:
    """Base template context with helpers and auth flag."""
    return {
        "pc": pc,
        "wr_color": wr_color,
        "to_json": to_json,
        "auth_enabled": bool(DASHBOARD_TOKEN),
        **kwargs,
    }

_db = None
_config = {
    "ANTHROPIC_KEY":    os.getenv("ANTHROPIC_API_KEY"),
    "BANKROLL":         float(os.getenv("BANKROLL", "1000")),
    "MIN_EV":           float(os.getenv("MIN_EV", "0.12")),
    "MIN_KL":           float(os.getenv("MIN_KL", "0.10")),
    "MAX_KELLY_FRAC":   float(os.getenv("MAX_KELLY_FRAC", "0.15")),
    "TAKE_PROFIT_PCT":  float(os.getenv("TAKE_PROFIT_PCT", "0.20")),
    "STOP_LOSS_PCT":    float(os.getenv("STOP_LOSS_PCT", "0.30")),
}

DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")
API_SECRET = os.getenv("API_SECRET", "")

# ── Auth ──

def _hash_token(token: str) -> str:
    """Hash token for secure cookie comparison."""
    return hmac.new(b"quant-dash", token.encode(), hashlib.sha256).hexdigest()


def _check_auth(request: Request) -> bool:
    """Check if request is authenticated. Returns True if no token is set (disabled)."""
    if not DASHBOARD_TOKEN:
        return True
    # Check cookie
    cookie = request.cookies.get("session_token", "")
    if cookie and hmac.compare_digest(cookie, _hash_token(DASHBOARD_TOKEN)):
        return True
    # Check Authorization header
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer ") and hmac.compare_digest(auth[7:], DASHBOARD_TOKEN):
        return True
    # Check query param
    if request.query_params.get("token") == DASHBOARD_TOKEN:
        return True
    return False


def _check_api_secret(request: Request) -> bool:
    """Check API secret for expensive operations. Returns True if no secret is set."""
    if not API_SECRET:
        return True
    # Check header
    secret = request.headers.get("x-api-secret", "")
    if secret and hmac.compare_digest(secret, API_SECRET):
        return True
    # Check cookie
    cookie = request.cookies.get("api_secret", "")
    if cookie and hmac.compare_digest(cookie, _hash_token(API_SECRET)):
        return True
    return False


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # Login page is always accessible
        if path in ("/login", "/favicon.ico"):
            return await call_next(request)
        # Check auth
        if not _check_auth(request):
            if path.startswith("/api"):
                return JSONResponse({"error": "Unauthorized"}, status_code=401)
            return RedirectResponse(url="/login", status_code=302)
        return await call_next(request)


app.add_middleware(AuthMiddleware)


# ── Login ──

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
                max_age=30 * 24 * 3600,  # 30 days
                httponly=True, samesite="lax",
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
    """Verify API secret and set cookie for future requests."""
    body = await request.json()
    secret = body.get("secret", "")
    if not API_SECRET:
        return JSONResponse({"ok": True})
    if hmac.compare_digest(secret, API_SECRET):
        response = JSONResponse({"ok": True})
        response.set_cookie(
            "api_secret", _hash_token(API_SECRET),
            max_age=30 * 24 * 3600,
            httponly=True, samesite="lax",
        )
        return response
    return JSONResponse({"error": "Invalid API secret"}, status_code=403)


def _parse_date(s):
    """Parse date string from query param, return ISO string or None."""
    if not s:
        return None
    try:
        return s.strip() + "T00:00:00+00:00" if "T" not in s else s
    except Exception:
        return None


# ── Dashboard ──

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, page: int = 1, date_from: str = None, date_to: str = None):
    try:
        per_page = 20
        df = _parse_date(date_from)
        dt = _parse_date(date_to)

        stats = await _db.get_stats()
        open_ = await _db.get_open_positions()
        total_closed = await _db.get_closed_positions_count(df, dt)
        total_pages = max(1, (total_closed + per_page - 1) // per_page)
        closed = await _db.get_closed_positions(limit=per_page, offset=(page - 1) * per_page, date_from=df, date_to=dt)
        signals = await _db.get_recent_signals(limit=10)
        pnl_data = await _db.get_cumulative_pnl()

        # Advanced metrics
        all_trades = await _db.get_all_closed_trades()
        rolling = await _db.get_rolling_performance()
        best_worst = await _db.get_best_worst_trades()

        start = _config["BANKROLL"]
        roi = ((stats["bankroll"] - start) / start * 100) if start > 0 else 0
        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0
        mode = "Simulation" if (_config or {}).get("SIMULATION", True) else "Live"

        sharpe = compute_sharpe_ratio(all_trades)
        drawdown = compute_max_drawdown(all_trades, start)
        streaks = compute_streaks(all_trades)
        equity = compute_equity_curve(all_trades, start)

        open_in_profit = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) >= 0)
        open_in_loss = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) < 0)
        open_total_upnl = sum((p.get("unrealized_pnl") or 0) for p in open_)

        return templates.TemplateResponse(request, "dashboard.html", _ctx(
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


# ── Trader Commands ──

@app.post("/api/commands/close", response_class=JSONResponse)
async def cmd_close_position(request: Request):
    """Insert a close_position command into trader_commands table."""
    try:
        body = await request.json()
        position_id = body.get("position_id")
        if not position_id:
            return JSONResponse({"error": "position_id required"}, status_code=400)
        async with _db.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO trader_commands (command, position_id, params)
                VALUES ('close_position', $1, '{}')
                RETURNING id
            """, position_id)
            # NOTIFY for instant pickup by engine
            await conn.execute(f"NOTIFY trader_commands, '{row['id']}'")

        return JSONResponse({"ok": True, "command_id": row["id"]})
    except Exception as e:
        log.error(f"[CMD] close command failed: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Analytics ──

@app.get("/analytics", response_class=HTMLResponse)
async def analytics(request: Request, date_from: str = None, date_to: str = None):
    try:
        data = await _db.get_analytics()
        pnl_data = await _db.get_cumulative_pnl()
        sig_outcomes = await _db.get_signal_outcomes(limit=50)
        market_metrics = await _db.get_all_market_metrics(limit=50)
        config_hist = await _db.get_config_history()
        config_map = {c["tag"]: c["params"] for c in config_hist}
        stats = await _db.get_stats()

        # Advanced metrics
        all_trades = await _db.get_all_closed_trades()
        rolling = await _db.get_rolling_performance()
        best_worst = await _db.get_best_worst_trades()

        start = _config["BANKROLL"]
        roi = ((stats["bankroll"] - start) / start * 100) if start > 0 else 0
        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0

        sharpe = compute_sharpe_ratio(all_trades)
        drawdown = compute_max_drawdown(all_trades, start)
        equity = compute_equity_curve(all_trades, start)
        pnl_dist = compute_pnl_distribution(all_trades)
        clv = await _db.get_clv_analytics()
        dma_weights = await _db.get_dma_weights()

        ev_pred = data["ev_predicted"] * 100
        ev_act = data["ev_actual"] * 100

        # Config A/B rows
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

        # Signal backtest stats
        valid_sigs = [s for s in sig_outcomes if s.get("price_move") is not None]
        exec_sigs = [s for s in valid_sigs if s["executed"]]
        rej_sigs = [s for s in valid_sigs if not s["executed"]]
        exec_right = sum(1 for s in exec_sigs if s.get("price_move") and s["price_move"] > 0)
        rej_right = sum(1 for s in rej_sigs if s.get("price_move") and s["price_move"] > 0)
        rej_saved = sum(1 for s in rej_sigs if not (s.get("price_move") and s["price_move"] > 0))

        return templates.TemplateResponse(request, "analytics.html", _ctx(
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
            rej_right=rej_right, rej_saved=rej_saved, clv=clv, dma_weights=dma_weights,
            date_from=date_from, date_to=date_to,
            has_api_secret=_check_api_secret(request),
            api_secret_required=bool(API_SECRET),
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Analytics error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Analytics Error</h1><pre>{e}</pre>", status_code=500)


# ── Scalping (micro) ──

@app.get("/scalping", response_class=HTMLResponse)
async def scalping(request: Request, page: int = 1):
    try:
        per_page = 20
        stats = await _db.get_micro_stats()
        open_ = await _db.get_micro_open_positions()
        total_closed = stats["wins"] + stats["losses"]
        total_pages = max(1, (total_closed + per_page - 1) // per_page)
        closed = await _db.get_micro_closed_positions(limit=per_page, offset=(page - 1) * per_page)
        pnl_data = await _db.get_micro_cumulative_pnl()
        data = await _db.get_micro_analytics()

        micro_bankroll = 500.0  # starting bankroll
        roi = ((stats["bankroll"] - micro_bankroll) / micro_bankroll * 100) if micro_bankroll > 0 else 0
        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0

        open_in_profit = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) >= 0)
        open_in_loss = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) < 0)
        open_total_upnl = sum((p.get("unrealized_pnl") or 0) for p in open_)

        return templates.TemplateResponse(request, "scalping.html", _ctx(
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


# ── ML Model ──

@app.get("/model", response_class=HTMLResponse)
async def model_page(request: Request):
    try:
        import httpx
        ml_url = _config.get("ML_API_URL") or os.getenv("ML_API_URL", "")
        health_data = {}
        metrics = {}

        if ml_url:
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    r = await client.get(f"{ml_url}/health")
                    health_data = r.json()
            except Exception as e:
                health_data = {"status": "offline", "error": str(e)}

            # Get model metrics from DB
            try:
                async with _db.pool.acquire() as conn:
                    row = await conn.fetchrow("SELECT metrics FROM ml_models WHERE id='main'")
                    if row and row["metrics"]:
                        m = row["metrics"]
                        if isinstance(m, str):
                            metrics = json.loads(m)
                        else:
                            metrics = dict(m)
            except Exception:
                pass

        return templates.TemplateResponse(request, "model.html", _ctx(
            active_page="model",
            health=health_data,
            metrics=metrics,
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Model page error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Model Error</h1><pre>{e}</pre>", status_code=500)


# ── ML API Proxy (browser can't reach internal Railway URLs) ──

@app.post("/api/ml/train")
async def proxy_ml_train():
    import httpx
    ml_url = _config.get("ML_API_URL") or os.getenv("ML_API_URL", "")
    if not ml_url:
        return JSONResponse({"error": "ML_API_URL not configured"}, status_code=500)
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{ml_url}/api/train")
        return JSONResponse(r.json(), status_code=r.status_code)

@app.post("/api/ml/train-only")
async def proxy_ml_train_only():
    import httpx
    ml_url = _config.get("ML_API_URL") or os.getenv("ML_API_URL", "")
    if not ml_url:
        return JSONResponse({"error": "ML_API_URL not configured"}, status_code=500)
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{ml_url}/api/train-only")
        return JSONResponse(r.json(), status_code=r.status_code)

@app.get("/api/ml/training-status")
async def proxy_ml_status():
    import httpx
    ml_url = _config.get("ML_API_URL") or os.getenv("ML_API_URL", "")
    if not ml_url:
        return JSONResponse({"error": "ML_API_URL not configured"}, status_code=500)
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.get(f"{ml_url}/api/training-status")
        return JSONResponse(r.json())

@app.get("/api/ml/health")
async def proxy_ml_health():
    import httpx
    ml_url = _config.get("ML_API_URL") or os.getenv("ML_API_URL", "")
    if not ml_url:
        return JSONResponse({"error": "ML_API_URL not configured"}, status_code=500)
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.get(f"{ml_url}/health")
        return JSONResponse(r.json())


@app.get("/api/system-audit")
async def system_audit():
    """Full system audit — comprehensive data dump for analysis."""
    try:
        stats = await _db.get_stats()
        open_pos = await _db.get_open_positions()
        all_trades = await _db.get_all_closed_trades()
        analytics = await _db.get_analytics()
        diagnostics = await _db.get_wr_diagnostics()
        clv = await _db.get_clv_analytics()
        dma = await _db.get_dma_weights()
        rolling = await _db.get_rolling_performance()
        best_worst = await _db.get_best_worst_trades()
        signals = await _db.get_recent_signals(limit=30)
        sig_outcomes = await _db.get_signal_outcomes(limit=50)
        market_metrics = await _db.get_all_market_metrics(limit=50)
        config_hist = await _db.get_config_history()
        start = _config["BANKROLL"]

        sharpe = compute_sharpe_ratio(all_trades)
        drawdown = compute_max_drawdown(all_trades, start)
        streaks = compute_streaks(all_trades)

        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0
        roi = ((stats["bankroll"] - start) / start * 100) if start > 0 else 0

        # Build structured text report
        lines = []
        diag = diagnostics
        wl = diag.get("win_loss_size", {})
        breakeven = diag.get('breakeven_wr', 0)

        lines.append("=" * 60)
        lines.append("QUANT ENGINE — SYSTEM AUDIT")
        lines.append("=" * 60)

        # ━━━ 1. HEALTH CHECK ━━━
        lines.append("\n" + "━" * 40)
        lines.append("1. HEALTH CHECK")
        lines.append("━" * 40)
        lines.append(f"Bank: ${stats['bankroll']:.0f} (start ${start:.0f}) | ROI: {roi:+.1f}% | P&L: ${stats['total_pnl']:+.0f}")
        lines.append(f"WR: {wr}% ({stats['wins']}W/{stats['losses']}L/{total}) | Breakeven: {breakeven}% | Gap: {wr - breakeven:+.1f}%")
        lines.append(f"7d: {rolling['pnl_7d']:+.0f}$ ({rolling['trades_7d']} trades) | 30d: {rolling['pnl_30d']:+.0f}$ ({rolling['trades_30d']} trades)")
        lines.append(f"Sharpe: {sharpe:.2f} | MaxDD: -{drawdown['max_dd_pct']:.1f}% | Avg lifetime: {analytics['avg_lifetime_hours']:.0f}h")
        lines.append(f"EV predicted: +{analytics['ev_predicted']*100:.1f}% | EV actual: {analytics['ev_actual']*100:+.1f}%")
        lines.append(f"Avg win: ${wl.get('avg_win', 0)} ({(wl.get('avg_win_pct') or 0)*100:.0f}%) | Avg loss: ${wl.get('avg_loss', 0)} ({(wl.get('avg_loss_pct') or 0)*100:.0f}%)")
        lines.append(f"Streaks — Current: {streaks['cur_win']}W/{streaks['cur_loss']}L | Max: {streaks['max_win']}W/{streaks['max_loss']}L")

        # Alerts
        alerts = []
        if wr < breakeven:
            alerts.append(f"WR {wr}% below breakeven {breakeven}% (gap {wr - breakeven:+.1f}%)")
        if rolling['pnl_7d'] < -30:
            alerts.append(f"7d P&L: {rolling['pnl_7d']:+.0f}$ (heavy losses)")
        if drawdown['max_dd_pct'] > 8:
            alerts.append(f"Max drawdown {drawdown['max_dd_pct']:.1f}% > 8% threshold")
        # Theme alerts from patterns
        try:
            async with _db.pool.acquire() as _aconn:
                _bad_themes = await _aconn.fetch("""
                    SELECT category, trade_wr, ev_mult FROM patterns
                    WHERE trade_n >= 10 AND trade_wr < 0.40 ORDER BY trade_wr ASC
                """)
                for bt in _bad_themes:
                    alerts.append(f"Theme '{bt['category']}' WR={float(bt['trade_wr'])*100:.0f}% ev_mult={float(bt['ev_mult']):.1f}")
        except Exception:
            pass
        if alerts:
            lines.append(f"\nALERTS:")
            for a in alerts:
                lines.append(f"  ! {a}")
        else:
            lines.append(f"\nNo alerts.")

        # ━━━ 2. PERFORMANCE ━━━
        lines.append("\n" + "━" * 40)
        lines.append("2. PERFORMANCE")
        lines.append("━" * 40)

        lines.append(f"\nDaily P&L (last 14d):")
        for r in analytics["daily_pnl"]:
            d_wr = round(r['wins'] / r['trades'] * 100, 1) if r['trades'] > 0 else 0
            lines.append(f"  {r['day']}: {r['pnl']:+.2f}$ ({r['trades']} trades, WR={d_wr}%)")

        lines.append(f"\nConfig A/B:")
        for r in sorted(analytics["by_config"], key=lambda x: x['total_pnl'], reverse=True):
            c_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
            lines.append(f"  {r['config_tag']}: {r['wins']}/{r['total']} ({c_wr}%) pnl={r['total_pnl']:+.0f}$ avg={r['avg_pnl']:+.2f}$ stake=${r['avg_stake']:.0f}")

        lines.append(f"\nBy Theme:")
        for r in analytics["by_theme"]:
            t_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
            flag = " !" if t_wr < 40 and r['total'] >= 10 else ""
            lines.append(f"  {r['theme']}: {r['wins']}/{r['total']} ({t_wr}%) avg={r['avg_pnl']:+.2f}${flag}")

        lines.append(f"\nBy Side:")
        for r in analytics["by_side"]:
            s_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
            lines.append(f"  {r['side']}: {r['wins']}/{r['total']} ({s_wr}%) avg={r['avg_pnl']:+.2f}$")

        lines.append(f"\nClose Reasons:")
        for r in diag.get("close_reasons", []):
            lines.append(f"  {r['event_type']}: {r['total']} trades, avg={r['avg_pnl']:+.2f}$, total={r['total_pnl']:+.0f}$")

        if best_worst.get("best"):
            lines.append(f"\nBest:  +{best_worst['best']['pnl']:.2f}$ — {best_worst['best']['question'][:60]}")
        if best_worst.get("worst"):
            lines.append(f"Worst: {best_worst['worst']['pnl']:.2f}$ — {best_worst['worst']['question'][:60]}")

        # ━━━ 3. RISK ━━━
        lines.append("\n" + "━" * 40)
        lines.append("3. RISK & PORTFOLIO")
        lines.append("━" * 40)

        # Portfolio concentration
        try:
            async with _db.pool.acquire() as conn:
                theme_conc = await conn.fetch("""
                    SELECT theme, COUNT(*) as cnt, ROUND(SUM(stake_amt)::numeric, 0) as total_stake,
                           ROUND(SUM(unrealized_pnl)::numeric, 2) as total_upnl
                    FROM positions WHERE status='open'
                    GROUP BY theme ORDER BY SUM(stake_amt) DESC
                """)
                if theme_conc:
                    lines.append(f"\nPortfolio ({len(open_pos)} positions):")
                    for r in theme_conc:
                        lines.append(f"  {r['theme']}: {r['cnt']} pos, ${r['total_stake']} staked, uPnL={float(r['total_upnl'] or 0):+.2f}$")

                # Correlated loss events
                corr_losses = await conn.fetch("""
                    SELECT DATE_TRUNC('hour', created_at) as hour,
                           COUNT(*) as sl_count,
                           ROUND(SUM((details->>'pnl')::numeric), 2) as total_pnl,
                           ARRAY_AGG(DISTINCT details->>'theme') as themes
                    FROM trade_log
                    WHERE event_type = 'CLOSE_SL' AND created_at > NOW() - INTERVAL '7 days'
                    GROUP BY DATE_TRUNC('hour', created_at)
                    HAVING COUNT(*) >= 3
                    ORDER BY SUM((details->>'pnl')::numeric) ASC LIMIT 5
                """)
                if corr_losses:
                    lines.append(f"\nCorrelated Losses (3+ SL in 1h):")
                    for r in corr_losses:
                        themes = [t for t in (r['themes'] or []) if t]
                        pnl_str = f"{r['total_pnl']:+.0f}$" if r['total_pnl'] is not None else "?$"
                        lines.append(f"  {r['hour'].strftime('%m-%d %H:00')}: {r['sl_count']} SLs, {pnl_str} | {', '.join(themes)}")

                # Expired positions
                expired_list = []
                for p in open_pos:
                    q = p.get("question", "")
                    import re as _re
                    _m = _re.search(r'(?:on|by|before)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:,?\s+(\d{4}))?', q, _re.IGNORECASE)
                    if _m:
                        from datetime import datetime as _dt, timezone as _tz
                        _ms, _ds, _ys = _m.group(1), _m.group(2), _m.group(3)
                        _yr = int(_ys) if _ys else _dt.now(_tz.utc).year
                        try:
                            _qd = _dt.strptime(f"{_ms} {_ds} {_yr}", "%B %d %Y").replace(tzinfo=_tz.utc)
                            _da = (_dt.now(_tz.utc) - _qd).days
                            if _da > 1:
                                expired_list.append(f"  ! {q[:60]} ({_da}d ago, ${p['stake_amt']:.0f})")
                        except ValueError:
                            pass
                if expired_list:
                    lines.append(f"\nExpired Open Positions:")
                    lines.extend(expired_list)
        except Exception as e:
            lines.append(f"\n  Risk section error: {e}")

        # ━━━ 4. SIGNAL QUALITY ━━━
        lines.append("\n" + "━" * 40)
        lines.append("4. SIGNAL QUALITY")
        lines.append("━" * 40)

        lines.append(f"\nCalibration (resolved only):")
        for r in analytics["calibration"]:
            lines.append(f"  {r['bucket']}: {r['total']} trades, predicted={float(r['avg_predicted'])*100:.1f}%, actual={float(r['actual_wr'])*100:.1f}%")

        lines.append(f"\nDMA Weights:")
        if dma:
            for w in sorted(dma, key=lambda x: -x['weight']):
                total = w.get('hits', 0) + w.get('misses', 0)
                acc = f"{w['hits']}/{total}" if total > 0 else "no data"
                lines.append(f"  {w['source']:15s} w={w['weight']:.2f} | {acc}")
        try:
            async with _db.pool.acquire() as conn:
                dma_diag = await conn.fetchrow("""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN p.signal_id IS NOT NULL THEN 1 ELSE 0 END) as has_signal_id
                    FROM (SELECT * FROM positions WHERE status = 'closed' AND result IS NOT NULL ORDER BY closed_at DESC LIMIT 200) p
                    LEFT JOIN trade_log tl ON tl.signal_id = p.signal_id AND tl.event_type = 'SIGNAL_GENERATED'
                """)
                lines.append(f"  DMA data: {dma_diag['has_signal_id']}/{dma_diag['total']} positions with source details")
        except Exception:
            pass

        lines.append(f"\nCLV: 1h={clv['avg_clv_1h']:+.0f}% 4h={clv['avg_clv_4h']:+.0f}% 24h={clv['avg_clv_24h']:+.0f}% close={clv['avg_clv_close']:+.0f}% | Positive: {clv['positive_clv_pct']}%")

        # Evidence source accuracy
        try:
            async with _db.pool.acquire() as conn:
                source_accuracy = await conn.fetch("""
                    SELECT key as source, COUNT(*) as total,
                           SUM(CASE WHEN correct THEN 1 ELSE 0 END) as hits,
                           ROUND(AVG(CASE WHEN correct THEN 1.0 ELSE 0.0 END)::numeric, 3) as accuracy
                    FROM (
                        SELECT key, CASE
                                WHEN p.side = 'YES' AND (value::float > 0.5) = (p.result = 'WIN') THEN true
                                WHEN p.side = 'NO' AND (value::float < 0.5) = (p.result = 'WIN') THEN true
                                ELSE false END as correct
                        FROM (SELECT p2.id, p2.side, p2.result, tl.details
                              FROM positions p2
                              JOIN trade_log tl ON tl.signal_id = p2.signal_id AND tl.event_type = 'SIGNAL_GENERATED'
                              WHERE p2.status = 'closed' AND p2.result IS NOT NULL AND tl.details IS NOT NULL) p,
                        jsonb_each_text(p.details) kv
                        WHERE kv.key IN ('p_history','p_momentum','p_long_mom','p_contrarian','p_vol_trend','p_arb','p_book','p_flb')
                            AND kv.value IS NOT NULL AND kv.value != 'null' AND kv.value::float > 0 AND kv.value::float < 1
                    ) sub GROUP BY key HAVING COUNT(*) >= 5 ORDER BY accuracy DESC
                """)
                if source_accuracy:
                    lines.append(f"\nSource Accuracy:")
                    for r in source_accuracy:
                        lines.append(f"  {r['source']:15s} {r['hits']}/{r['total']} ({float(r['accuracy'])*100:.0f}%)")
        except Exception:
            pass

        lines.append(f"\nSignal Backtest (last 50):")
        valid_sigs = [s for s in sig_outcomes if s.get("price_move") is not None]
        exec_sigs = [s for s in valid_sigs if s["executed"]]
        rej_sigs = [s for s in valid_sigs if not s["executed"]]
        exec_right = sum(1 for s in exec_sigs if s.get("price_move") and s["price_move"] > 0)
        rej_saved = sum(1 for s in rej_sigs if not (s.get("price_move") and s["price_move"] > 0))
        lines.append(f"  Executed: {exec_right}/{len(exec_sigs)} correct ({round(exec_right/len(exec_sigs)*100) if exec_sigs else 0}%) | Rejected: {rej_saved}/{len(rej_sigs)} correctly avoided")

        # ━━━ 5. DIAGNOSTICS ━━━
        lines.append("\n" + "━" * 40)
        lines.append("5. DIAGNOSTICS")
        lines.append("━" * 40)

        lines.append(f"\nWR by EV:")
        for r in diag.get("ev_buckets", []):
            b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
            lines.append(f"  EV {r['ev_bucket']}: {r['wins']}/{r['total']} ({b_wr}%) total={r['total_pnl']:+.0f}$")

        lines.append(f"\nWR by Lifetime:")
        for r in diag.get("lifetime_wr", []):
            b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
            lines.append(f"  {r['lifetime']}: {r['wins']}/{r['total']} ({b_wr}%) avg={r['avg_pnl']:+.2f}$")

        lines.append(f"\nWR by Stake:")
        for r in diag.get("stake_wr", []):
            b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
            lines.append(f"  {r['stake_bucket']}: {r['wins']}/{r['total']} ({b_wr}%) total={r['total_pnl']:+.0f}$")

        lines.append(f"\nTP/SL Distribution:")
        for r in diag.get("tp_sl_dist", []):
            b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
            lines.append(f"  TP={r['tp']} SL={r['sl']}: {r['wins']}/{r['total']} ({b_wr}%) avg={r['avg_pnl']:+.2f}$")

        # Grace period, theme momentum, signal journey, hourly, liquidity
        try:
            async with _db.pool.acquire() as conn:
                grace_stats = await conn.fetch("""
                    SELECT CASE
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at))/3600 < 1 THEN 'stopped_<1h'
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at))/3600 BETWEEN 1 AND 3 THEN 'survived_1-3h'
                        ELSE 'survived_3h+' END as bucket,
                        COUNT(*) as total, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(SUM(pnl)::numeric, 2) as total_pnl
                    FROM positions WHERE status='closed' AND result IS NOT NULL AND closed_at IS NOT NULL
                    GROUP BY bucket ORDER BY bucket
                """)
                if grace_stats:
                    lines.append(f"\nGrace Period:")
                    for r in grace_stats:
                        if r['bucket'] and r['total'] > 0:
                            g_wr = round(r['wins'] / r['total'] * 100, 1)
                            lines.append(f"  {r['bucket']}: {r['wins']}/{r['total']} ({g_wr}%) total={r['total_pnl']:+.0f}$")

                theme_momentum = await conn.fetch("""
                    WITH recent AS (
                        SELECT theme, COUNT(*) as n, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END)::float/NULLIF(COUNT(*),0) as wr
                        FROM positions WHERE status='closed' AND result IS NOT NULL AND closed_at > NOW()-INTERVAL '7 days'
                        GROUP BY theme HAVING COUNT(*) >= 3),
                    previous AS (
                        SELECT theme, COUNT(*) as n, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END)::float/NULLIF(COUNT(*),0) as wr
                        FROM positions WHERE status='closed' AND result IS NOT NULL
                            AND closed_at BETWEEN NOW()-INTERVAL '14 days' AND NOW()-INTERVAL '7 days'
                        GROUP BY theme HAVING COUNT(*) >= 3)
                    SELECT r.theme, r.n as rn, ROUND(r.wr::numeric,3) as rwr, p.n as pn, ROUND(p.wr::numeric,3) as pwr
                    FROM recent r LEFT JOIN previous p ON r.theme=p.theme ORDER BY r.n DESC
                """)
                if theme_momentum:
                    lines.append(f"\nTheme Momentum (7d vs prev 7d):")
                    for r in theme_momentum:
                        rwr = float(r['rwr'] or 0) * 100
                        pwr = float(r['pwr'] or 0) * 100 if r['pwr'] else None
                        arrow = '↑' if pwr and rwr - pwr > 3 else '↓' if pwr and rwr - pwr < -3 else '→'
                        prev_str = f"{pwr:.0f}%→" if pwr else ""
                        lines.append(f"  {r['theme']}: {prev_str}{rwr:.0f}% {arrow} ({r['rn']} trades)")

                journey = await conn.fetchrow("""
                    SELECT
                        ROUND(AVG(CASE WHEN event_type='CLOSE_SL' THEN EXTRACT(EPOCH FROM (created_at-(SELECT opened_at FROM positions WHERE id=trade_log.position_id)))/3600 END)::numeric,1) as sl,
                        ROUND(AVG(CASE WHEN event_type='CLOSE_TP' THEN EXTRACT(EPOCH FROM (created_at-(SELECT opened_at FROM positions WHERE id=trade_log.position_id)))/3600 END)::numeric,1) as tp,
                        ROUND(AVG(CASE WHEN event_type='CLOSE_TRAILING_TP' THEN EXTRACT(EPOCH FROM (created_at-(SELECT opened_at FROM positions WHERE id=trade_log.position_id)))/3600 END)::numeric,1) as trail,
                        ROUND(AVG(CASE WHEN event_type='CLOSE_RESOLVED' THEN EXTRACT(EPOCH FROM (created_at-(SELECT opened_at FROM positions WHERE id=trade_log.position_id)))/3600 END)::numeric,1) as resolved
                    FROM trade_log WHERE event_type IN ('CLOSE_SL','CLOSE_TP','CLOSE_TRAILING_TP','CLOSE_RESOLVED') AND created_at > NOW()-INTERVAL '14 days'
                """)
                if journey:
                    lines.append(f"\nSignal Journey (avg hours): SL={journey['sl'] or '?'}h | TP={journey['tp'] or '?'}h | Trail={journey['trail'] or '?'}h | Resolved={journey['resolved'] or '?'}h")

                # Repeat losers
                repeat_losers = await conn.fetch("""
                    SELECT question, COUNT(*) as entries, SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses, SUM(pnl) as total_pnl
                    FROM positions WHERE status='closed' AND result IS NOT NULL
                    GROUP BY question HAVING COUNT(*) >= 2 ORDER BY SUM(pnl) ASC LIMIT 5
                """)
                if repeat_losers:
                    lines.append(f"\nRepeat Losers:")
                    for r in repeat_losers:
                        lines.append(f"  {r['entries']}x {r['losses']}L {r['total_pnl']:+.0f}$ | {r['question'][:60]}")

                # Short-term bets
                short_term = await conn.fetchrow("""
                    SELECT COUNT(*) as total, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins, SUM(pnl) as total_pnl
                    FROM positions WHERE status='closed' AND result IS NOT NULL
                    AND (question ~* 'up or down' OR question ~* 'higher or lower' OR question ~* E'\\\\d{1,2}:\\\\d{2}\\\\s*(AM|PM)')
                """)
                if short_term and short_term['total'] and short_term['total'] > 0:
                    st_wr = round(short_term['wins'] / short_term['total'] * 100)
                    lines.append(f"\nShort-term Bets: {short_term['wins']}/{short_term['total']} ({st_wr}%) total={short_term['total_pnl']:+.0f}$")
        except Exception as e:
            lines.append(f"\n  Diagnostics error: {e}")

        # ━━━ 6. CONFIG & PATTERNS ━━━
        lines.append("\n" + "━" * 40)
        lines.append("6. CONFIG & PATTERNS")
        lines.append("━" * 40)

        lines.append(f"\nCurrent: BANKROLL={start} MIN_EV={_config['MIN_EV']} MIN_KL={_config['MIN_KL']} MAX_KELLY={_config['MAX_KELLY_FRAC']} TP={_config['TAKE_PROFIT_PCT']} SL={_config['STOP_LOSS_PCT']}")

        try:
            async with _db.pool.acquire() as conn:
                patterns = await conn.fetch("""
                    SELECT category, trade_n, trade_wr, trade_roi, kelly_mult, ev_mult
                    FROM patterns WHERE trade_n > 0 ORDER BY trade_n DESC
                """)
            if patterns:
                lines.append(f"\nTheme Calibration:")
                for p in patterns:
                    p = dict(p)
                    lines.append(f"  {p['category']:12s} n={p['trade_n']:3d} WR={float(p['trade_wr'] or 0)*100:.0f}% ROI={float(p['trade_roi'] or 0)*100:+.1f}% kelly={float(p['kelly_mult'] or 1):.2f} ev={float(p['ev_mult'] or 1):.2f}")
        except Exception:
            pass

        # Open positions (compact)
        lines.append(f"\nOpen Positions ({len(open_pos)}):")
        # Group by theme
        from collections import defaultdict as _ddict
        _by_theme = _ddict(list)
        for p in open_pos:
            _by_theme[p.get('theme', '?')].append(p)
        for theme in sorted(_by_theme, key=lambda t: -sum(p['stake_amt'] for p in _by_theme[t])):
            positions = _by_theme[theme]
            total_stake = sum(p['stake_amt'] for p in positions)
            total_upnl = sum((p.get('unrealized_pnl') or 0) for p in positions)
            lines.append(f"  [{theme}] {len(positions)} pos, ${total_stake:.0f} staked, uPnL={total_upnl:+.2f}$")
            for p in positions:
                upnl = p.get("unrealized_pnl") or 0
                lines.append(f"    {p['side']} {p.get('question','')[:55]} | {p['side_price']*100:.0f}c→{((p.get('current_price') or p['side_price'])*100):.0f}c | {upnl:+.2f}$ | ${p['stake_amt']:.0f}")

        # === ARBITRAGE BOT (full) ===
        try:
            arb_stats = await _db.get_arb_stats()
            if arb_stats and arb_stats.get("wins", 0) + arb_stats.get("losses", 0) > 0:
                arb_total = arb_stats["wins"] + arb_stats["losses"]
                arb_wr = round(arb_stats["wins"] / arb_total * 100, 1) if arb_total > 0 else 0
                lines.append(f"\n## ARBITRAGE BOT")
                lines.append(f"  Bankroll: ${arb_stats['bankroll']:.2f} | P&L: ${arb_stats['total_pnl']:+.2f} | WR: {arb_wr}% ({arb_stats['wins']}W/{arb_stats['losses']}L)")

                arb_open = await _db.get_arb_open_positions()
                if arb_open:
                    lines.append(f"  Open arb positions: {len(arb_open)}")
                    for p in arb_open:
                        upnl = p.get("unrealized_pnl") or 0
                        lines.append(f"    [{p['side']}] {p.get('question','')[:60]} | entry={p.get('side_price',0)*100:.1f}c now={((p.get('current_price') or p.get('side_price',0))*100):.1f}c | uPnL={upnl:+.2f}$ | group={p.get('group_name','?')}")

                arb_analytics = await _db.get_arb_analytics()
                if arb_analytics.get("by_group"):
                    lines.append(f"  Arb avg lifetime: {arb_analytics['avg_lifetime_min']:.1f} min")
                    lines.append(f"  Arb WR by group:")
                    for r in arb_analytics["by_group"]:
                        g_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"    {r['group_name']}: {r['wins']}/{r['total']} ({g_wr}%) avg_pnl={r['avg_pnl']:+.2f}$ total_pnl={r['total_pnl']:+.2f}$")
                if arb_analytics.get("by_reason"):
                    lines.append(f"  Arb WR by close reason:")
                    for r in arb_analytics["by_reason"]:
                        g_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"    {r['reason']}: {r['wins']}/{r['total']} ({g_wr}%) avg_pnl={r['avg_pnl']:+.2f}$")
                if arb_analytics.get("by_side"):
                    lines.append(f"  Arb WR by side:")
                    for r in arb_analytics["by_side"]:
                        g_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"    {r['side']}: {r['wins']}/{r['total']} ({g_wr}%) avg_pnl={r['avg_pnl']:+.2f}$")
                if arb_analytics.get("daily_pnl"):
                    lines.append(f"  Arb daily P&L:")
                    for r in arb_analytics["daily_pnl"]:
                        g_wr = round(r['wins'] / r['trades'] * 100, 1) if r['trades'] > 0 else 0
                        lines.append(f"    {r['day']}: {r['pnl']:+.2f}$ ({r['trades']} trades, WR={g_wr}%)")
        except Exception:
            pass

        # === MICRO (SCALPING) BOT (full) ===
        try:
            micro_stats = await _db.get_micro_stats()
            if micro_stats and micro_stats.get("wins", 0) + micro_stats.get("losses", 0) > 0:
                m_total = micro_stats["wins"] + micro_stats["losses"]
                m_wr = round(micro_stats["wins"] / m_total * 100, 1) if m_total > 0 else 0
                lines.append(f"\n## MICRO (SCALPING) BOT")
                lines.append(f"  Bankroll: ${micro_stats['bankroll']:.2f} | P&L: ${micro_stats['total_pnl']:+.2f} | WR: {m_wr}% ({micro_stats['wins']}W/{micro_stats['losses']}L)")
                lines.append(f"  Total trades: {micro_stats.get('total_trades',0)} | Peak equity: ${micro_stats.get('peak_equity',0):.2f}")

                micro_open = await _db.get_micro_open_positions()
                if micro_open:
                    lines.append(f"  Open micro positions: {len(micro_open)}")
                    for p in micro_open:
                        upnl = p.get("unrealized_pnl") or 0
                        lines.append(f"    [{p['side']}] {p.get('question','')[:60]} | entry={p.get('entry_price',0)*100:.1f}c now={((p.get('current_price') or p.get('entry_price',0))*100):.1f}c | uPnL={upnl:+.2f}$ | theme={p.get('theme','?')}")

                micro_analytics = await _db.get_micro_analytics()
                if micro_analytics.get("by_theme"):
                    lines.append(f"  Micro avg lifetime: {micro_analytics.get('avg_lifetime_hours',0):.1f}h")
                    lines.append(f"  Micro WR by theme:")
                    for r in micro_analytics["by_theme"]:
                        g_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"    {r['theme']}: {r['wins']}/{r['total']} ({g_wr}%) avg_pnl={r['avg_pnl']:+.2f}$ total_pnl={r['total_pnl']:+.2f}$")
                if micro_analytics.get("by_reason"):
                    lines.append(f"  Micro WR by close reason:")
                    for r in micro_analytics["by_reason"]:
                        g_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"    {r['reason']}: {r['wins']}/{r['total']} ({g_wr}%) avg_pnl={r['avg_pnl']:+.2f}$")
                if micro_analytics.get("by_side"):
                    lines.append(f"  Micro WR by side:")
                    for r in micro_analytics["by_side"]:
                        g_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"    {r['side']}: {r['wins']}/{r['total']} ({g_wr}%) avg_pnl={r['avg_pnl']:+.2f}$")
                if micro_analytics.get("daily_pnl"):
                    lines.append(f"  Micro daily P&L:")
                    for r in micro_analytics["daily_pnl"]:
                        g_wr = round(r['wins'] / r['trades'] * 100, 1) if r['trades'] > 0 else 0
                        lines.append(f"    {r['day']}: {r['pnl']:+.2f}$ ({r['trades']} trades, WR={g_wr}%)")
        except Exception:
            pass

        # === CALIBRATION TABLE (per-agent from calibration table) ===
        try:
            async with _db.pool.acquire() as conn:
                cal_rows = await conn.fetch("""
                    SELECT agent, ROUND(AVG(brier_score)::numeric, 4) as avg_brier,
                           ROUND(AVG(bias)::numeric, 4) as avg_bias,
                           ROUND(AVG(correction_factor)::numeric, 4) as avg_correction,
                           COUNT(*) as n
                    FROM calibration
                    GROUP BY agent ORDER BY n DESC
                """)
            if cal_rows:
                lines.append(f"\n## CALIBRATION BY AGENT")
                for r in cal_rows:
                    lines.append(f"  {r['agent']}: brier={float(r['avg_brier']):.4f} bias={float(r['avg_bias']):+.4f} correction={float(r['avg_correction']):.4f} (n={r['n']})")
        except Exception:
            pass

        lines.append(f"\n{'=' * 60}")
        lines.append("END OF AUDIT")

        report = "\n".join(lines)
        return Response(report, media_type="text/plain; charset=utf-8")
    except Exception as e:
        log.error(f"[DASHBOARD] System audit error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


# ── API ──

@app.get("/api")
async def api_stats():
    try:
        stats = await _db.get_stats()
        open_ = await _db.get_open_positions()
        closed = await _db.get_closed_positions(limit=5)
        return Response(to_json({"stats": stats, "open": len(open_), "recent": len(closed)}), media_type="application/json")
    except Exception as e:
        log.warning(f"[DASHBOARD] API error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/export/positions")
async def export_positions(date_from: str = None, date_to: str = None):
    """CSV export of closed positions."""
    df = _parse_date(date_from)
    dt = _parse_date(date_to)
    rows = await _db.get_positions_for_export(df, dt)
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


@app.get("/api/diagnostics")
async def api_diagnostics():
    """Deep WR diagnostics — close reasons, avg win/loss, EV/Kelly/lifetime/stake buckets."""
    try:
        diag = await _db.get_wr_diagnostics()
        return Response(to_json(diag), media_type="application/json")
    except Exception as e:
        log.warning(f"[DASHBOARD] Diagnostics error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "3000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
