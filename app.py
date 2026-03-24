import os
import io
import csv
import json
import hmac
import hashlib
import logging
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

app = FastAPI()
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
        "now_utc": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC"),
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
    form = await request.form()
    token = form.get("token", "")
    if hmac.compare_digest(token, DASHBOARD_TOKEN):
        response = RedirectResponse(url="/", status_code=302)
        response.set_cookie(
            "session_token", _hash_token(DASHBOARD_TOKEN),
            max_age=30 * 24 * 3600,  # 30 days
            httponly=True, samesite="lax",
        )
        log.info("[AUTH] Login successful")
        return response
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
        per_page = 100
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
                f"EV≥{params.get('MIN_EV', '')} KL≥{params.get('MIN_KL', '')} "
                f"Kelly:{params.get('MAX_KELLY_FRAC', '')} SL:{params.get('STOP_LOSS_PCT', '')}"
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
            equity_data=to_json(equity),
            drawdown_data=to_json(drawdown["series"]),
            dist_data=to_json(pnl_dist),
            sharpe=sharpe, drawdown=drawdown,
            rolling=rolling, best_worst=best_worst,
            exec_right=exec_right, exec_total=len(exec_sigs),
            rej_right=rej_right, rej_saved=rej_saved,
            date_from=date_from, date_to=date_to,
            has_api_secret=_check_api_secret(request),
            api_secret_required=bool(API_SECRET),
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Analytics error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Analytics Error</h1><pre>{e}</pre>", status_code=500)


# ── Arbitrage ──

@app.get("/arbitrage", response_class=HTMLResponse)
async def arbitrage(request: Request, page: int = 1):
    try:
        per_page = 100
        stats = await _db.get_arb_stats()
        open_ = await _db.get_arb_open_positions()
        total_closed = stats["wins"] + stats["losses"]
        total_pages = max(1, (total_closed + per_page - 1) // per_page)
        closed = await _db.get_arb_closed_positions(limit=per_page, offset=(page - 1) * per_page)
        signals = await _db.get_arb_signals(limit=20)
        pnl_data = await _db.get_arb_cumulative_pnl()
        data = await _db.get_arb_analytics()

        arb_bankroll = _config["BANKROLL"]
        roi = ((stats["bankroll"] - arb_bankroll) / arb_bankroll * 100) if arb_bankroll > 0 else 0
        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0

        arb_open_in_profit = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) >= 0)
        arb_open_in_loss = sum(1 for p in open_ if (p.get("unrealized_pnl") or 0) < 0)
        arb_open_total_upnl = sum((p.get("unrealized_pnl") or 0) for p in open_)

        return templates.TemplateResponse(request, "arbitrage.html", _ctx(
            active_page="arbitrage",
            stats=stats, roi=roi, wr=wr, total=total,
            arb_bankroll=arb_bankroll,
            open_positions=open_, closed=list(reversed(closed)),
            signals=signals, data=data,
            arb_open_in_profit=arb_open_in_profit,
            arb_open_in_loss=arb_open_in_loss,
            arb_open_total_upnl=arb_open_total_upnl,
            total_closed=total_closed, page=page, total_pages=total_pages,
            pnl_data=to_json(pnl_data),
        ))
    except Exception as e:
        log.error(f"[DASHBOARD] Arbitrage error: {e}", exc_info=True)
        return HTMLResponse(f"<h1>Arbitrage Error</h1><pre>{e}</pre>", status_code=500)


# ── API ──

@app.get("/api")
async def api_stats():
    try:
        stats = await _db.get_stats()
        open_ = await _db.get_open_positions()
        closed = await _db.get_closed_positions(limit=5)
        return JSONResponse({"stats": stats, "open": len(open_), "recent": len(closed)})
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


@app.post("/api/run-analysis")
async def run_analysis(request: Request):
    """Run Sonnet analysis on demand via dashboard button."""
    if not _check_api_secret(request):
        return JSONResponse({"error": "API secret required", "need_secret": True}, status_code=403)
    try:
        from anthropic import AsyncAnthropic
        client = AsyncAnthropic(api_key=_config["ANTHROPIC_KEY"])

        data = await _db.get_analytics()
        stats = await _db.get_stats()
        open_pos = await _db.get_open_positions()
        start = _config["BANKROLL"]

        summary = (
            f"=== QUANT ENGINE DAILY STATS ===\n"
            f"Bankroll: ${stats['bankroll']:.2f} (start: ${start:.0f}, ROI: {(stats['bankroll']-start)/start*100:+.1f}%)\n"
            f"P&L: ${stats['total_pnl']:+.2f} | WR: {stats['wins']}W/{stats['losses']}L\n"
            f"Open positions: {len(open_pos)} | Avg EV: {stats['avg_ev']*100:.1f}% | Avg Kelly: {stats['avg_kelly']*100:.1f}%\n\n"
        )
        for section, key, fields in [
            ("WIN RATE BY THEME", "by_theme", "theme"),
            ("WIN RATE BY SOURCE", "by_source", "source"),
            ("WIN RATE BY SIDE", "by_side", "side"),
        ]:
            summary += f"=== {section} ===\n"
            for r in data[key]:
                wr = round(r['wins'] / r['total'] * 100) if r['total'] > 0 else 0
                summary += f"  {r[fields]}: {r['wins']}/{r['total']} ({wr}%) avg_pnl={float(r['avg_pnl']):+.2f}\n"
            summary += "\n"

        summary += "=== CALIBRATION ===\n"
        for r in data["calibration"]:
            summary += f"  {r['bucket']}: {r['total']} trades, predicted={float(r['avg_predicted'])*100:.1f}%, actual={float(r['actual_wr'])*100:.1f}%\n"

        summary += (
            f"\n=== EV ACCURACY ===\n"
            f"  Predicted EV: +{data['ev_predicted']*100:.1f}% | Actual return: {data['ev_actual']*100:+.1f}%\n"
            f"  Avg position lifetime: {data['avg_lifetime_hours']:.1f}h\n"
            f"\n=== CLOSE REASONS ===\n"
        )
        for r in data["by_reason"]:
            summary += f"  {r['reason']}: {r['total']} trades, avg_pnl={float(r['avg_pnl']):+.2f}\n"

        summary += (
            f"\n=== CONFIG ===\n"
            f"  MIN_EV={_config['MIN_EV']} MIN_KL={_config['MIN_KL']} MAX_KELLY_FRAC={_config['MAX_KELLY_FRAC']}\n"
            f"  TAKE_PROFIT={_config['TAKE_PROFIT_PCT']} STOP_LOSS={_config['STOP_LOSS_PCT']}\n"
        )

        r = await client.messages.create(
            model="claude-sonnet-4-5", max_tokens=800,
            system="""You are a quantitative trading analyst reviewing a prediction market bot's performance.
Give specific, actionable recommendations. Be direct and concise.
Focus on: what's working, what's not, config changes to suggest (with specific numbers), and risks.
Reply in English, max 500 words. Use plain text (no markdown).""",
            messages=[{"role": "user", "content": summary}],
        )
        analysis = "".join(b.text for b in r.content if hasattr(b, "text"))
        if analysis:
            return JSONResponse({"analysis": analysis})
        return JSONResponse({"error": "Analysis returned empty"}, status_code=500)
    except Exception as e:
        log.error(f"[DASHBOARD] Analysis error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Lifecycle ──

@app.on_event("startup")
async def startup():
    global _db
    _db = Database(os.getenv("DATABASE_URL"))
    await _db.init()
    log.info(f"[DASHBOARD] Ready on port {os.getenv('PORT', '3000')}")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "3000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
