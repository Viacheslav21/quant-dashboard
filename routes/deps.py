"""Shared dependencies for all route modules.

All routes import db, config, templates, and helpers from here.
The `init()` function is called once from app.py after startup."""

import os
import logging
from utils.helpers import pc, wr_color, to_json
from utils.metrics import compute_sharpe_ratio, compute_max_drawdown, compute_streaks

log = logging.getLogger("dashboard")

# Set by app.py on startup
db = None
config = {}
templates = None

DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")
API_SECRET = os.getenv("API_SECRET", "")
ML_API_URL = os.getenv("ML_API_URL", "")


def init(database, app_config, app_templates):
    """Called from app.py after lifespan init."""
    global db, config, templates
    db = database
    config = app_config
    templates = app_templates


def ctx(**kwargs) -> dict:
    """Base template context with helpers."""
    return {
        "pc": pc,
        "wr_color": wr_color,
        "to_json": to_json,
        "auth_enabled": bool(DASHBOARD_TOKEN),
        **kwargs,
    }


def parse_date(s):
    """Parse date string from query param."""
    if not s:
        return None
    try:
        return s.strip() + "T00:00:00+00:00" if "T" not in s else s
    except Exception:
        return None
