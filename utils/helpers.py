import json
from datetime import datetime, date as _date
from decimal import Decimal


def pc(v):
    """Price color: blue for positive, red for negative."""
    return "#3B82F6" if v >= 0 else "#EF4444"


def wr_color(w, t):
    """Win rate color: blue >= 50%, red < 50%, gray if no data."""
    if t > 0 and w / t >= 0.5:
        return "#3B82F6"
    elif t > 0:
        return "#EF4444"
    return "#6B7280"


def _json_serial(obj):
    if isinstance(obj, (datetime, _date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)


def to_json(data):
    return json.dumps(data, default=_json_serial)
