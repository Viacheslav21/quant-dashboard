import math


def compute_sharpe_ratio(trades: list, risk_free_rate: float = 0.0) -> float:
    """Annualized Sharpe ratio from closed trades."""
    if len(trades) < 2:
        return 0.0
    returns = []
    for t in trades:
        stake = float(t.get("stake_amt") or 0)
        if stake > 0:
            returns.append(float(t["pnl"]) / stake)
    if len(returns) < 2:
        return 0.0
    mean_r = sum(returns) / len(returns)
    var = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
    std_r = math.sqrt(var) if var > 0 else 0
    if std_r == 0:
        return 0.0
    # Annualize assuming ~365 trades/year for prediction markets
    return (mean_r - risk_free_rate) / std_r * math.sqrt(365)


def compute_max_drawdown(trades: list, start_bankroll: float) -> dict:
    """Max drawdown from peak equity. Returns absolute, percentage, and series."""
    if not trades:
        return {"max_dd_pct": 0, "max_dd_abs": 0, "series": []}

    equity = float(start_bankroll)
    peak = equity
    max_dd_abs = 0.0
    max_dd_pct = 0.0
    series = []

    for t in trades:
        equity += float(t["pnl"])
        if equity > peak:
            peak = equity
        dd_abs = peak - equity
        dd_pct = (dd_abs / peak * 100) if peak > 0 else 0
        if dd_abs > max_dd_abs:
            max_dd_abs = dd_abs
            max_dd_pct = dd_pct
        closed_at = t.get("closed_at")
        ts = closed_at.isoformat() if closed_at else ""
        series.append({"t": ts, "dd": round(dd_pct, 2)})

    return {
        "max_dd_pct": round(max_dd_pct, 2),
        "max_dd_abs": round(max_dd_abs, 2),
        "series": series,
    }


def compute_streaks(trades: list) -> dict:
    """Win/loss streaks from chronologically ordered trades."""
    if not trades:
        return {"cur_win": 0, "cur_loss": 0, "max_win": 0, "max_loss": 0}

    cur_win = cur_loss = max_win = max_loss = 0
    for t in trades:
        if t.get("result") == "WIN":
            cur_win += 1
            cur_loss = 0
            max_win = max(max_win, cur_win)
        elif t.get("result") == "LOSS":
            cur_loss += 1
            cur_win = 0
            max_loss = max(max_loss, cur_loss)

    return {"cur_win": cur_win, "cur_loss": cur_loss, "max_win": max_win, "max_loss": max_loss}


def compute_equity_curve(trades: list, start_bankroll: float) -> list:
    """Equity (bankroll) over time for charting."""
    equity = float(start_bankroll)
    curve = [{"t": "", "eq": round(equity, 2)}]
    for t in trades:
        equity += float(t["pnl"])
        closed_at = t.get("closed_at")
        ts = closed_at.isoformat() if closed_at else ""
        curve.append({"t": ts, "eq": round(equity, 2)})
    return curve


def compute_pnl_distribution(trades: list, n_bins: int = 15) -> dict:
    """P&L histogram buckets."""
    if not trades:
        return {"labels": [], "counts": []}
    pnls = [float(t["pnl"]) for t in trades]
    mn, mx = min(pnls), max(pnls)
    if mn == mx:
        return {"labels": [f"{mn:.2f}"], "counts": [len(pnls)]}
    step = (mx - mn) / n_bins
    labels = []
    counts = []
    for i in range(n_bins):
        lo = mn + i * step
        hi = lo + step
        label = f"{lo:+.2f}"
        cnt = sum(1 for p in pnls if (lo <= p < hi) or (i == n_bins - 1 and p == hi))
        labels.append(label)
        counts.append(cnt)
    return {"labels": labels, "counts": counts}
