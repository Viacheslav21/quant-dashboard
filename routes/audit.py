"""Audit routes: /api/system-audit, /api/micro-audit."""

import asyncio
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response

import routes.deps as deps
from routes.deps import log, compute_sharpe_ratio, compute_max_drawdown, compute_streaks

router = APIRouter(prefix="/api")


# ENGINE DISABLED ↓
# @router.get("/system-audit")
async def system_audit():
    """Full system audit — comprehensive data dump for analysis."""
    try:
        stats = await deps.db.get_stats()
        open_pos = await deps.db.get_open_positions()
        all_trades = await deps.db.get_all_closed_trades()
        analytics = await deps.db.get_analytics()
        diagnostics = await deps.db.get_wr_diagnostics()
        clv = await deps.db.get_clv_analytics()
        dma = await deps.db.get_dma_weights()
        rolling = await deps.db.get_rolling_performance()
        best_worst = await deps.db.get_best_worst_trades()
        signals = await deps.db.get_recent_signals(limit=30)
        sig_outcomes = await deps.db.get_signal_outcomes(limit=50)
        market_metrics = await deps.db.get_all_market_metrics(limit=50)
        config_hist = await deps.db.get_config_history()
        start = deps.config["BANKROLL"]

        sharpe = compute_sharpe_ratio(all_trades)
        drawdown = compute_max_drawdown(all_trades, start)
        streaks = compute_streaks(all_trades)

        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0
        # ROI on starting capital — uses realized total_pnl, NOT (bankroll - start).
        # bankroll nets open stakes, so it would flag a profitable bot as -ROI whenever
        # a lot of capital is tied up in open positions.
        roi = (stats["total_pnl"] / start * 100) if start > 0 else 0

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
            async with deps.db.pool.acquire() as _aconn:
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
            total_pnl = float(r.get('total_pnl') or r['avg_pnl'] * r['total'])
            flag = " !" if t_wr < 40 and r['total'] >= 10 else ""
            lines.append(f"  {r['theme']}: {r['wins']}/{r['total']} ({t_wr}%) avg={r['avg_pnl']:+.2f}$ total={total_pnl:+.0f}${flag}")

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
            async with deps.db.pool.acquire() as conn:
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
                    _m = re.search(r'(?:on|by|before)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:,?\s+(\d{4}))?', q, re.IGNORECASE)
                    if _m:
                        _ms, _ds, _ys = _m.group(1), _m.group(2), _m.group(3)
                        _yr = int(_ys) if _ys else datetime.now(timezone.utc).year
                        try:
                            _qd = datetime.strptime(f"{_ms} {_ds} {_yr}", "%B %d %Y").replace(tzinfo=timezone.utc)
                            _da = (datetime.now(timezone.utc) - _qd).days
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
            async with deps.db.pool.acquire() as conn:
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
            async with deps.db.pool.acquire() as conn:
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
            async with deps.db.pool.acquire() as conn:
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

        lines.append(f"\nCurrent: BANKROLL={start} MIN_EV={deps.config['MIN_EV']} MIN_KL={deps.config['MIN_KL']} MAX_KELLY={deps.config['MAX_KELLY_FRAC']} TP={deps.config['TAKE_PROFIT_PCT']} SL={deps.config['STOP_LOSS_PCT']}")

        try:
            async with deps.db.pool.acquire() as conn:
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

        # ━━━ 7. EVIDENCE & FEATURES (from trade_log details) ━━━
        lines.append("\n" + "━" * 40)
        lines.append("7. EVIDENCE & FEATURES")
        lines.append("━" * 40)
        try:
            async with deps.db.pool.acquire() as conn:
                # WR by n_evidence
                n_ev_stats = await conn.fetch("""
                    SELECT CASE
                        WHEN (tl.details->>'n_evidence')::int <= 2 THEN '1-2'
                        WHEN (tl.details->>'n_evidence')::int <= 4 THEN '3-4'
                        ELSE '5+' END as bucket,
                        COUNT(*) as total,
                        SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(AVG(p.pnl)::numeric, 2) as avg_pnl
                    FROM positions p
                    JOIN trade_log tl ON tl.position_id = p.id AND tl.event_type = 'OPEN'
                    WHERE p.status='closed' AND p.result IS NOT NULL
                        AND tl.details->>'n_evidence' IS NOT NULL
                    GROUP BY bucket ORDER BY bucket
                """)
                if n_ev_stats:
                    lines.append(f"\nWR by Evidence Count:")
                    for r in n_ev_stats:
                        wr_n = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"  {r['bucket']} sources: {r['wins']}/{r['total']} ({wr_n}%) avg={float(r['avg_pnl']):+.2f}$")

                # WR by Hurst regime
                hurst_stats = await conn.fetch("""
                    SELECT CASE
                        WHEN (tl.details->>'hurst')::float < 0.4 THEN 'mean-revert (<0.4)'
                        WHEN (tl.details->>'hurst')::float BETWEEN 0.4 AND 0.6 THEN 'random (0.4-0.6)'
                        ELSE 'trending (>0.6)' END as regime,
                        COUNT(*) as total,
                        SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(AVG(p.pnl)::numeric, 2) as avg_pnl
                    FROM positions p
                    JOIN trade_log tl ON tl.position_id = p.id AND tl.event_type = 'OPEN'
                    WHERE p.status='closed' AND p.result IS NOT NULL
                        AND tl.details->>'hurst' IS NOT NULL
                    GROUP BY regime ORDER BY regime
                """)
                if hurst_stats:
                    lines.append(f"\nWR by Hurst Regime:")
                    for r in hurst_stats:
                        wr_h = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"  {r['regime']}: {r['wins']}/{r['total']} ({wr_h}%) avg={float(r['avg_pnl']):+.2f}$")

                # Recheck effectiveness
                recheck_stats = await conn.fetchrow("""
                    SELECT
                        COUNT(*) FILTER (WHERE details->>'reason' = 'stale_price') as stale_blocked,
                        COUNT(*) FILTER (WHERE details->>'reason' = 'market_in_review') as review_blocked,
                        COUNT(*) FILTER (WHERE details->>'reason' = 'market_closed_pre_exec') as closed_blocked
                    FROM trade_log WHERE event_type = 'SIGNAL_REJECTED'
                        AND details->>'reason' IN ('stale_price','market_in_review','market_closed_pre_exec')
                        AND created_at > NOW() - INTERVAL '7 days'
                """)
                if recheck_stats:
                    total_blocked = (recheck_stats['stale_blocked'] or 0) + (recheck_stats['review_blocked'] or 0) + (recheck_stats['closed_blocked'] or 0)
                    if total_blocked > 0:
                        lines.append(f"\nRecheck Blocked (7d): {total_blocked} signals (stale:{recheck_stats['stale_blocked']}, review:{recheck_stats['review_blocked']}, closed:{recheck_stats['closed_blocked']})")

                # Grace period with 2h boundary
                grace2h = await conn.fetch("""
                    SELECT CASE
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at))/3600 < 1 THEN '<1h'
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at))/3600 < 2 THEN '1-2h'
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at))/3600 < 3 THEN '2-3h'
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at))/3600 < 6 THEN '3-6h'
                        ELSE '6h+' END as bucket,
                        COUNT(*) as total,
                        SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(AVG(pnl)::numeric, 2) as avg_pnl
                    FROM positions WHERE status='closed' AND result IS NOT NULL AND closed_at IS NOT NULL
                    GROUP BY bucket ORDER BY bucket
                """)
                if grace2h:
                    lines.append(f"\nWR by Age (2h grace boundary):")
                    for r in grace2h:
                        if r['bucket'] and r['total'] > 0:
                            wr_g = round(r['wins'] / r['total'] * 100, 1)
                            lines.append(f"  {r['bucket']}: {r['wins']}/{r['total']} ({wr_g}%) avg={float(r['avg_pnl']):+.2f}$")

                # Bimodal sizing effectiveness
                bimodal = await conn.fetch("""
                    SELECT CASE
                        WHEN stake_amt <= 4 THEN '$1-4'
                        WHEN stake_amt BETWEEN 4.01 AND 5 THEN '$4-5 (bimodal low)'
                        WHEN stake_amt BETWEEN 5.01 AND 10 THEN '$5-10 (toxic zone)'
                        WHEN stake_amt BETWEEN 10.01 AND 13 THEN '$11-13 (bimodal high)'
                        WHEN stake_amt BETWEEN 13.01 AND 20 THEN '$13-20'
                        ELSE '$20+' END as bucket,
                        COUNT(*) as total,
                        SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(SUM(pnl)::numeric, 2) as total_pnl
                    FROM positions WHERE status='closed' AND result IS NOT NULL
                    GROUP BY bucket ORDER BY MIN(stake_amt)
                """)
                if bimodal:
                    lines.append(f"\nWR by Stake (bimodal zones):")
                    for r in bimodal:
                        if r['total'] > 0:
                            wr_b = round(r['wins'] / r['total'] * 100, 1)
                            lines.append(f"  {r['bucket']}: {r['wins']}/{r['total']} ({wr_b}%) total={float(r['total_pnl']):+.0f}$")

                # TP shield / resolved near expiry
                tp_shield = await conn.fetch("""
                    SELECT event_type,
                        COUNT(*) as total,
                        ROUND(AVG(pnl)::numeric, 2) as avg_pnl,
                        ROUND(AVG(pnl_pct)::numeric, 3) as avg_pnl_pct
                    FROM trade_log
                    WHERE event_type IN ('CLOSE_TP','CLOSE_RESOLVED','CLOSE_TRAILING_TP','CLOSE_SL','CLOSE_MANUAL')
                    GROUP BY event_type ORDER BY avg_pnl DESC
                """)
                if tp_shield:
                    lines.append(f"\nClose Type Comparison:")
                    for r in tp_shield:
                        lines.append(f"  {r['event_type']}: {r['total']} trades, avg={float(r['avg_pnl']):+.2f}$ ({float(r['avg_pnl_pct'])*100:+.1f}%)")

                # Contrarian vs non-contrarian
                contrarian_wr = await conn.fetch("""
                    SELECT tl.is_contrarian,
                        COUNT(*) as total,
                        SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(AVG(p.pnl)::numeric, 2) as avg_pnl
                    FROM positions p
                    JOIN trade_log tl ON tl.position_id = p.id AND tl.event_type = 'OPEN'
                    WHERE p.status='closed' AND p.result IS NOT NULL
                    GROUP BY tl.is_contrarian
                """)
                if contrarian_wr:
                    lines.append(f"\nContrarian vs Normal:")
                    for r in contrarian_wr:
                        label = "Contrarian" if r['is_contrarian'] else "Normal"
                        wr_c = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"  {label}: {r['wins']}/{r['total']} ({wr_c}%) avg={float(r['avg_pnl']):+.2f}$")

        except Exception as e:
            lines.append(f"\n  Evidence section error: {e}")

        # Open positions (compact)
        lines.append(f"\nOpen Positions ({len(open_pos)}):")
        # Group by theme
        _by_theme = defaultdict(list)
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

        # === MICRO (SCALPING) BOT (full) ===
        try:
            micro_stats = await deps.db.get_micro_stats()
            if micro_stats and micro_stats.get("wins", 0) + micro_stats.get("losses", 0) > 0:
                m_total = micro_stats["wins"] + micro_stats["losses"]
                m_wr = round(micro_stats["wins"] / m_total * 100, 1) if m_total > 0 else 0
                lines.append(f"\n## MICRO (SCALPING) BOT")
                lines.append(f"  Bankroll: ${micro_stats['bankroll']:.2f} | P&L: ${micro_stats['total_pnl']:+.2f} | WR: {m_wr}% ({micro_stats['wins']}W/{micro_stats['losses']}L)")
                lines.append(f"  Total trades: {micro_stats.get('total_trades',0)}")

                micro_open = await deps.db.get_micro_open_positions()
                if micro_open:
                    lines.append(f"  Open micro positions: {len(micro_open)}")
                    for p in micro_open:
                        upnl = p.get("unrealized_pnl") or 0
                        lines.append(f"    [{p['side']}] {p.get('question','')[:60]} | entry={p.get('entry_price',0)*100:.1f}c now={((p.get('current_price') or p.get('entry_price',0))*100):.1f}c | uPnL={upnl:+.2f}$ | theme={p.get('theme','?')}")

                micro_analytics = await deps.db.get_micro_analytics()
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
            async with deps.db.pool.acquire() as conn:
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


@router.get("/micro-audit")
async def micro_audit():
    """Full micro bot audit — comprehensive data dump."""
    try:
        stats, open_pos, closed_all, analytics, pnl_data, price_paths = await asyncio.gather(
            deps.db.get_micro_stats(),
            deps.db.get_micro_open_positions(),
            deps.db.get_micro_closed_positions(limit=9999, offset=0),
            deps.db.get_micro_analytics(),
            deps.db.get_micro_cumulative_pnl(),
            deps.db.get_micro_price_paths(limit=15),
        )

        try:
            async with deps.db.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT value FROM config_live WHERE service='micro' AND key='BANKROLL'"
                )
                start = float(row["value"]) if row else 500.0
        except Exception:
            start = 500.0
        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0
        # ROI on starting capital — uses realized total_pnl, NOT (bankroll - start).
        # bankroll nets open stakes, so it would flag a profitable bot as -ROI whenever
        # a lot of capital is tied up in open positions.
        roi = (stats["total_pnl"] / start * 100) if start > 0 else 0

        sharpe = compute_sharpe_ratio(closed_all)
        drawdown = compute_max_drawdown(closed_all, start)
        streaks = compute_streaks(closed_all)

        # Win/loss sizes
        wins_pnl = [float(t.get("pnl") or 0) for t in closed_all if t.get("result") == "WIN"]
        losses_pnl = [float(t.get("pnl") or 0) for t in closed_all if t.get("result") == "LOSS"]
        avg_win = round(sum(wins_pnl) / len(wins_pnl), 2) if wins_pnl else 0
        avg_loss = round(sum(losses_pnl) / len(losses_pnl), 2) if losses_pnl else 0

        lines = []
        lines.append("=" * 60)
        lines.append("MICRO SCALPER — FULL AUDIT")
        lines.append("=" * 60)

        # ━━━ 1. HEALTH CHECK ━━━
        lines.append("\n" + "━" * 40)
        lines.append("1. HEALTH CHECK")
        lines.append("━" * 40)
        lines.append(f"Bank: ${stats['bankroll']:.2f} (start ${start:.0f}) | ROI: {roi:+.1f}% | P&L: ${stats['total_pnl']:+.2f}")
        lines.append(f"WR: {wr}% ({stats['wins']}W/{stats['losses']}L/{total}) | Peak: ${stats['peak_equity']:.2f}")
        lines.append(f"Sharpe: {sharpe:.2f} | MaxDD: -{drawdown['max_dd_pct']:.1f}% | Avg lifetime: {analytics['avg_lifetime_hours']:.1f}h")
        lines.append(f"Avg win: ${avg_win:+.2f} | Avg loss: ${avg_loss:+.2f} | Ratio: {abs(avg_win/avg_loss):.2f}x" if avg_loss != 0 else f"Avg win: ${avg_win:+.2f} | Avg loss: $0")
        lines.append(f"Streaks — Current: {streaks['cur_win']}W/{streaks['cur_loss']}L | Max: {streaks['max_win']}W/{streaks['max_loss']}L")

        # Alerts
        alerts = []
        if wr < 50:
            alerts.append(f"WR {wr}% below 50%")
        if roi < 0:
            alerts.append(f"ROI negative: {roi:+.1f}%")
        if drawdown['max_dd_pct'] > 10:
            alerts.append(f"Max drawdown {drawdown['max_dd_pct']:.1f}% > 10%")
        # EV check replaces naive win/loss ratio — for a resolution harvester with 95% WR
        # and tiny avg wins vs rare big losses, ratio < 1 is expected AND profitable.
        # The honest question is: EV per trade = WR × avg_win + (1 − WR) × avg_loss.
        if total > 0 and (wins_pnl or losses_pnl):
            wr_frac = stats["wins"] / total
            ev_per_trade = wr_frac * avg_win + (1 - wr_frac) * avg_loss
            if ev_per_trade < 0:
                alerts.append(
                    f"Negative EV: ${ev_per_trade:+.3f}/trade "
                    f"({wr:.0f}% × ${avg_win:+.2f} + {(1-wr_frac)*100:.0f}% × ${avg_loss:+.2f})"
                )
        # 7d performance
        _now = datetime.now(timezone.utc)
        recent_7d = [t for t in closed_all if t.get("closed_at") and (isinstance(t["closed_at"], datetime) and (_now - t["closed_at"]).days < 7)]
        pnl_7d = sum(float(t.get("pnl") or 0) for t in recent_7d)
        wins_7d = sum(1 for t in recent_7d if t.get("result") == "WIN")
        wr_7d = round(wins_7d / len(recent_7d) * 100, 1) if recent_7d else 0
        recent_30d = [t for t in closed_all if t.get("closed_at") and (isinstance(t["closed_at"], datetime) and (_now - t["closed_at"]).days < 30)]
        pnl_30d = sum(float(t.get("pnl") or 0) for t in recent_30d)
        lines.append(f"7d: {pnl_7d:+.2f}$ ({len(recent_7d)} trades, WR={wr_7d}%) | 30d: {pnl_30d:+.2f}$ ({len(recent_30d)} trades)")
        if pnl_7d < -10:
            alerts.append(f"7d P&L: {pnl_7d:+.2f}$ (heavy losses)")

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

        lines.append(f"\nBy Theme:")
        for r in analytics["by_theme"]:
            t_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
            flag = " !" if t_wr < 40 and r['total'] >= 5 else ""
            lines.append(f"  {r['theme']}: {r['wins']}/{r['total']} ({t_wr}%) avg={r['avg_pnl']:+.2f}$ total={r['total_pnl']:+.2f}${flag}")

        lines.append(f"\nBy Side:")
        for r in analytics["by_side"]:
            s_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
            lines.append(f"  {r['side']}: {r['wins']}/{r['total']} ({s_wr}%) avg={r['avg_pnl']:+.2f}$")

        lines.append(f"\nClose Reasons:")
        for r in analytics["by_reason"]:
            c_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
            lines.append(f"  {r['reason']}: {r['wins']}/{r['total']} ({c_wr}%) avg={r['avg_pnl']:+.2f}$")

        # Best/worst
        if closed_all:
            best = max(closed_all, key=lambda t: float(t.get("pnl") or 0))
            worst = min(closed_all, key=lambda t: float(t.get("pnl") or 0))
            lines.append(f"\nBest:  {float(best['pnl']):+.2f}$ — {best.get('question','')[:60]}")
            lines.append(f"Worst: {float(worst['pnl']):+.2f}$ — {worst.get('question','')[:60]}")

        # ━━━ 3. RISK ━━━
        lines.append("\n" + "━" * 40)
        lines.append("3. RISK & PORTFOLIO")
        lines.append("━" * 40)

        if open_pos:
            from collections import defaultdict as _ddict
            _by_theme = _ddict(list)
            for p in open_pos:
                _by_theme[p.get('theme', '?')].append(p)
            total_stake = sum(p.get('stake_amt', 0) for p in open_pos)
            total_upnl = sum((p.get('unrealized_pnl') or 0) for p in open_pos)
            lines.append(f"\nOpen Positions ({len(open_pos)}): ${total_stake:.2f} staked, uPnL={total_upnl:+.2f}$")
            for theme in sorted(_by_theme, key=lambda t: -len(_by_theme[t])):
                positions = _by_theme[theme]
                t_stake = sum(p.get('stake_amt', 0) for p in positions)
                t_upnl = sum((p.get('unrealized_pnl') or 0) for p in positions)
                lines.append(f"  [{theme}] {len(positions)} pos, ${t_stake:.2f} staked, uPnL={t_upnl:+.2f}$")
                for p in positions:
                    upnl = p.get("unrealized_pnl") or 0
                    entry = p.get('entry_price', 0)
                    curr = p.get('current_price') or entry
                    lines.append(f"    {p['side']} {p.get('question','')[:55]} | {entry*100:.0f}c→{curr*100:.0f}c | {upnl:+.2f}$ | ${p.get('stake_amt',0):.2f}")
        else:
            lines.append(f"\nNo open positions.")

        # ━━━ 4. DIAGNOSTICS ━━━
        lines.append("\n" + "━" * 40)
        lines.append("4. DIAGNOSTICS")
        lines.append("━" * 40)

        try:
            async with deps.db.pool.acquire() as conn:
                # WR by entry price bucket
                entry_wr = await conn.fetch("""
                    SELECT CASE
                        WHEN entry_price >= 0.93 THEN '93-100c'
                        WHEN entry_price >= 0.90 THEN '90-93c'
                        WHEN entry_price >= 0.85 THEN '85-90c'
                        ELSE '<85c' END as bucket,
                        COUNT(*) as total, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(SUM(pnl)::numeric, 2) as total_pnl
                    FROM micro_positions WHERE status='closed' AND result IS NOT NULL
                    GROUP BY bucket ORDER BY bucket
                """)
                if entry_wr:
                    lines.append(f"\nWR by Entry Price:")
                    for r in entry_wr:
                        b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"  {r['bucket']}: {r['wins']}/{r['total']} ({b_wr}%) total={r['total_pnl']:+.2f}$")

                # WR by stake bucket
                stake_wr = await conn.fetch("""
                    SELECT CASE
                        WHEN stake_amt <= 5 THEN '$0-5'
                        WHEN stake_amt <= 10 THEN '$5-10'
                        WHEN stake_amt <= 20 THEN '$10-20'
                        WHEN stake_amt <= 50 THEN '$20-50'
                        ELSE '$50+' END as bucket,
                        COUNT(*) as total, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(SUM(pnl)::numeric, 2) as total_pnl
                    FROM micro_positions WHERE status='closed' AND result IS NOT NULL
                    GROUP BY bucket ORDER BY bucket
                """)
                if stake_wr:
                    lines.append(f"\nWR by Stake:")
                    for r in stake_wr:
                        b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"  {r['bucket']}: {r['wins']}/{r['total']} ({b_wr}%) total={r['total_pnl']:+.2f}$")

                # WR by hold time
                hold_wr = await conn.fetch("""
                    SELECT CASE
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at))/3600 < 1 THEN '<1h'
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at))/3600 < 6 THEN '1-6h'
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at))/3600 < 24 THEN '6-24h'
                        WHEN EXTRACT(EPOCH FROM (closed_at - opened_at))/3600 < 72 THEN '1-3d'
                        ELSE '3d+' END as bucket,
                        COUNT(*) as total, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(AVG(pnl)::numeric, 2) as avg_pnl, ROUND(SUM(pnl)::numeric, 2) as total_pnl
                    FROM micro_positions WHERE status='closed' AND result IS NOT NULL AND closed_at IS NOT NULL
                    GROUP BY bucket ORDER BY bucket
                """)
                if hold_wr:
                    lines.append(f"\nWR by Hold Time:")
                    for r in hold_wr:
                        b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"  {r['bucket']}: {r['wins']}/{r['total']} ({b_wr}%) avg={r['avg_pnl']:+.2f}$ total={r['total_pnl']:+.2f}$")

                # SL distribution section removed — micro no longer uses % SL
                # (MAX_LOSS + RAPID_DROP are the real exit mechanisms).

                # WR by quality score (join with watchlist which stores quality)
                quality_wr = await conn.fetch("""
                    SELECT CASE
                        WHEN w.quality >= 80 THEN 'Q80+'
                        WHEN w.quality >= 60 THEN 'Q60-80'
                        WHEN w.quality >= 40 THEN 'Q40-60'
                        ELSE 'Q<40' END as bucket,
                        COUNT(*) as total, SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(AVG(p.pnl)::numeric, 2) as avg_pnl, ROUND(SUM(p.pnl)::numeric, 2) as total_pnl
                    FROM micro_positions p JOIN micro_watchlist w ON p.market_id = w.market_id AND p.side = w.side
                    WHERE p.status='closed' AND p.result IS NOT NULL AND w.quality IS NOT NULL
                    GROUP BY bucket ORDER BY bucket
                """)
                if quality_wr:
                    lines.append(f"\nWR by Quality Score:")
                    for r in quality_wr:
                        b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"  {r['bucket']}: {r['wins']}/{r['total']} ({b_wr}%) avg={r['avg_pnl']:+.2f}$ total={r['total_pnl']:+.2f}$")

                # Q60-80 breakdown — worst offenders
                q6080_rows = await conn.fetch("""
                    SELECT p.side, p.result, p.theme,
                        ROUND(p.entry_price * 100, 1) as entry_c,
                        ROUND(p.pnl::numeric, 2) as pnl,
                        ROUND(p.stake_amt::numeric, 2) as stake,
                        p.close_reason, w.quality,
                        p.question
                    FROM micro_positions p
                    JOIN micro_watchlist w ON p.market_id = w.market_id AND p.side = w.side
                    WHERE p.status='closed' AND p.result IS NOT NULL
                      AND w.quality >= 60 AND w.quality < 80
                    ORDER BY p.pnl ASC
                    LIMIT 30
                """)
                if q6080_rows:
                    lines.append(f"\nQ60-80 positions (worst first):")
                    lines.append(f"  {'R':<1} {'Q':>3} {'Entry':>5} {'PnL':>7} {'Stake':>6} {'Reason':<12} {'Theme':<10} Question")
                    for r in q6080_rows:
                        flag = 'W' if r['result'] == 'WIN' else 'L'
                        lines.append(
                            f"  {flag} {r['quality']:>3} {r['entry_c']:>4.1f}c "
                            f"{r['pnl']:>+7.2f}$ ${r['stake']:>5.2f} "
                            f"{r['close_reason']:<12} {(r['theme'] or '?'):<10} "
                            f"{r['question'][:55]}"
                        )

                # WR by days_left at entry (join with watchlist)
                days_wr = await conn.fetch("""
                    SELECT CASE
                        WHEN w.days_left <= 1 THEN '<=1d'
                        WHEN w.days_left <= 3 THEN '1-3d'
                        WHEN w.days_left <= 5 THEN '3-5d'
                        ELSE '5d+' END as bucket,
                        COUNT(*) as total, SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(AVG(p.pnl)::numeric, 2) as avg_pnl, ROUND(SUM(p.pnl)::numeric, 2) as total_pnl
                    FROM micro_positions p JOIN micro_watchlist w ON p.market_id = w.market_id AND p.side = w.side
                    WHERE p.status='closed' AND p.result IS NOT NULL AND w.days_left IS NOT NULL
                    GROUP BY bucket ORDER BY bucket
                """)
                if days_wr:
                    lines.append(f"\nWR by Days Left at Entry:")
                    for r in days_wr:
                        b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"  {r['bucket']}: {r['wins']}/{r['total']} ({b_wr}%) avg={r['avg_pnl']:+.2f}$ total={r['total_pnl']:+.2f}$")

                # WR by config_tag (proxy for source/version)
                tag_wr = await conn.fetch("""
                    SELECT COALESCE(config_tag, 'unknown') as tag,
                        COUNT(*) as total, SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(AVG(pnl)::numeric, 2) as avg_pnl, ROUND(SUM(pnl)::numeric, 2) as total_pnl
                    FROM micro_positions WHERE status='closed' AND result IS NOT NULL
                    GROUP BY tag ORDER BY total DESC
                """)
                if tag_wr:
                    lines.append(f"\nWR by Config Tag:")
                    for r in tag_wr:
                        b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"  {r['tag']}: {r['wins']}/{r['total']} ({b_wr}%) avg={r['avg_pnl']:+.2f}$ total={r['total_pnl']:+.2f}$")

                # Current micro config from config_live
                micro_config_rows = await conn.fetch("""
                    SELECT key, value FROM config_live
                    WHERE service = 'micro'
                    ORDER BY key
                """)
                if micro_config_rows:
                    _KEY_ORDER = [
                        'ENTRY_MIN_PRICE', 'ENTRY_PRICE_1D', 'ENTRY_PRICE_2D', 'ENTRY_PRICE_3D',
                        'WATCHLIST_MIN_PRICE', 'MIN_QUALITY_SCORE', 'MIN_ROI',
                        'MAX_STAKE', 'MIN_STAKE', 'MAX_STAKE_1D', 'MAX_STAKE_6H',
                        'MAX_LOSS_PER_POS', 'RAPID_DROP_PCT',
                        'TAKE_PROFIT_PRICE', 'TAKE_PROFIT_MIN_DAYS',
                        'MAX_OPEN', 'MAX_PER_THEME', 'MAX_PER_NEG_RISK',
                        'MAX_DAYS_LEFT', 'MIN_VOLUME',
                        'SLIPPAGE', 'FEE_PCT',
                        'CONFIG_TAG',
                    ]
                    cfg = {r['key']: r['value'] for r in micro_config_rows}
                    current_tag = cfg.get('CONFIG_TAG', 'current')
                    _perf = {r['tag']: {'wr': round(r['wins']/r['total']*100,1) if r['total']>0 else 0, 'avg': float(r['avg_pnl'])} for r in tag_wr}
                    perf = _perf.get(current_tag, {})
                    perf_str = f"  WR={perf.get('wr','?')}% avg={perf.get('avg',0):+.2f}$" if perf else ""
                    lines.append(f"\nCurrent Config [{current_tag}]{perf_str}:")
                    for k in _KEY_ORDER:
                        if k in cfg:
                            lines.append(f"  {k}: {cfg[k]}")
                    for k, v in cfg.items():
                        if k not in _KEY_ORDER:
                            lines.append(f"  {k}: {v}")

                # Theme auto-block status (computed from positions)
                theme_stats = await conn.fetch("""
                    SELECT p.theme, COUNT(*) as trades,
                        SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN p.result='LOSS' THEN 1 ELSE 0 END) as losses,
                        ROUND(SUM(p.pnl)::numeric, 2) as total_pnl,
                        COALESCE(t.blocked, false) as blocked
                    FROM micro_positions p
                    LEFT JOIN micro_theme_stats t ON p.theme = t.theme
                    WHERE p.status = 'closed' AND p.theme IS NOT NULL
                    GROUP BY p.theme, t.blocked
                    ORDER BY COUNT(*) DESC
                """)
                if theme_stats:
                    lines.append(f"\nTheme Calibration:")
                    for r in theme_stats:
                        flag = " BLOCKED" if r['blocked'] else ""
                        wr = int(r['wins']) * 100 // int(r['trades']) if int(r['trades']) > 0 else 0
                        lines.append(f"  {r['theme']}: {r['wins']}/{r['trades']} WR={wr}% pnl={r['total_pnl']:+.2f}${flag}")

                # Repeat losers
                repeat_losers = await conn.fetch("""
                    SELECT question, COUNT(*) as entries, SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
                           ROUND(SUM(pnl)::numeric, 2) as total_pnl
                    FROM micro_positions WHERE status='closed' AND result IS NOT NULL
                    GROUP BY question HAVING COUNT(*) >= 2 ORDER BY SUM(pnl) ASC LIMIT 5
                """)
                if repeat_losers:
                    lines.append(f"\nRepeat Losers:")
                    for r in repeat_losers:
                        lines.append(f"  {r['entries']}x {r['losses']}L {r['total_pnl']:+.2f}$ | {r['question'][:60]}")

                # SL blacklist (markets where bot won't re-enter)
                sl_blacklist = await conn.fetch("""
                    SELECT market_id, side, question, ROUND(pnl::numeric, 2) as pnl, closed_at
                    FROM micro_positions
                    WHERE close_reason IN ('stop_loss', 'rapid_drop') AND status='closed'
                    ORDER BY closed_at DESC LIMIT 10
                """)
                if sl_blacklist:
                    lines.append(f"\nRecent SL Blacklist (no re-entry):")
                    for r in sl_blacklist:
                        lines.append(f"  {r['side']} {r['pnl']:+.2f}$ | {r.get('question', r['market_id'])[:55]}")

                # Expired open positions
                expired_list = []
                for p in open_pos:
                    q = p.get("question", "")
                    _m = re.search(r'(?:on|by|before)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:,?\s+(\d{4}))?', q, re.IGNORECASE)
                    if _m:
                        _ms, _ds, _ys = _m.group(1), _m.group(2), _m.group(3)
                        _yr = int(_ys) if _ys else _now.year
                        try:
                            _qd = datetime.strptime(f"{_ms} {_ds} {_yr}", "%B %d %Y").replace(tzinfo=timezone.utc)
                            _da = (_now - _qd).days
                            if _da > 1:
                                expired_list.append(f"  ! {q[:60]} ({_da}d ago, ${p.get('stake_amt',0):.2f})")
                        except ValueError:
                            pass
                if expired_list:
                    lines.append(f"\nExpired Open Positions:")
                    lines.extend(expired_list)

        except Exception as e:
            lines.append(f"\n  Diagnostics error: {e}")

        # ━━━ 5. EFFICIENCY ━━━
        lines.append("\n" + "━" * 40)
        lines.append("5. EFFICIENCY & SCALING")
        lines.append("━" * 40)

        # ROI per theme with avg hold time
        lines.append(f"\nROI by Theme:")
        for r in analytics["by_theme"]:
            if r['total'] > 0:
                theme_trades = [t for t in closed_all if t.get("theme") == r["theme"]]
                theme_stake = sum(float(t.get("stake_amt", 0)) for t in theme_trades)
                theme_roi = (float(r['total_pnl']) / theme_stake * 100) if theme_stake > 0 else 0
                avg_hold = sum(
                    (t["closed_at"] - t["opened_at"]).total_seconds() / 3600
                    for t in theme_trades if t.get("closed_at") and t.get("opened_at")
                    and isinstance(t["closed_at"], datetime) and isinstance(t["opened_at"], datetime)
                ) / max(len(theme_trades), 1)
                lines.append(f"  {r['theme']}: ROI={theme_roi:+.1f}% | {r['total']} trades | ${theme_stake:.0f} staked | avg hold {avg_hold:.1f}h")

        # Resolution rate
        resolved_count = sum(1 for t in closed_all if t.get("close_reason") == "resolved")
        expired_count = sum(1 for t in closed_all if t.get("close_reason") == "expired")
        sl_count = sum(1 for t in closed_all if t.get("close_reason") in ("stop_loss", "rapid_drop", "max_loss"))
        other_count = total - resolved_count - expired_count - sl_count
        lines.append(f"\nResolution Rate:")
        lines.append(f"  Resolved: {resolved_count}/{total} ({resolved_count*100//max(total,1)}%) — full payout")
        lines.append(f"  Expired:  {expired_count}/{total} ({expired_count*100//max(total,1)}%) — partial payout")
        lines.append(f"  SL/Loss:  {sl_count}/{total} ({sl_count*100//max(total,1)}%) — stopped out")
        if other_count > 0:
            lines.append(f"  Other:    {other_count}/{total}")

        # Trades per day
        n_days = len(analytics["daily_pnl"])
        if n_days > 0:
            daily_trades = [int(d.get("trades", 0)) for d in analytics["daily_pnl"]]
            daily_pnls = [float(d["pnl"]) for d in analytics["daily_pnl"]]
            avg_trades = sum(daily_trades) / n_days
            max_trades = max(daily_trades) if daily_trades else 0
            min_trades = min(daily_trades) if daily_trades else 0
            lines.append(f"\nDaily Volume:")
            lines.append(f"  Avg: {avg_trades:.1f} trades/day | Best: {max_trades} | Worst: {min_trades}")
            lines.append(f"  Total: {total} trades over {n_days} days")

            # Profit per day
            avg_daily = sum(daily_pnls) / n_days
            profitable_days = sum(1 for d in daily_pnls if d > 0)
            lines.append(f"\nDaily Profit:")
            lines.append(f"  Avg: ${avg_daily:+.2f}/day | {profitable_days}/{n_days} profitable days ({profitable_days*100//max(n_days,1)}%)")
            lines.append(f"  Best day: ${max(daily_pnls):+.2f} | Worst day: ${min(daily_pnls):+.2f}")
            lines.append(f"  Avg P&L per trade: ${sum(daily_pnls)/max(total,1):+.3f}")
            if avg_daily > 0:
                lines.append(f"  Projected: ${avg_daily*7:.2f}/week | ${avg_daily*30:.2f}/month")

        # Worst case risk on open positions
        if open_pos:
            max_loss_cap = 3.0
            worst_case = len(open_pos) * max_loss_cap
            total_stake = sum(p.get('stake_amt', 0) for p in open_pos)
            bankroll_plus_stake = stats['bankroll'] + total_stake
            lines.append(f"\nOpen Risk:")
            lines.append(f"  Positions: {len(open_pos)} | Staked: ${total_stake:.2f}")
            lines.append(f"  Worst case (all hit max_loss): -${worst_case:.2f}")
            lines.append(f"  Capital utilization: {total_stake/bankroll_plus_stake*100:.0f}%")

        # ━━━ 6. ALL CLOSED POSITIONS ━━━
        lines.append("\n" + "━" * 40)
        lines.append("6. CLOSED POSITIONS")
        lines.append("━" * 40)

        if closed_all:
            # Header
            lines.append(f"\n{'Side':<4} {'Result':<5} {'PnL':>8} {'Stake':>7} {'Entry':>6} {'Exit':>6} {'Reason':<12} {'Theme':<10} {'Hold':>6} {'Opened':<16} {'Closed':<16} {'Question'}")
            lines.append("-" * 140)
            for t in closed_all:
                side = t.get("side", "?")
                result = t.get("result", "?")
                pnl = float(t.get("pnl") or 0)
                stake = float(t.get("stake_amt") or 0)
                entry_p = float(t.get("entry_price") or 0)
                exit_p = float(t.get("current_price") or entry_p)
                reason = t.get("close_reason", "?")
                theme = t.get("theme", "?")
                opened = t.get("opened_at")
                closed = t.get("closed_at")
                question = t.get("question", "")

                # Hold time
                hold = ""
                if opened and closed and isinstance(opened, datetime) and isinstance(closed, datetime):
                    h = (closed - opened).total_seconds() / 3600
                    hold = f"{h:.1f}h"

                opened_str = opened.strftime("%m/%d %H:%M") if isinstance(opened, datetime) else str(opened)[:16] if opened else ""
                closed_str = closed.strftime("%m/%d %H:%M") if isinstance(closed, datetime) else str(closed)[:16] if closed else ""

                lines.append(
                    f"{side:<4} {'W' if result=='WIN' else 'L':<5} "
                    f"${pnl:>+7.2f} ${stake:>6.2f} "
                    f"{entry_p*100:>5.1f}c {exit_p*100:>5.1f}c "
                    f"{reason:<12} {theme:<10} "
                    f"{hold:>6} {opened_str:<16} {closed_str:<16} "
                    f"{question[:50]}"
                )
        else:
            lines.append("\nNo closed positions.")

        # ━━━ 7. PRICE PATH HISTORY ━━━
        lines.append("\n" + "━" * 40)
        lines.append("7. PRICE PATH (last 15 positions with history)")
        lines.append("━" * 40)

        if not price_paths:
            lines.append("\nNo price history recorded yet.")
        else:
            for entry in price_paths:
                pos = entry["pos"]
                ticks = entry["ticks"]
                question = pos.get("question", "")[:70]
                side = pos.get("side", "?")
                result = pos.get("result", "?")
                reason = pos.get("close_reason", "?")
                entry_p = float(pos.get("entry_price") or 0)
                exit_p = float(pos.get("current_price") or entry_p)
                pnl = float(pos.get("pnl") or 0)
                closed_at = pos.get("closed_at")
                closed_str = closed_at.strftime("%m/%d %H:%M") if isinstance(closed_at, datetime) else ""

                marker = "✓" if result == "WIN" else "✗"
                lines.append(f"\n{marker} {side} ${pnl:+.2f}  {entry_p*100:.1f}¢→{exit_p*100:.1f}¢  [{reason}]  {closed_str}")
                lines.append(f"  {question}")

                if not ticks:
                    lines.append("  (no ticks)")
                    continue

                entry_p_ticks = float(pos.get("entry_price") or 0)
                lines.append(f"  {'Time':<10} {'Price':>7} {'Delta':>8}  {'Src'}")
                lines.append(f"  {'-'*38}")
                prev_price = None
                for t in ticks:
                    price = t["price"]
                    src = t["source"]
                    ts = t["ts"]
                    ts_str = ts.strftime("%H:%M:%S") if hasattr(ts, "strftime") else str(ts)[11:19]
                    if prev_price is None:
                        # Show delta from entry_price for first tick
                        delta = (price - entry_p_ticks) * 100
                        if abs(delta) < 0.05:
                            delta_str = "  (entry)"
                        else:
                            arrow = "▼" if delta < 0 else "▲"
                            delta_str = f"{arrow}{abs(delta):>5.1f}¢ *"
                    else:
                        delta = (price - prev_price) * 100
                        arrow = "▼" if delta < -0.05 else "▲" if delta > 0.05 else " "
                        delta_str = f"{arrow}{abs(delta):>5.1f}¢" if abs(delta) >= 0.05 else "        "
                    lines.append(f"  {ts_str:<10} {price*100:>5.1f}¢  {delta_str:<9} {src}")
                    prev_price = price

        lines.append(f"\n{'=' * 60}")
        lines.append("END OF MICRO AUDIT")

        report = "\n".join(lines)
        return Response(report, media_type="text/plain; charset=utf-8")
    except Exception as e:
        log.error(f"[DASHBOARD] Micro audit error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)

