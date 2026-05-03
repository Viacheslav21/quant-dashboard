"""Audit route: /api/micro-audit — full text data dump for the micro bot."""

import asyncio
from datetime import datetime, timezone
from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response

import routes.deps as deps
from routes.deps import log, compute_sharpe_ratio, compute_max_drawdown, compute_streaks

router = APIRouter(prefix="/api")


@router.get("/micro-audit")
async def micro_audit():
    """Full micro bot audit — comprehensive data dump."""
    try:
        # closed_all is bounded to 200 rows for the section-6 table dump only.
        # Numeric aggregates (avg_win/loss, best/worst, recent windows, theme ROI,
        # resolution-rate buckets, hold-time split) come from get_micro_audit_aggregates,
        # so growing the trade history past 200 doesn't silently corrupt the report.
        (stats, open_pos, closed_recent, analytics, pnl_data, price_paths, agg,
         recent_cfg, pnl_by_hour, theme_adj, worst_per_reason, rapid_blocks) = await asyncio.gather(
            deps.db.get_micro_stats(),
            deps.db.get_micro_open_positions(),
            deps.db.get_micro_closed_positions(limit=200, offset=0),
            deps.db.get_micro_analytics(),
            deps.db.get_micro_cumulative_pnl(),
            deps.db.get_micro_price_paths(limit=15),
            deps.db.get_micro_audit_aggregates(),
            deps.db.get_micro_recent_config_changes(days=7),
            deps.db.get_micro_pnl_by_hour(),
            deps.db.get_micro_theme_adj_wr(),
            deps.db.get_micro_worst_per_reason(per_reason=3),
            deps.db.get_micro_rapid_drop_blocks(),
        )
        # Kept under the old name in places where ordering doesn't matter; section 6's
        # table loop uses the same `closed_recent` 200-row slice.
        closed_all = closed_recent

        # Pull live config values that the audit needs at the top — saves repeated
        # roundtrips and means worst-case calc reflects whatever the user actually
        # configured (was hardcoded to 3.0 before).
        start = 500.0
        max_loss_cap = 3.0
        try:
            async with deps.db.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT key, value FROM config_live
                    WHERE service='micro' AND key IN ('BANKROLL', 'MAX_LOSS_PER_POS')
                """)
                cfg_top = {r["key"]: r["value"] for r in rows}
                start = float(cfg_top.get("BANKROLL", start))
                max_loss_cap = float(cfg_top.get("MAX_LOSS_PER_POS", max_loss_cap))
        except Exception:
            pass
        total = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0
        # ROI on starting capital — uses realized total_pnl, NOT (bankroll - start).
        # bankroll nets open stakes, so it would flag a profitable bot as -ROI whenever
        # a lot of capital is tied up in open positions.
        roi = (stats["total_pnl"] / start * 100) if start > 0 else 0

        sharpe = compute_sharpe_ratio(closed_all)
        drawdown = compute_max_drawdown(closed_all, start)
        streaks = compute_streaks(closed_all)

        # Win/loss sizes — pulled from SQL aggregates so growth past 200 doesn't lie.
        avg_win = round(float(agg.get("avg_win") or 0), 2)
        avg_loss = round(float(agg.get("avg_loss") or 0), 2)

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
        hold_w = float(agg.get("hold_h_win") or 0)
        hold_l = float(agg.get("hold_h_loss") or 0)
        lines.append(
            f"Sharpe: {sharpe:.2f} | MaxDD: -{drawdown['max_dd_pct']:.1f}% | "
            f"Avg lifetime: {analytics['avg_lifetime_hours']:.1f}h "
            f"(W: {hold_w:.1f}h, L: {hold_l:.1f}h)"
        )
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
        if total > 0 and (avg_win or avg_loss):
            wr_frac = stats["wins"] / total
            ev_per_trade = wr_frac * avg_win + (1 - wr_frac) * avg_loss
            if ev_per_trade < 0:
                alerts.append(
                    f"Negative EV: ${ev_per_trade:+.3f}/trade "
                    f"({wr:.0f}% × ${avg_win:+.2f} + {(1-wr_frac)*100:.0f}% × ${avg_loss:+.2f})"
                )
        # 7d / 30d windows — SQL aggregates, not capped by closed_recent's 200-row slice.
        _now = datetime.now(timezone.utc)
        pnl_7d    = float(agg.get("pnl_7d") or 0)
        trades_7d = int(agg.get("trades_7d") or 0)
        wins_7d   = int(agg.get("wins_7d") or 0)
        wr_7d     = round(wins_7d / trades_7d * 100, 1) if trades_7d else 0
        pnl_30d    = float(agg.get("pnl_30d") or 0)
        trades_30d = int(agg.get("trades_30d") or 0)
        lines.append(f"7d: {pnl_7d:+.2f}$ ({trades_7d} trades, WR={wr_7d}%) | 30d: {pnl_30d:+.2f}$ ({trades_30d} trades)")
        if pnl_7d < -10:
            alerts.append(f"7d P&L: {pnl_7d:+.2f}$ (heavy losses)")

        if alerts:
            lines.append(f"\nALERTS:")
            for a in alerts:
                lines.append(f"  ! {a}")
        else:
            lines.append(f"\nNo alerts.")

        # ━━━ 1b. RECENT CONFIG CHANGES (last 7d) ━━━
        # Surfaced near the top so the reader can correlate "what we tuned" with
        # "what happened" before drilling into the metrics.
        if recent_cfg:
            lines.append("\n" + "━" * 40)
            lines.append("1b. RECENT CONFIG CHANGES (last 7d)")
            lines.append("━" * 40)
            for c in recent_cfg:
                ts = c["changed_at"]
                ts_str = ts.strftime("%m/%d %H:%M") if hasattr(ts, "strftime") else str(ts)[:16]
                lines.append(
                    f"  {ts_str} {c['key']}: {c['old_value']} → {c['new_value']} "
                    f"(v{c['version']})"
                )

        # ━━━ 2. PERFORMANCE ━━━
        lines.append("\n" + "━" * 40)
        lines.append("2. PERFORMANCE")
        lines.append("━" * 40)

        lines.append(f"\nDaily P&L (last 14d):")
        for r in analytics["daily_pnl"]:
            d_wr = round(r['wins'] / r['trades'] * 100, 1) if r['trades'] > 0 else 0
            lines.append(f"  {r['day']}: {r['pnl']:+.2f}$ ({r['trades']} trades, WR={d_wr}%)")

        if pnl_by_hour:
            lines.append(f"\nP&L by Hour (UTC):")
            # Compact display: only print hours with trades, flag big losers.
            for r in pnl_by_hour:
                hwr = round(int(r['wins']) / int(r['total']) * 100, 0) if int(r['total']) else 0
                flag = " !" if float(r['total_pnl']) < -2 else ""
                lines.append(
                    f"  {int(r['hour']):02d}h: {int(r['total']):>3} trades | "
                    f"WR={int(hwr):>3}% | pnl={float(r['total_pnl']):+7.2f}${flag}"
                )

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

        # Best/worst from SQL — must scan the whole closed table, not just last 200.
        if agg.get("best") and agg["best"].get("question"):
            lines.append(f"\nBest:  {agg['best']['pnl']:+.2f}$ — {agg['best']['question'][:60]}")
            lines.append(f"Worst: {agg['worst']['pnl']:+.2f}$ — {agg['worst']['question'][:60]}")

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

                # WR by quality score — reads denormalized micro_positions.quality
                # (copied from watchlist at entry; watchlist row is deleted after entry,
                # so a JOIN would lose every position newer than that change).
                quality_wr = await conn.fetch("""
                    SELECT CASE
                        WHEN p.quality >= 80 THEN 'Q80+'
                        WHEN p.quality >= 60 THEN 'Q60-80'
                        WHEN p.quality >= 40 THEN 'Q40-60'
                        ELSE 'Q<40' END as bucket,
                        COUNT(*) as total, SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(AVG(p.pnl)::numeric, 2) as avg_pnl, ROUND(SUM(p.pnl)::numeric, 2) as total_pnl
                    FROM micro_positions p
                    WHERE p.status='closed' AND p.result IS NOT NULL AND p.quality IS NOT NULL
                    GROUP BY bucket ORDER BY bucket
                """)
                if quality_wr:
                    lines.append(f"\nWR by Quality Score:")
                    for r in quality_wr:
                        b_wr = round(r['wins'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                        lines.append(f"  {r['bucket']}: {r['wins']}/{r['total']} ({b_wr}%) avg={r['avg_pnl']:+.2f}$ total={r['total_pnl']:+.2f}$")

                # Q60-80 breakdown — denormalized micro_positions.quality
                q6080_rows = await conn.fetch("""
                    SELECT p.side, p.result, p.theme,
                        ROUND((p.entry_price * 100)::numeric, 1) as entry_c,
                        ROUND(p.pnl::numeric, 2) as pnl,
                        ROUND(p.stake_amt::numeric, 2) as stake,
                        p.close_reason, p.quality,
                        p.question
                    FROM micro_positions p
                    WHERE p.status='closed' AND p.result IS NOT NULL
                      AND p.quality >= 60 AND p.quality < 80
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

                # WR by days_left at entry — denormalized micro_positions.entry_days_left
                days_wr = await conn.fetch("""
                    SELECT CASE
                        WHEN p.entry_days_left <= 1 THEN '<=1d'
                        WHEN p.entry_days_left <= 3 THEN '1-3d'
                        WHEN p.entry_days_left <= 5 THEN '3-5d'
                        ELSE '5d+' END as bucket,
                        COUNT(*) as total, SUM(CASE WHEN p.result='WIN' THEN 1 ELSE 0 END) as wins,
                        ROUND(AVG(p.pnl)::numeric, 2) as avg_pnl, ROUND(SUM(p.pnl)::numeric, 2) as total_pnl
                    FROM micro_positions p
                    WHERE p.status='closed' AND p.result IS NOT NULL AND p.entry_days_left IS NOT NULL
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
                        'MAX_STAKE_Q80_6H', 'MAX_STAKE_Q80_1D', 'PCT_STAKE_Q80',
                        'MAX_LOSS_PER_POS', 'MAX_LOSS_BYPASS_BLOCKS', 'RAPID_DROP_PCT',
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

                # Bayesian-shrunk WR per theme — same shrinkage (k=20) micro uses
                # internally for the auto-block decision (block if adj_wr < 40%
                # after BLOCK_MIN_TRADES). Surfacing this lets us see which themes
                # are CLOSE to the auto-block threshold before they trip.
                if theme_adj:
                    lines.append(f"\nBayesian Theme adj_wr (auto-block threshold: 40% @ ≥5 trades):")
                    for r in theme_adj:
                        warn = ""
                        if r["n"] >= 5 and r["adj_wr"] < 0.50:
                            warn = " ← near block threshold" if r["adj_wr"] >= 0.40 else " ← BLOCK ZONE"
                        lines.append(
                            f"  {r['theme']:<14} n={r['n']:>3} raw={r['raw_wr']*100:>5.1f}% "
                            f"adj={r['adj_wr']*100:>5.1f}%{warn}"
                        )

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
                    SELECT market_id, side, question, ROUND(pnl::numeric, 2) as pnl,
                           close_reason, closed_at
                    FROM micro_positions
                    WHERE close_reason IN ('rapid_drop', 'max_loss') AND status='closed'
                    ORDER BY closed_at DESC LIMIT 10
                """)
                if sl_blacklist:
                    lines.append(f"\nRecent SL Blacklist (no re-entry):")
                    for r in sl_blacklist:
                        lines.append(
                            f"  {r['side']} {r['pnl']:+.2f}$ [{r['close_reason']}] | "
                            f"{(r.get('question') or r['market_id'])[:55]}"
                        )

                # Expired open positions — read end_date directly (TEXT column from
                # Gamma, ISO-8601 like "2026-04-15T23:59:00Z"). Way more reliable than
                # regex-parsing the question text, which misses formats like
                # "Q4 2026", "by end of November", "April 30 (NY time)".
                expired_list = []
                for p in open_pos:
                    end_str = p.get("end_date")
                    if not end_str:
                        continue
                    try:
                        end = datetime.fromisoformat(str(end_str).replace("Z", "+00:00"))
                    except (ValueError, TypeError):
                        continue
                    days_past = (_now - end).total_seconds() / 86400
                    if days_past > 1:
                        q = p.get("question", "")
                        expired_list.append(
                            f"  ! {q[:60]} ({days_past:.1f}d past expiry, "
                            f"${p.get('stake_amt', 0):.2f})"
                        )
                if expired_list:
                    lines.append(f"\nExpired Open Positions:")
                    lines.extend(expired_list)

        except Exception as e:
            lines.append(f"\n  Diagnostics error: {e}")

        # ━━━ 5. EFFICIENCY ━━━
        lines.append("\n" + "━" * 40)
        lines.append("5. EFFICIENCY & SCALING")
        lines.append("━" * 40)

        # ROI per theme with avg hold time — SQL aggregates over the full closed
        # table, not the 200-row dump. Includes total_stake so ROI = pnl/stake.
        lines.append(f"\nROI by Theme:")
        for r in agg.get("theme_roi", []):
            stake = float(r.get("total_stake") or 0)
            pnl = float(r.get("total_pnl") or 0)
            roi_pct = (pnl / stake * 100) if stake > 0 else 0
            avg_hold = float(r.get("avg_hold_h") or 0)
            lines.append(
                f"  {r['theme']}: ROI={roi_pct:+.1f}% | {int(r['total'])} trades | "
                f"${stake:.0f} staked | avg hold {avg_hold:.1f}h"
            )

        # Resolution rate — buckets must cover EVERY close_reason emitted by monitor.py
        # and resolver.py: resolved (WIN), resolved_loss (LOSS at ≤1¢), take_profit
        # (early exit at TP), max_loss (hard cap), rapid_drop (>7¢ drop), expired (72h+
        # past end_date force-close). 'stop_loss' is legacy — % SL is disabled in micro.
        # Counts come from get_micro_audit_aggregates so they're not capped at 200.
        resolved_win  = int(agg.get("n_resolved")      or 0)
        resolved_loss = int(agg.get("n_resolved_loss") or 0)
        take_profit   = int(agg.get("n_take_profit")   or 0)
        expired_count = int(agg.get("n_expired")       or 0)
        max_loss_n    = int(agg.get("n_max_loss")      or 0)
        rapid_drop_n  = int(agg.get("n_rapid_drop")    or 0)
        sl_count = max_loss_n + rapid_drop_n
        accounted = resolved_win + resolved_loss + take_profit + expired_count + sl_count
        # `total` from stats covers full closed set, not just `closed_recent`.
        other_count = total - accounted

        def _pct(n):
            return f"{n*100//max(total,1)}%"

        lines.append(f"\nResolution Rate:")
        lines.append(f"  Resolved WIN:  {resolved_win}/{total} ({_pct(resolved_win)}) — full payout")
        lines.append(f"  Resolved LOSS: {resolved_loss}/{total} ({_pct(resolved_loss)}) — bid hit ≤1¢")
        lines.append(f"  Take Profit:   {take_profit}/{total} ({_pct(take_profit)}) — early exit @ TP")
        lines.append(f"  Expired:       {expired_count}/{total} ({_pct(expired_count)}) — force-closed 72h past expiry")
        lines.append(f"  Max Loss:      {max_loss_n}/{total} ({_pct(max_loss_n)}) — hard $ cap")
        lines.append(f"  Rapid Drop:    {rapid_drop_n}/{total} ({_pct(rapid_drop_n)}) — bid dropped >RAPID_DROP_PCT")
        if other_count > 0:
            lines.append(f"  Other:         {other_count}/{total} — unrecognized close_reason, investigate")

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

        # Worst case risk on open positions — uses live MAX_LOSS_PER_POS from
        # config_live so it reflects whatever the user actually configured.
        if open_pos:
            worst_case = len(open_pos) * max_loss_cap
            total_stake = sum(p.get('stake_amt', 0) for p in open_pos)
            bankroll_plus_stake = stats['bankroll'] + total_stake
            lines.append(f"\nOpen Risk:")
            lines.append(f"  Positions: {len(open_pos)} | Staked: ${total_stake:.2f}")
            lines.append(f"  Worst case (all hit ${max_loss_cap:.2f} max_loss): -${worst_case:.2f}")
            lines.append(f"  Capital utilization: {total_stake/bankroll_plus_stake*100:.0f}%")

        # MAX_LOSS REST-block diagnostics — counts how often REST verify blocked a max_loss close.
        # Logged from monitor.py via record_price_tick(source='max_loss_blocked'). High counts
        # indicate REST (CLOB book / Gamma midpoint) is lagging vs WS, which can delay cap enforcement.
        try:
            async with deps.db.pool.acquire() as conn:
                blocked_rows = await conn.fetch("""
                    SELECT
                        COUNT(*) FILTER (WHERE ts > NOW() - INTERVAL '24 hours') AS d1,
                        COUNT(*) FILTER (WHERE ts > NOW() - INTERVAL '7 days')   AS d7,
                        COUNT(*) AS total,
                        COUNT(DISTINCT market_id) FILTER (WHERE ts > NOW() - INTERVAL '7 days') AS uniq_markets
                    FROM micro_price_history
                    WHERE source = 'max_loss_blocked'
                """)
                if blocked_rows:
                    b = blocked_rows[0]
                    if int(b["total"] or 0) > 0:
                        lines.append(f"\nMAX_LOSS REST Blocks (REST lag diagnostic):")
                        lines.append(f"  24h: {int(b['d1'] or 0)} | 7d: {int(b['d7'] or 0)} (across {int(b['uniq_markets'] or 0)} markets) | All-time: {int(b['total'] or 0)}")
                        # Top recent offenders — markets where REST blocked most
                        top = await conn.fetch("""
                            SELECT market_id, side, COUNT(*) AS n
                            FROM micro_price_history
                            WHERE source = 'max_loss_blocked' AND ts > NOW() - INTERVAL '7 days'
                            GROUP BY market_id, side
                            ORDER BY n DESC
                            LIMIT 5
                        """)
                        if top:
                            for row in top:
                                lines.append(f"    {row['market_id'][:12]} {row['side']}: {int(row['n'])} blocks")
        except Exception as _e:
            pass  # diagnostic is best-effort, never break the report

        # Rapid-drop frequency, alongside MAX_LOSS blocks. Both are exit-path
        # signals: high counts mean either real volatility or REST lag triggering
        # spurious exits. We compare them to see if rapid_drop dominates.
        if rapid_blocks and rapid_blocks.get("total"):
            lines.append(f"\nRapid Drop closes (frequency check):")
            lines.append(
                f"  24h: {int(rapid_blocks['d1'] or 0)} | "
                f"7d:  {int(rapid_blocks['d7'] or 0)} | "
                f"All-time: {int(rapid_blocks['total'] or 0)}"
            )

        # Top 3 worst trades per close_reason — pattern detection. If max_loss
        # consistently hits crypto markets, we can fix the filter; if rapid_drop
        # consistently hits sports near game-time, we can pre-exit instead.
        if worst_per_reason:
            from collections import defaultdict as _dd
            by_reason = _dd(list)
            for r in worst_per_reason:
                by_reason[r["close_reason"]].append(r)
            lines.append(f"\nTop 3 Worst Trades per Close Reason:")
            for reason in sorted(by_reason):
                lines.append(f"  [{reason}]")
                for r in by_reason[reason]:
                    lines.append(
                        f"    {r['side']} ${r['pnl']:+7.2f} ${r['stake']:>5.2f} "
                        f"{r['entry_c']:>4.1f}c→{r['exit_c']:>4.1f}c "
                        f"{(r['theme'] or '?'):<10} {(r['question'] or '')[:55]}"
                    )

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

