import os
import json
import asyncio
import logging
from datetime import datetime, timezone, date as _date
from decimal import Decimal
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

from utils.db import Database

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

def _json_serial(obj):
    if isinstance(obj, (datetime, _date)): return obj.isoformat()
    if isinstance(obj, Decimal): return float(obj)
    return str(obj)

def to_json(data):
    return json.dumps(data, default=_json_serial)

log = logging.getLogger("dashboard")
app = FastAPI()

_db     = None
_config = {
    "ANTHROPIC_KEY":    os.getenv("ANTHROPIC_API_KEY"),
    "BANKROLL":         float(os.getenv("BANKROLL", "1000")),
    "MIN_EV":           float(os.getenv("MIN_EV", "0.12")),
    "MIN_KL":           float(os.getenv("MIN_KL", "0.10")),
    "MAX_KELLY_FRAC":   float(os.getenv("MAX_KELLY_FRAC", "0.15")),
    "TAKE_PROFIT_PCT":  float(os.getenv("TAKE_PROFIT_PCT", "0.20")),
    "STOP_LOSS_PCT":    float(os.getenv("STOP_LOSS_PCT", "0.30")),
}

@app.get("/", response_class=HTMLResponse)
async def dashboard(page: int = 1):
  try:
    per_page = 100
    stats   = await _db.get_stats()
    open_   = await _db.get_open_positions()
    all_closed = await _db.get_closed_positions(limit=per_page * page)
    # Paginate: skip previous pages
    closed = all_closed[(page-1)*per_page : page*per_page]
    total_closed = stats["wins"] + stats["losses"]
    total_pages = max(1, (total_closed + per_page - 1) // per_page)
    signals = await _db.get_recent_signals(limit=10)
    pnl_data = await _db.get_cumulative_pnl()

    start = float(os.getenv("BANKROLL","1000"))
    roi   = ((stats["bankroll"]-start)/start*100)
    total = stats["wins"]+stats["losses"]
    wr    = round(stats["wins"]/total*100,1) if total>0 else 0

    mode = "Simulation" if (_config or {}).get("SIMULATION", True) else "Live"

    def pc(v): return "#3B82F6" if v>=0 else "#EF4444"

    open_rows = "".join([f"""<tr>
        <td class="q">{p['question'][:70]}...</td>
        <td class="{'yes' if p['side']=='YES' else 'no'}">{p['side']}</td>
        <td class="num">{p['side_price']*100:.1f}&#162;</td>
        <td class="num">{(p.get('current_price') or p['side_price'])*100:.1f}&#162;</td>
        <td class="num" style="color:{pc(p.get('unrealized_pnl',0))}">{p.get('unrealized_pnl',0):+.2f}$</td>
        <td class="num ev">+{p['ev']*100:.1f}%</td>
        <td class="num kl">{p['kl']:.3f}</td>
        <td class="num">${p['stake_amt']:.2f}</td>
        <td><a href="{p['url']}" target="_blank" class="link-arrow">&#8599;</a></td>
    </tr>""" for p in open_]) or '<tr><td colspan="9" class="empty">Нет открытых позиций</td></tr>'

    closed_rows = "".join([f"""<tr>
        <td class="q">{t['question'][:60]}...</td>
        <td class="{'yes' if t['side']=='YES' else 'no'}">{t['side']}</td>
        <td class="num">{t['side_price']*100:.1f}&#162;</td>
        <td>{t.get('outcome','?')}</td>
        <td class="num" style="color:{pc(t['pnl'])}">{t['pnl']:+.2f}$</td>
        <td><span class="badge {'win' if t['result']=='WIN' else 'loss'}">{t['result']}</span></td>
        <td class="num ev">+{t['ev']*100:.1f}%</td>
    </tr>""" for t in reversed(closed)]) or '<tr><td colspan="7" class="empty">Нет сделок</td></tr>'

    sig_rows = "".join([f"""<tr>
        <td class="q">{s['question'][:60]}...</td>
        <td class="{'yes' if s['side']=='YES' else 'no'}">{s['side']}</td>
        <td class="num">{s['p_market']*100:.1f}&#162;</td>
        <td class="num">{s['p_final']*100:.1f}&#162;</td>
        <td class="num ev">+{s['ev']*100:.1f}%</td>
        <td class="num kl">{s['kl']:.3f}</td>
        <td class="source">{s.get('source','math')}</td>
    </tr>""" for s in signals]) or '<tr><td colspan="7" class="empty">Нет сигналов</td></tr>'

    return f"""<!DOCTYPE html>
<html lang="ru"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Quant Engine v3</title>
<!-- auto-refresh disabled -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{
  background:#111827;
  color:#E5E7EB;
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
  font-size:14px;
  line-height:1.5;
  -webkit-font-smoothing:antialiased;
}}
.container{{
  max-width:1400px;
  margin:0 auto;
  padding:24px 32px;
}}
.header{{
  display:flex;
  align-items:center;
  justify-content:space-between;
  margin-bottom:28px;
  padding-bottom:20px;
  border-bottom:1px solid #1F2937;
}}
.header-left{{display:flex;align-items:center;gap:12px}}
.header h1{{
  color:#F9FAFB;
  font-size:20px;
  font-weight:600;
  letter-spacing:-0.02em;
}}
.status-dot{{
  width:8px;height:8px;
  border-radius:50%;
  background:#10B981;
  box-shadow:0 0 0 3px rgba(16,185,129,0.15);
  flex-shrink:0;
}}
.header-right{{
  color:#6B7280;
  font-size:13px;
}}
.grid{{
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(200px,1fr));
  gap:16px;
  margin-bottom:28px;
}}
.card{{
  background:#1F2937;
  border:1px solid #374151;
  border-radius:12px;
  padding:20px;
  transition:border-color 0.15s ease;
}}
.card:hover{{border-color:#4B5563}}
.card .label{{
  color:#9CA3AF;
  font-size:12px;
  font-weight:500;
  letter-spacing:0.05em;
  text-transform:uppercase;
  margin-bottom:8px;
}}
.card .value{{
  font-family:'SF Mono',SFMono-Regular,ui-monospace,Menlo,Monaco,Consolas,monospace;
  font-size:28px;
  font-weight:700;
  letter-spacing:-0.02em;
}}
.card .sub{{
  color:#6B7280;
  font-size:12px;
  margin-top:6px;
}}
.panel{{
  background:#1F2937;
  border:1px solid #374151;
  border-radius:12px;
  margin-bottom:20px;
  overflow:hidden;
}}
.panel-header{{
  padding:16px 20px;
  border-bottom:1px solid #374151;
  display:flex;
  align-items:center;
  gap:8px;
}}
.panel-header h2{{
  color:#D1D5DB;
  font-size:13px;
  font-weight:600;
  letter-spacing:0.03em;
  text-transform:uppercase;
}}
.panel-header .count{{
  background:#374151;
  color:#9CA3AF;
  font-size:11px;
  font-weight:500;
  padding:2px 8px;
  border-radius:10px;
}}
table{{width:100%;border-collapse:collapse}}
th{{
  color:#6B7280;
  text-align:left;
  padding:10px 16px;
  font-size:11px;
  font-weight:500;
  letter-spacing:0.05em;
  text-transform:uppercase;
  background:#1a2332;
  border-bottom:1px solid #374151;
}}
td{{
  padding:12px 16px;
  border-bottom:1px solid rgba(55,65,81,0.5);
  vertical-align:middle;
  font-size:13px;
}}
tr:nth-child(even) td{{background:rgba(17,24,39,0.3)}}
tr:hover td{{background:rgba(55,65,81,0.3)}}
.empty{{
  text-align:center;
  color:#4B5563;
  padding:32px;
  font-style:italic;
}}
.num{{
  font-family:'SF Mono',SFMono-Regular,ui-monospace,Menlo,Monaco,Consolas,monospace;
  font-size:13px;
  font-variant-numeric:tabular-nums;
}}
.yes{{color:#3B82F6;font-weight:600}}
.no{{color:#EF4444;font-weight:600}}
.ev{{color:#3B82F6}}
.kl{{color:#F59E0B}}
.q{{color:#9CA3AF;font-size:12px;max-width:280px;line-height:1.4}}
.source{{
  color:#6B7280;
  font-size:12px;
  background:#374151;
  padding:2px 8px;
  border-radius:6px;
  display:inline-block;
}}
.badge{{
  padding:3px 10px;
  border-radius:6px;
  font-size:11px;
  font-weight:600;
  letter-spacing:0.02em;
}}
.badge.win{{background:rgba(59,130,246,0.12);color:#60A5FA}}
.badge.loss{{background:rgba(239,68,68,0.12);color:#F87171}}
a.link-arrow{{
  color:#6B7280;
  text-decoration:none;
  font-size:16px;
  transition:color 0.15s;
}}
a.link-arrow:hover{{color:#3B82F6}}
.footer{{
  margin-top:32px;
  padding-top:20px;
  border-top:1px solid #1F2937;
  text-align:center;
  color:#4B5563;
  font-size:12px;
}}
</style></head><body>
<div class="container">

<div class="header">
  <div class="header-left">
    <span class="status-dot"></span>
    <h1>Quant Engine v3</h1>
  </div>
  <div class="header-right">
    <a href="/analytics" style="color:#6B7280;text-decoration:none;margin-right:16px">Analytics</a>
    <a href="/arbitrage" style="color:#6B7280;text-decoration:none;margin-right:16px">Arbitrage</a>
    {datetime.now(timezone.utc).strftime('%d.%m.%Y %H:%M UTC')}
  </div>
</div>

<div class="grid">
  <div class="card" title="Текущий баланс. Банкролл = свободные деньги + замороженные в открытых позициях">
    <div class="label">Bankroll</div>
    <div class="value num" style="color:{pc(roi)}">${stats['bankroll']:.2f}</div>
    <div class="sub">Start: ${start:.0f}</div>
  </div>
  <div class="card" title="Return on Investment — общая доходность в % от начального банкролла">
    <div class="label">ROI</div>
    <div class="value num" style="color:{pc(roi)}">{roi:+.2f}%</div>
    <div class="sub">P&amp;L: {stats['total_pnl']:+.2f}$</div>
  </div>
  <div class="card" title="Процент выигранных сделок из закрытых. >50% = прибыльно">
    <div class="label">Win Rate</div>
    <div class="value num" style="color:{'#3B82F6' if wr>=50 else '#EF4444'}">{wr}%</div>
    <div class="sub">{stats['wins']}W / {stats['losses']}L / {total} total</div>
  </div>
  <div class="card" title="Expected Value — средняя ожидаемая прибыль на сделку при входе. Kelly — % банкролла на ставку">
    <div class="label">Avg EV</div>
    <div class="value num" style="color:#3B82F6">+{stats['avg_ev']*100:.1f}%</div>
    <div class="sub">Avg Kelly: {stats['avg_kelly']*100:.1f}%</div>
  </div>
  <div class="card" title="Количество открытых позиций прямо сейчас">
    <div class="label">Открытые</div>
    <div class="value num" style="color:#F59E0B">{len(open_)}</div>
    <div class="sub">Max: {os.getenv('MAX_OPEN','5')}</div>
  </div>
</div>

<div class="panel" style="margin-bottom:20px;padding:20px">
  <div class="panel-header" style="padding:0 0 12px 0"><h2>Cumulative P&amp;L</h2></div>
  <div style="height:250px"><canvas id="pnlChart"></canvas></div>
</div>

<div class="panel">
  <div class="panel-header">
    <h2>Открытые позиции</h2>
    <span class="count">{len(open_)}</span>
  </div>
  <table>
    <tr><th>Вопрос</th><th title="YES = ставка на ДА, NO = ставка на НЕТ">Side</th><th title="Цена при входе в позицию">Вход</th><th title="Текущая рыночная цена">Сейчас</th><th title="Unrealized P&amp;L — нереализованная прибыль/убыток">uPnL</th><th title="Expected Value — ожидаемая прибыль при входе">EV</th><th title="KL-дивергенция — расхождение нашей оценки от рыночной цены. Чем выше, тем сильнее сигнал">KL</th><th title="Размер ставки в долларах">Ставка</th><th></th></tr>
    {open_rows}
  </table>
</div>

<div class="panel">
  <div class="panel-header">
    <h2>Последние сигналы</h2>
    <span class="count">{len(signals)}</span>
  </div>
  <table>
    <tr><th>Вопрос</th><th title="YES = ставка на ДА, NO = ставка на НЕТ">Side</th><th title="Текущая рыночная цена">Рынок</th><th title="Наша расчётная вероятность (после Байесовского анализа)">pTrue</th><th title="Expected Value — ожидаемая прибыль">EV</th><th title="KL-дивергенция — мера расхождения от рынка">KL</th><th title="Источник сигнала: math=математика, news=новости, claude=AI подтверждение">Источник</th></tr>
    {sig_rows}
  </table>
</div>

<div class="panel">
  <div class="panel-header">
    <h2>История</h2>
    <span class="count">{total_closed} total / page {page} of {total_pages}</span>
  </div>
  <table>
    <tr><th>Вопрос</th><th title="YES = ставка на ДА, NO = ставка на НЕТ">Side</th><th title="Цена при входе">Вход</th><th title="Как закрылась позиция: YES/NO = рынок решился, YES@65¢ = продали по цене">Исход</th><th title="Profit &amp; Loss — реальная прибыль или убыток">P&amp;L</th><th title="WIN = прибыль, LOSS = убыток">Итог</th><th title="Expected Value при входе">EV</th></tr>
    {closed_rows}
  </table>
  <div style="padding:16px 20px;display:flex;justify-content:center;gap:12px">
    {'<a href="/?page='+str(page-1)+'" style="color:#3B82F6;text-decoration:none">&larr; Prev</a>' if page > 1 else '<span style="color:#374151">&larr; Prev</span>'}
    <span style="color:#6B7280">Page {page}/{total_pages}</span>
    {'<a href="/?page='+str(page+1)+'" style="color:#3B82F6;text-decoration:none">Next &rarr;</a>' if page < total_pages else '<span style="color:#374151">Next &rarr;</span>'}
  </div>
</div>

<div class="footer">Quant Engine v3 &middot; {mode} Mode</div>

</div>
<script>
const pnlData = {to_json(pnl_data)};
if(pnlData.length > 0) {{
  const ctx = document.getElementById('pnlChart');
  new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: pnlData.map(d => new Date(d.t).toLocaleDateString('en', {{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}})),
      datasets: [{{
        label: 'Cumulative P&L ($)',
        data: pnlData.map(d => d.cum),
        borderColor: pnlData[pnlData.length-1].cum >= 0 ? '#3B82F6' : '#EF4444',
        backgroundColor: pnlData[pnlData.length-1].cum >= 0 ? 'rgba(59,130,246,0.08)' : 'rgba(239,68,68,0.08)',
        fill: true,
        tension: 0.3,
        pointRadius: pnlData.length > 50 ? 0 : 3,
        pointHoverRadius: 5,
        borderWidth: 2,
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: (c) => `P&L: ${{c.parsed.y >= 0 ? '+' : ''}}${{c.parsed.y.toFixed(2)}}`
          }}
        }}
      }},
      scales: {{
        x: {{ ticks: {{ color: '#6B7280', maxTicksLimit: 8, font: {{ size: 11 }} }}, grid: {{ color: 'rgba(55,65,81,0.3)' }} }},
        y: {{ ticks: {{ color: '#6B7280', callback: v => '$'+v.toFixed(0), font: {{ size: 11 }} }}, grid: {{ color: 'rgba(55,65,81,0.3)' }} }}
      }}
    }}
  }});
}}
</script>
</body></html>"""
  except Exception as e:
    log.error(f"[DASHBOARD] Render error: {e}", exc_info=True)
    return HTMLResponse(f"<h1>Dashboard Error</h1><pre>{e}</pre>", status_code=500)

@app.get("/analytics", response_class=HTMLResponse)
async def analytics():
  try:
    data = await _db.get_analytics()
    pnl_data = await _db.get_cumulative_pnl()
    sig_outcomes = await _db.get_signal_outcomes(limit=200)
    market_metrics = await _db.get_all_market_metrics()
    config_hist = await _db.get_config_history()
    config_map = {c["tag"]: c["params"] for c in config_hist}
    stats = await _db.get_stats()
    start = float(os.getenv("BANKROLL","1000"))
    roi   = ((stats["bankroll"]-start)/start*100)
    total = stats["wins"]+stats["losses"]
    wr    = round(stats["wins"]/total*100,1) if total>0 else 0

    def pc(v): return "#3B82F6" if v>=0 else "#EF4444"
    def wr_color(w, t): return "#3B82F6" if t>0 and w/t>=0.5 else "#EF4444" if t>0 else "#6B7280"

    # Theme table
    theme_rows = "".join([f"""<tr>
        <td>{r['theme']}</td>
        <td class="num">{r['total']}</td>
        <td class="num" style="color:{wr_color(r['wins'],r['total'])}">{r['wins']}/{r['total']} ({round(r['wins']/r['total']*100) if r['total']>0 else 0}%)</td>
        <td class="num" style="color:{pc(float(r['avg_pnl']))}">{float(r['avg_pnl']):+.2f}$</td>
    </tr>""" for r in data["by_theme"]]) or '<tr><td colspan="4" class="empty">Нет данных</td></tr>'

    # Source table
    source_rows = "".join([f"""<tr>
        <td>{r['source'] or 'unknown'}</td>
        <td class="num">{r['total']}</td>
        <td class="num" style="color:{wr_color(r['wins'],r['total'])}">{r['wins']}/{r['total']} ({round(r['wins']/r['total']*100) if r['total']>0 else 0}%)</td>
        <td class="num" style="color:{pc(float(r['avg_pnl']))}">{float(r['avg_pnl']):+.2f}$</td>
    </tr>""" for r in data["by_source"]]) or '<tr><td colspan="4" class="empty">Нет данных</td></tr>'

    # Side table
    side_rows = "".join([f"""<tr>
        <td class="{'yes' if r['side']=='YES' else 'no'}">{r['side']}</td>
        <td class="num">{r['total']}</td>
        <td class="num" style="color:{wr_color(r['wins'],r['total'])}">{r['wins']}/{r['total']} ({round(r['wins']/r['total']*100) if r['total']>0 else 0}%)</td>
        <td class="num" style="color:{pc(float(r['avg_pnl']))}">{float(r['avg_pnl']):+.2f}$</td>
    </tr>""" for r in data["by_side"]]) or '<tr><td colspan="4" class="empty">Нет данных</td></tr>'

    # Close reason
    reason_rows = "".join([f"""<tr>
        <td>{r['reason']}</td>
        <td class="num">{r['total']}</td>
        <td class="num" style="color:{pc(float(r['avg_pnl']))}">{float(r['avg_pnl']):+.2f}$</td>
    </tr>""" for r in data["by_reason"]]) or '<tr><td colspan="3" class="empty">Нет данных</td></tr>'

    # Calibration
    cal_rows = "".join([f"""<tr>
        <td>{r['bucket']}</td>
        <td class="num">{r['total']}</td>
        <td class="num">{float(r['avg_predicted'])*100:.1f}%</td>
        <td class="num" style="color:{pc(float(r['actual_wr'])-float(r['avg_predicted']))}">{float(r['actual_wr'])*100:.1f}%</td>
        <td class="num" style="color:{pc(float(r['actual_wr'])-float(r['avg_predicted']))}">{(float(r['actual_wr'])-float(r['avg_predicted']))*100:+.1f}%</td>
    </tr>""" for r in data["calibration"]]) or '<tr><td colspan="5" class="empty">Нет данных</td></tr>'

    # Daily PnL
    daily_rows = "".join([f"""<tr>
        <td>{r['day']}</td>
        <td class="num">{r['trades']}</td>
        <td class="num">{r['wins']}/{r['trades']} ({round(r['wins']/r['trades']*100) if r['trades']>0 else 0}%)</td>
        <td class="num" style="color:{pc(float(r['pnl']))}">{float(r['pnl']):+.2f}$</td>
    </tr>""" for r in data["daily_pnl"]]) or '<tr><td colspan="4" class="empty">Нет данных</td></tr>'

    # EV accuracy
    ev_pred = data["ev_predicted"]*100
    ev_act  = data["ev_actual"]*100

    # Config A/B comparison
    config_rows = ""
    for r in data["by_config"]:
        wr_val = round(r['wins']/r['total']*100) if r['total'] > 0 else 0
        tag = r['config_tag']
        params = config_map.get(tag, {})
        if isinstance(params, str):
            import json as _j
            params = _j.loads(params)
        param_str = f"EV≥{params.get('MIN_EV','')} KL≥{params.get('MIN_KL','')} Kelly:{params.get('MAX_KELLY_FRAC','')} SL:{params.get('STOP_LOSS_PCT','')}" if params else "—"
        config_rows += f"""<tr>
            <td style="font-weight:600" title="{param_str}">{tag}</td>
            <td class="num">{r['total']}</td>
            <td class="num" style="color:{wr_color(r['wins'],r['total'])}">{r['wins']}/{r['total']} ({wr_val}%)</td>
            <td class="num" style="color:{pc(float(r['total_pnl']))}">{float(r['total_pnl']):+.2f}$</td>
            <td class="num" style="color:{pc(float(r['avg_pnl']))}">{float(r['avg_pnl']):+.2f}$</td>
            <td class="num">{float(r['avg_ev'])*100:.1f}%</td>
            <td class="num">${float(r['avg_stake']):.2f}</td>
            <td class="q" style="font-size:11px">{param_str}</td>
        </tr>"""
    if not config_rows:
        config_rows = '<tr><td colspan="8" class="empty">No data yet</td></tr>'

    # Signal backtest — all signals with price_move data
    # Resolved markets are stable (use position outcome), active use live price
    valid_sigs = [s for s in sig_outcomes if s.get("price_move") is not None]
    exec_sigs = [s for s in valid_sigs if s["executed"]]
    rej_sigs = [s for s in valid_sigs if not s["executed"]]
    def _would_win(s): return s.get("price_move") and s["price_move"] > 0
    exec_right = sum(1 for s in exec_sigs if _would_win(s))
    rej_right = sum(1 for s in rej_sigs if _would_win(s))
    rej_saved = sum(1 for s in rej_sigs if not _would_win(s))

    bt_rows = ""
    for s in sig_outcomes[:50]:
        move = s.get("price_move") or 0
        won = move > 0
        status = "EXEC" if s["executed"] else "REJ"
        bt_rows += f"""<tr>
            <td class="q">{s['question'][:55]}...</td>
            <td class="{'yes' if s['side']=='YES' else 'no'}">{s['side']}</td>
            <td class="num">{s['side_price']*100:.1f}&#162;</td>
            <td class="num">{(s.get('current_price') or 0)*100:.1f}&#162;</td>
            <td class="num" style="color:{pc(move)}">{move*100:+.1f}&#162;</td>
            <td style="color:{'#3B82F6' if won else '#EF4444'}">{'RIGHT' if won else 'WRONG'}</td>
            <td style="color:{'#10B981' if s['executed'] else '#6B7280'}">{status}</td>
        </tr>"""
    if not bt_rows:
        bt_rows = '<tr><td colspan="7" class="empty">No signals</td></tr>'

    # Market metrics table
    mm_rows = ""
    for m in market_metrics[:50]:
        vol = m.get("volatility") or 0
        mom = m.get("momentum") or 0
        vr = m.get("vol_ratio") or 1.0
        q = (m.get("question") or "")[:55]
        price = (m.get("yes_price") or 0) * 100
        theme = m.get("theme") or "other"
        vol_color = "#EF4444" if vol > 0.02 else "#F59E0B" if vol > 0.005 else "#10B981"
        mom_color = "#3B82F6" if mom > 0 else "#EF4444" if mom < 0 else "#6B7280"
        mm_rows += f"""<tr>
            <td class="q">{q}...</td>
            <td class="num">{price:.1f}&#162;</td>
            <td class="num">{theme}</td>
            <td class="num" style="color:{vol_color}">{vol*100:.3f}%</td>
            <td class="num" style="color:{mom_color}">{mom*100:+.2f}%</td>
            <td class="num">{vr:.2f}x</td>
        </tr>"""
    if not mm_rows:
        mm_rows = '<tr><td colspan="6" class="empty">No metrics yet</td></tr>'

    return f"""<!DOCTYPE html>
<html lang="ru"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Analytics &mdash; Quant Engine v3</title>
<!-- auto-refresh disabled -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#111827;color:#E5E7EB;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;font-size:14px;line-height:1.5;-webkit-font-smoothing:antialiased}}
.container{{max-width:1400px;margin:0 auto;padding:24px 32px}}
.header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:28px;padding-bottom:20px;border-bottom:1px solid #1F2937}}
.header h1{{color:#F9FAFB;font-size:20px;font-weight:600;letter-spacing:-0.02em}}
.header a{{color:#6B7280;text-decoration:none;font-size:13px}}
.header a:hover{{color:#3B82F6}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:28px}}
.card{{background:#1F2937;border:1px solid #374151;border-radius:12px;padding:20px}}
.card .label{{color:#9CA3AF;font-size:12px;font-weight:500;letter-spacing:0.05em;text-transform:uppercase;margin-bottom:8px}}
.card .value{{font-family:'SF Mono',SFMono-Regular,ui-monospace,Menlo,Monaco,Consolas,monospace;font-size:28px;font-weight:700;letter-spacing:-0.02em}}
.card .sub{{color:#6B7280;font-size:12px;margin-top:6px}}
.row{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}}
@media(max-width:900px){{.row{{grid-template-columns:1fr}}}}
.panel{{background:#1F2937;border:1px solid #374151;border-radius:12px;overflow:hidden}}
.panel-header{{padding:16px 20px;border-bottom:1px solid #374151}}
.panel-header h2{{color:#D1D5DB;font-size:13px;font-weight:600;letter-spacing:0.03em;text-transform:uppercase}}
table{{width:100%;border-collapse:collapse}}
th{{color:#6B7280;text-align:left;padding:10px 16px;font-size:11px;font-weight:500;letter-spacing:0.05em;text-transform:uppercase;background:#1a2332;border-bottom:1px solid #374151}}
td{{padding:12px 16px;border-bottom:1px solid rgba(55,65,81,0.5);font-size:13px}}
tr:nth-child(even) td{{background:rgba(17,24,39,0.3)}}
tr:hover td{{background:rgba(55,65,81,0.3)}}
.num{{font-family:'SF Mono',SFMono-Regular,ui-monospace,Menlo,Monaco,Consolas,monospace;font-size:13px;font-variant-numeric:tabular-nums}}
.yes{{color:#3B82F6;font-weight:600}}.no{{color:#EF4444;font-weight:600}}
.empty{{text-align:center;color:#4B5563;padding:32px;font-style:italic}}
.footer{{margin-top:32px;padding-top:20px;border-top:1px solid #1F2937;text-align:center;color:#4B5563;font-size:12px}}
</style></head><body>
<div class="container">

<div class="header">
  <h1>Analytics</h1>
  <div><a href="/" style="color:#6B7280;text-decoration:none;margin-right:16px">&larr; Dashboard</a><a href="/arbitrage" style="color:#6B7280;text-decoration:none">Arbitrage</a></div>
</div>

<div class="grid">
  <div class="card" title="Процент выигранных сделок. >50% = прибыльно">
    <div class="label">Win Rate</div>
    <div class="value num" style="color:{'#3B82F6' if wr>=50 else '#EF4444'}">{wr}%</div>
    <div class="sub">{stats['wins']}W / {stats['losses']}L</div>
  </div>
  <div class="card" title="Средний Expected Value при входе — сколько модель обещала заработать на каждой сделке">
    <div class="label">EV Predicted</div>
    <div class="value num" style="color:#3B82F6">+{ev_pred:.1f}%</div>
    <div class="sub">Avg predicted EV at entry</div>
  </div>
  <div class="card" title="Реальная средняя доходность сделки. Если меньше EV Predicted — модель overconfident">
    <div class="label">EV Actual</div>
    <div class="value num" style="color:{pc(ev_act)}">{ev_act:+.1f}%</div>
    <div class="sub">Avg real return per trade</div>
  </div>
  <div class="card" title="Среднее время от открытия до закрытия позиции">
    <div class="label">Avg Lifetime</div>
    <div class="value num" style="color:#F59E0B">{data['avg_lifetime_hours']:.1f}h</div>
    <div class="sub">Avg position duration</div>
  </div>
</div>

<div class="panel" style="margin-bottom:20px">
  <div class="panel-header"><h2 title="Сравнение разных конфигураций. Меняй CONFIG_TAG в env при смене параметров чтобы отслеживать какая настройка лучше">Config A/B Testing</h2></div>
  <table>
    <tr><th title="Тег конфигурации (env CONFIG_TAG)">Config</th><th>Trades</th><th>W/L (WR)</th><th title="Суммарный P&amp;L">Total PnL</th><th title="Средний P&amp;L на сделку">Avg PnL</th><th title="Средний EV при входе">Avg EV</th><th title="Средний размер ставки">Avg Stake</th><th title="Ключевые параметры этого конфига">Params</th></tr>
    {config_rows}
  </table>
</div>

<div class="row">
  <div class="panel">
    <div class="panel-header"><h2 title="Какие темы рынков прибыльнее: crypto, iran, election и т.д.">Win Rate by Theme</h2></div>
    <table><tr><th>Theme</th><th>Trades</th><th title="Выигранные/Всего (Win Rate %)">W/L (WR)</th><th title="Средний P&amp;L на сделку в этой теме">Avg PnL</th></tr>{theme_rows}</table>
  </div>
  <div class="panel">
    <div class="panel-header"><h2 title="Откуда пришёл сигнал: math=математика, news=новости, claude=AI подтвердил">Win Rate by Source</h2></div>
    <table><tr><th>Source</th><th>Trades</th><th>W/L (WR)</th><th>Avg PnL</th></tr>{source_rows}</table>
  </div>
</div>

<div class="row">
  <div class="panel">
    <div class="panel-header"><h2 title="YES = ставка что событие произойдёт, NO = не произойдёт">Win Rate by Side</h2></div>
    <table><tr><th>Side</th><th>Trades</th><th>W/L (WR)</th><th>Avg PnL</th></tr>{side_rows}</table>
  </div>
  <div class="panel">
    <div class="panel-header"><h2 title="Как закрылись позиции: TAKE_PROFIT=забрали прибыль, STOP_LOSS=ограничили убыток, RESOLVED=рынок решился">Close Reason</h2></div>
    <table><tr><th title="TAKE_PROFIT: цена выросла до +20%. STOP_LOSS: цена упала на -25%. RESOLVED: рынок закрылся окончательно">Reason</th><th>Count</th><th>Avg PnL</th></tr>{reason_rows}</table>
  </div>
</div>

<div class="row">
  <div class="panel" style="padding:20px">
    <div class="panel-header" style="padding:0 0 12px 0"><h2>Cumulative P&amp;L</h2></div>
    <div style="height:280px"><canvas id="cumPnlChart"></canvas></div>
  </div>
  <div class="panel" style="padding:20px">
    <div class="panel-header" style="padding:0 0 12px 0"><h2>Daily P&amp;L</h2></div>
    <div style="height:280px"><canvas id="dailyPnlChart"></canvas></div>
  </div>
</div>

<div class="row">
  <div class="panel" style="padding:20px">
    <div class="panel-header" style="padding:0 0 12px 0"><h2>Calibration</h2></div>
    <div style="height:280px"><canvas id="calChart"></canvas></div>
  </div>
  <div class="panel" style="padding:20px">
    <div class="panel-header" style="padding:0 0 12px 0"><h2>Win Rate by Theme</h2></div>
    <div style="height:280px"><canvas id="themeChart"></canvas></div>
  </div>
</div>

<div class="row">
  <div class="panel" style="margin-bottom:20px">
    <div class="panel-header"><h2 title="Проверка точности модели: что мы предсказали vs что случилось. Если Bias положительный — модель overconfident">Calibration (table)</h2></div>
    <table><tr><th title="Диапазон предсказанной вероятности">Bucket</th><th>Trades</th><th title="Средняя вероятность которую предсказала модель">Avg Predicted</th><th title="Реальный процент выигрышей в этом диапазоне">Actual WR</th><th title="Разница: Actual - Predicted. + = модель недооценивает (хорошо), - = переоценивает (плохо)">Bias</th></tr>{cal_rows}</table>
  </div>
  <div class="panel">
    <div class="panel-header"><h2>Daily P&amp;L (table)</h2></div>
    <table><tr><th>Date</th><th>Trades</th><th>W/L (WR)</th><th>P&amp;L</th></tr>{daily_rows}</table>
  </div>
</div>

<div class="row">
  <div class="card" title="Сигналы которые мы исполнили — сколько из них цена двинулась в нашу сторону">
    <div class="label">Executed → Right</div>
    <div class="value num" style="color:#3B82F6">{exec_right}/{len(exec_sigs)}</div>
    <div class="sub">{round(exec_right/len(exec_sigs)*100) if exec_sigs else 0}% of executed moved our way</div>
  </div>
  <div class="card" title="Сигналы отвергнутые Claude, но цена двинулась в нашу сторону — мы упустили прибыль">
    <div class="label">Missed Profit</div>
    <div class="value num" style="color:#F59E0B">{rej_right}</div>
    <div class="sub">Rejected but would have won</div>
  </div>
  <div class="card" title="Сигналы отвергнутые Claude и цена пошла против нас — Claude спас от убытка">
    <div class="label">Saved by Rejection</div>
    <div class="value num" style="color:#10B981">{rej_saved}</div>
    <div class="sub">Rejected and would have lost</div>
  </div>
</div>

<div class="panel" style="margin-bottom:20px">
  <div class="panel-header" style="display:flex;justify-content:space-between;align-items:center">
    <h2>AI Analysis (Sonnet)</h2>
    <button id="runAnalysis" onclick="runSonnetAnalysis()" style="padding:8px 20px;background:#3B82F6;color:white;border:none;border-radius:6px;cursor:pointer;font-size:14px">Run Analysis</button>
  </div>
  <pre id="analysisResult" style="white-space:pre-wrap;color:#E5E7EB;font-size:13px;padding:12px;background:#1a1f2e;border-radius:6px;min-height:40px">Click "Run Analysis" to get Sonnet recommendations</pre>
</div>

<div class="panel">
  <div class="panel-header"><h2>Signal Backtest (last 50)</h2></div>
  <table><tr><th>Question</th><th>Side</th><th>Entry</th><th>Now</th><th>Move</th><th>Direction</th><th>Status</th></tr>
    {bt_rows}
  </table>
</div>

<div class="panel">
  <div class="panel-header"><h2 title="Волатильность и моментум по рынкам. Volatility = ATR (средний размер колебания). Momentum = направление тренда">Market Metrics (top 50)</h2></div>
  <table><tr>
    <th>Question</th><th>Price</th><th>Theme</th>
    <th title="ATR — средний размер колебания цены за 30 мин. Зелёный=спокойный, жёлтый=средний, красный=волатильный">Volatility</th>
    <th title="Направление тренда. Синий=растёт, красный=падает">Momentum</th>
    <th title="Объём сейчас / средний. >2.5x = кто-то что-то знает">Vol Ratio</th>
  </tr>
    {mm_rows}
  </table>
</div>

<div class="footer"><a href="/" style="color:#6B7280;text-decoration:none">Quant Engine v3</a> &middot; Analytics</div>

</div>
<script>
const pnlData = {to_json(pnl_data)};
const dailyData = {to_json(data['daily_pnl'])};
const calData = {to_json(data['calibration'])};
const themeData = {to_json(data['by_theme'])};

const chartColors = {{
  blue: '#3B82F6', red: '#EF4444', yellow: '#F59E0B', green: '#10B981',
  grid: 'rgba(55,65,81,0.3)', text: '#6B7280'
}};
const tickOpts = {{ color: chartColors.text, font: {{ size: 11 }} }};
const gridOpts = {{ color: chartColors.grid }};

// Cumulative PnL
if(pnlData.length > 0) {{
  new Chart(document.getElementById('cumPnlChart'), {{
    type: 'line',
    data: {{
      labels: pnlData.map(d => new Date(d.t).toLocaleDateString('en',{{month:'short',day:'numeric'}})),
      datasets: [{{
        label: 'Cumulative P&L',
        data: pnlData.map(d => d.cum),
        borderColor: pnlData[pnlData.length-1].cum>=0 ? chartColors.blue : chartColors.red,
        backgroundColor: pnlData[pnlData.length-1].cum>=0 ? 'rgba(59,130,246,0.08)' : 'rgba(239,68,68,0.08)',
        fill: true, tension: 0.3, pointRadius: pnlData.length>50?0:3, borderWidth: 2
      }}]
    }},
    options: {{
      responsive:true, maintainAspectRatio:false,
      plugins:{{ legend:{{display:false}}, tooltip:{{callbacks:{{label:c=>`$${{c.parsed.y>=0?'+':''}}${{c.parsed.y.toFixed(2)}}`}}}} }},
      scales:{{ x:{{ticks:{{...tickOpts,maxTicksLimit:8}},grid:gridOpts}}, y:{{ticks:{{...tickOpts,callback:v=>'$'+v}},grid:gridOpts}} }}
    }}
  }});
}}

// Daily PnL bars
if(dailyData.length > 0) {{
  const sorted = [...dailyData].reverse();
  new Chart(document.getElementById('dailyPnlChart'), {{
    type: 'bar',
    data: {{
      labels: sorted.map(d => d.day),
      datasets: [{{
        label: 'Daily P&L',
        data: sorted.map(d => parseFloat(d.pnl)),
        backgroundColor: sorted.map(d => parseFloat(d.pnl)>=0 ? 'rgba(59,130,246,0.7)' : 'rgba(239,68,68,0.7)'),
        borderRadius: 4
      }}]
    }},
    options: {{
      responsive:true, maintainAspectRatio:false,
      plugins:{{ legend:{{display:false}}, tooltip:{{callbacks:{{label:c=>`$${{c.parsed.y>=0?'+':''}}${{c.parsed.y.toFixed(2)}}`}}}} }},
      scales:{{ x:{{ticks:tickOpts,grid:gridOpts}}, y:{{ticks:{{...tickOpts,callback:v=>'$'+v}},grid:gridOpts}} }}
    }}
  }});
}}

// Calibration: predicted vs actual
if(calData.length > 0) {{
  new Chart(document.getElementById('calChart'), {{
    type: 'bar',
    data: {{
      labels: calData.map(d => d.bucket),
      datasets: [
        {{ label: 'Predicted', data: calData.map(d => (parseFloat(d.avg_predicted)*100).toFixed(1)), backgroundColor: 'rgba(59,130,246,0.6)', borderRadius: 4 }},
        {{ label: 'Actual WR', data: calData.map(d => (parseFloat(d.actual_wr)*100).toFixed(1)), backgroundColor: 'rgba(16,185,129,0.6)', borderRadius: 4 }}
      ]
    }},
    options: {{
      responsive:true, maintainAspectRatio:false,
      plugins:{{ legend:{{labels:{{color:chartColors.text}}}}, tooltip:{{callbacks:{{label:c=>c.dataset.label+': '+c.parsed.y+'%'}}}} }},
      scales:{{ x:{{ticks:tickOpts,grid:gridOpts}}, y:{{ticks:{{...tickOpts,callback:v=>v+'%'}},grid:gridOpts}} }}
    }}
  }});
}}

// Win rate by theme
if(themeData.length > 0) {{
  new Chart(document.getElementById('themeChart'), {{
    type: 'bar',
    data: {{
      labels: themeData.map(d => d.theme),
      datasets: [
        {{ label: 'Win Rate %', data: themeData.map(d => d.total>0 ? Math.round(d.wins/d.total*100) : 0), backgroundColor: themeData.map(d => d.total>0 && d.wins/d.total>=0.5 ? 'rgba(59,130,246,0.7)' : 'rgba(239,68,68,0.5)'), borderRadius: 4 }}
      ]
    }},
    options: {{
      responsive:true, maintainAspectRatio:false, indexAxis: 'y',
      plugins:{{ legend:{{display:false}}, tooltip:{{callbacks:{{label:c=>c.parsed.x+'% ('+themeData[c.dataIndex].wins+'/'+themeData[c.dataIndex].total+')'}}}} }},
      scales:{{ x:{{ticks:{{...tickOpts,callback:v=>v+'%'}},grid:gridOpts,max:100}}, y:{{ticks:tickOpts,grid:gridOpts}} }}
    }}
  }});
}}
async function runSonnetAnalysis() {{
  const btn = document.getElementById('runAnalysis');
  const result = document.getElementById('analysisResult');
  btn.disabled = true;
  btn.textContent = 'Running...';
  btn.style.background = '#6B7280';
  result.textContent = 'Requesting Sonnet analysis...';
  try {{
    const r = await fetch('/api/run-analysis', {{method: 'POST'}});
    const data = await r.json();
    if (data.analysis) {{
      result.textContent = data.analysis;
    }} else {{
      result.textContent = 'Error: ' + (data.error || 'Unknown error');
      result.style.color = '#EF4444';
    }}
  }} catch(e) {{
    result.textContent = 'Request failed: ' + e.message;
    result.style.color = '#EF4444';
  }}
  btn.disabled = false;
  btn.textContent = 'Run Analysis';
  btn.style.background = '#3B82F6';
}}
</script>
</body></html>"""
  except Exception as e:
    log.error(f"[DASHBOARD] Analytics error: {e}", exc_info=True)
    return HTMLResponse(f"<h1>Analytics Error</h1><pre>{e}</pre>", status_code=500)

@app.get("/arbitrage", response_class=HTMLResponse)
async def arbitrage(page: int = 1):
  try:
    per_page = 100
    stats = await _db.get_arb_stats()
    open_ = await _db.get_arb_open_positions()
    all_closed = await _db.get_arb_closed_positions(limit=per_page * page)
    closed = all_closed[(page-1)*per_page : page*per_page]
    total_closed = stats["wins"] + stats["losses"]
    total_pages = max(1, (total_closed + per_page - 1) // per_page)
    signals = await _db.get_arb_signals(limit=20)
    pnl_data = await _db.get_arb_cumulative_pnl()
    data = await _db.get_arb_analytics()

    arb_bankroll = float(os.getenv("BANKROLL", "1000"))
    roi = ((stats["bankroll"] - arb_bankroll) / arb_bankroll * 100) if arb_bankroll > 0 else 0
    total = stats["wins"] + stats["losses"]
    wr = round(stats["wins"] / total * 100, 1) if total > 0 else 0

    def pc(v): return "#3B82F6" if v >= 0 else "#EF4444"
    def wr_color(w, t): return "#3B82F6" if t > 0 and w / t >= 0.5 else "#EF4444" if t > 0 else "#6B7280"

    open_rows = "".join([f"""<tr>
        <td class="q">{p['question'][:70]}...</td>
        <td class="{'yes' if p['side']=='YES' else 'no'}">{p['side']}</td>
        <td class="num">{p['side_price']*100:.1f}&#162;</td>
        <td class="num">{(p.get('current_price') or p['side_price'])*100:.1f}&#162;</td>
        <td class="num" style="color:{pc(p.get('unrealized_pnl',0))}">{p.get('unrealized_pnl',0):+.2f}$</td>
        <td class="num ev">+{p['ev']*100:.1f}%</td>
        <td class="num">${p['stake_amt']:.2f}</td>
        <td class="source">{p.get('group_name','?')}</td>
    </tr>""" for p in open_]) or '<tr><td colspan="8" class="empty">Нет открытых позиций</td></tr>'

    closed_rows = "".join([f"""<tr>
        <td class="q">{t['question'][:60]}...</td>
        <td class="{'yes' if t['side']=='YES' else 'no'}">{t['side']}</td>
        <td class="num">{t['side_price']*100:.1f}&#162;</td>
        <td class="num">{t.get('close_reason','?')}</td>
        <td class="num" style="color:{pc(t['pnl'] or 0)}">{(t['pnl'] or 0):+.2f}$</td>
        <td><span class="badge {'win' if t['result']=='WIN' else 'loss'}">{t['result']}</span></td>
        <td class="num ev">+{t['ev']*100:.1f}%</td>
        <td class="source">{t.get('group_name','?')}</td>
    </tr>""" for t in reversed(closed)]) or '<tr><td colspan="8" class="empty">Нет сделок</td></tr>'

    sig_rows = "".join([f"""<tr>
        <td class="q">{s['question'][:60]}...</td>
        <td class="{'yes' if s['side']=='YES' else 'no'}">{s['side']}</td>
        <td class="num">{s['side_price']*100:.1f}&#162;</td>
        <td class="num ev">+{s['ev']*100:.1f}%</td>
        <td class="source">{s.get('group_name','?')}</td>
        <td class="q">{(s.get('leader_question') or '?')[:50]}</td>
        <td class="num">{(s.get('leader_move') or 0)*100:+.1f}&#162;</td>
        <td>{'✅' if s.get('executed') else '❌'}</td>
    </tr>""" for s in signals]) or '<tr><td colspan="8" class="empty">Нет сигналов</td></tr>'

    # Analytics tables
    group_rows = "".join([f"""<tr>
        <td>{r['group_name']}</td>
        <td class="num">{r['total']}</td>
        <td class="num" style="color:{wr_color(r['wins'],r['total'])}">{r['wins']}/{r['total']} ({round(r['wins']/r['total']*100) if r['total']>0 else 0}%)</td>
        <td class="num" style="color:{pc(float(r['avg_pnl']))}">{float(r['avg_pnl']):+.2f}$</td>
        <td class="num" style="color:{pc(float(r['total_pnl']))}">{float(r['total_pnl']):+.2f}$</td>
    </tr>""" for r in data["by_group"]]) or '<tr><td colspan="5" class="empty">Нет данных</td></tr>'

    reason_rows = "".join([f"""<tr>
        <td>{r['reason']}</td>
        <td class="num">{r['total']}</td>
        <td class="num" style="color:{wr_color(r['wins'],r['total'])}">{r['wins']}/{r['total']} ({round(r['wins']/r['total']*100) if r['total']>0 else 0}%)</td>
        <td class="num" style="color:{pc(float(r['avg_pnl']))}">{float(r['avg_pnl']):+.2f}$</td>
    </tr>""" for r in data["by_reason"]]) or '<tr><td colspan="4" class="empty">Нет данных</td></tr>'

    side_rows = "".join([f"""<tr>
        <td class="{'yes' if r['side']=='YES' else 'no'}">{r['side']}</td>
        <td class="num">{r['total']}</td>
        <td class="num" style="color:{wr_color(r['wins'],r['total'])}">{r['wins']}/{r['total']} ({round(r['wins']/r['total']*100) if r['total']>0 else 0}%)</td>
        <td class="num" style="color:{pc(float(r['avg_pnl']))}">{float(r['avg_pnl']):+.2f}$</td>
    </tr>""" for r in data["by_side"]]) or '<tr><td colspan="4" class="empty">Нет данных</td></tr>'

    daily_rows = "".join([f"""<tr>
        <td>{r['day']}</td>
        <td class="num">{r['trades']}</td>
        <td class="num" style="color:{wr_color(r['wins'],r['trades'])}">{r['wins']}/{r['trades']} ({round(r['wins']/r['trades']*100) if r['trades']>0 else 0}%)</td>
        <td class="num" style="color:{pc(float(r['pnl']))}">{float(r['pnl']):+.2f}$</td>
    </tr>""" for r in data["daily_pnl"]]) or '<tr><td colspan="4" class="empty">Нет данных</td></tr>'

    # Pagination
    page_links = ""
    if total_pages > 1:
        for p_num in range(1, total_pages + 1):
            if p_num == page:
                page_links += f'<span style="color:#3B82F6;font-weight:600;margin:0 4px">{p_num}</span>'
            else:
                page_links += f'<a href="/arbitrage?page={p_num}" style="color:#6B7280;text-decoration:none;margin:0 4px">{p_num}</a>'

    return f"""<!DOCTYPE html>
<html lang="ru"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Arbitrage Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{
  background:#111827;color:#E5E7EB;
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
  font-size:14px;line-height:1.5;-webkit-font-smoothing:antialiased;
}}
.container{{max-width:1400px;margin:0 auto;padding:24px 32px}}
.header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:28px;padding-bottom:20px;border-bottom:1px solid #1F2937}}
.header-left{{display:flex;align-items:center;gap:12px}}
.header h1{{color:#F9FAFB;font-size:20px;font-weight:600;letter-spacing:-0.02em}}
.status-dot{{width:8px;height:8px;border-radius:50%;background:#F59E0B;box-shadow:0 0 0 3px rgba(245,158,11,0.15);flex-shrink:0}}
.header-right{{color:#6B7280;font-size:13px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:28px}}
.card{{background:#1F2937;border:1px solid #374151;border-radius:12px;padding:20px;transition:border-color 0.15s ease}}
.card:hover{{border-color:#4B5563}}
.card .label{{color:#9CA3AF;font-size:12px;font-weight:500;letter-spacing:0.05em;text-transform:uppercase;margin-bottom:8px}}
.card .value{{font-family:'SF Mono',SFMono-Regular,ui-monospace,Menlo,Monaco,Consolas,monospace;font-size:28px;font-weight:700;letter-spacing:-0.02em}}
.card .sub{{color:#6B7280;font-size:12px;margin-top:6px}}
.panel{{background:#1F2937;border:1px solid #374151;border-radius:12px;margin-bottom:20px;overflow:hidden}}
.panel-header{{padding:16px 20px;border-bottom:1px solid #374151;display:flex;align-items:center;gap:8px}}
.panel-header h2{{color:#D1D5DB;font-size:13px;font-weight:600;letter-spacing:0.03em;text-transform:uppercase}}
.panel-header .count{{background:#374151;color:#9CA3AF;font-size:11px;font-weight:500;padding:2px 8px;border-radius:10px}}
table{{width:100%;border-collapse:collapse}}
th{{color:#6B7280;text-align:left;padding:10px 16px;font-size:11px;font-weight:500;letter-spacing:0.05em;text-transform:uppercase;background:#1a2332;border-bottom:1px solid #374151}}
td{{padding:12px 16px;border-bottom:1px solid rgba(55,65,81,0.5);vertical-align:middle;font-size:13px}}
tr:nth-child(even) td{{background:rgba(17,24,39,0.3)}}
tr:hover td{{background:rgba(55,65,81,0.3)}}
.empty{{text-align:center;color:#4B5563;padding:32px;font-style:italic}}
.num{{font-family:'SF Mono',SFMono-Regular,ui-monospace,Menlo,Monaco,Consolas,monospace;font-size:13px;font-variant-numeric:tabular-nums}}
.yes{{color:#3B82F6;font-weight:600}}.no{{color:#EF4444;font-weight:600}}
.ev{{color:#3B82F6}}.kl{{color:#F59E0B}}
.q{{color:#9CA3AF;font-size:12px;max-width:280px;line-height:1.4}}
.source{{color:#6B7280;font-size:12px;background:#374151;padding:2px 8px;border-radius:6px;display:inline-block}}
.badge{{padding:3px 10px;border-radius:6px;font-size:11px;font-weight:600;letter-spacing:0.02em}}
.badge.win{{background:rgba(59,130,246,0.12);color:#60A5FA}}
.badge.loss{{background:rgba(239,68,68,0.12);color:#F87171}}
.two-col{{display:grid;grid-template-columns:1fr 1fr;gap:20px}}
.footer{{margin-top:32px;padding-top:20px;border-top:1px solid #1F2937;text-align:center;color:#4B5563;font-size:12px}}
@media(max-width:900px){{.two-col{{grid-template-columns:1fr}}}}
</style></head><body>
<div class="container">

<div class="header">
  <div class="header-left">
    <span class="status-dot"></span>
    <h1>Arbitrage Bot</h1>
  </div>
  <div class="header-right">
    <a href="/" style="color:#6B7280;text-decoration:none;margin-right:16px">&larr; Dashboard</a>
    <a href="/analytics" style="color:#6B7280;text-decoration:none;margin-right:16px">Analytics</a>
    {datetime.now(timezone.utc).strftime('%d.%m.%Y %H:%M UTC')}
  </div>
</div>

<div class="grid">
  <div class="card">
    <div class="label">Arb Bankroll</div>
    <div class="value num" style="color:{pc(roi)}">${stats['bankroll']:.2f}</div>
    <div class="sub">Start: ${arb_bankroll:.0f} | ROI: {roi:+.1f}%</div>
  </div>
  <div class="card">
    <div class="label">Arb P&amp;L</div>
    <div class="value num" style="color:{pc(stats['total_pnl'])}">{stats['total_pnl']:+.2f}$</div>
    <div class="sub">{total} trades</div>
  </div>
  <div class="card">
    <div class="label">Win Rate</div>
    <div class="value num" style="color:{'#3B82F6' if wr>=50 else '#EF4444'}">{wr}%</div>
    <div class="sub">{stats['wins']}W / {stats['losses']}L</div>
  </div>
  <div class="card">
    <div class="label">Открытые</div>
    <div class="value num" style="color:#F59E0B">{len(open_)}</div>
    <div class="sub">Avg hold: {data['avg_lifetime_min']:.0f} min</div>
  </div>
</div>

<div class="panel" style="margin-bottom:20px;padding:20px">
  <div class="panel-header" style="padding:0 0 12px 0"><h2>Arb Cumulative P&amp;L</h2></div>
  <div style="height:250px"><canvas id="pnlChart"></canvas></div>
</div>

<div class="panel">
  <div class="panel-header"><h2>Открытые позиции</h2><span class="count">{len(open_)}</span></div>
  <table>
    <tr><th>Market</th><th>Side</th><th>Entry</th><th>Current</th><th>uPnL</th><th>EV</th><th>Stake</th><th>Group</th></tr>
    {open_rows}
  </table>
</div>

<div class="panel">
  <div class="panel-header"><h2>Последние сигналы</h2><span class="count">{len(signals)}</span></div>
  <table>
    <tr><th>Market</th><th>Side</th><th>Price</th><th>EV</th><th>Group</th><th>Leader</th><th>Move</th><th>Exec</th></tr>
    {sig_rows}
  </table>
</div>

<div class="two-col">
  <div class="panel">
    <div class="panel-header"><h2>По группам</h2></div>
    <table>
      <tr><th>Group</th><th>Total</th><th>Win Rate</th><th>Avg PnL</th><th>Total PnL</th></tr>
      {group_rows}
    </table>
  </div>
  <div class="panel">
    <div class="panel-header"><h2>По причине закрытия</h2></div>
    <table>
      <tr><th>Reason</th><th>Total</th><th>Win Rate</th><th>Avg PnL</th></tr>
      {reason_rows}
    </table>
  </div>
</div>

<div class="two-col">
  <div class="panel">
    <div class="panel-header"><h2>По стороне</h2></div>
    <table>
      <tr><th>Side</th><th>Total</th><th>Win Rate</th><th>Avg PnL</th></tr>
      {side_rows}
    </table>
  </div>
  <div class="panel">
    <div class="panel-header"><h2>Daily P&amp;L (14d)</h2></div>
    <table>
      <tr><th>Date</th><th>Trades</th><th>Win Rate</th><th>P&amp;L</th></tr>
      {daily_rows}
    </table>
  </div>
</div>

<div class="panel">
  <div class="panel-header"><h2>История</h2><span class="count">{total_closed}</span></div>
  <table>
    <tr><th>Market</th><th>Side</th><th>Entry</th><th>Close</th><th>P&amp;L</th><th>Result</th><th>EV</th><th>Group</th></tr>
    {closed_rows}
  </table>
  {"<div style='padding:16px;text-align:center'>" + page_links + "</div>" if total_pages > 1 else ""}
</div>

<div class="footer"><a href="/" style="color:#6B7280;text-decoration:none">Quant Engine v3</a> &middot; Arbitrage</div>
</div>

<script>
const pnlData = {to_json(pnl_data)};
if (pnlData.length > 0) {{
  new Chart(document.getElementById('pnlChart'), {{
    type: 'line',
    data: {{
      labels: pnlData.map(d => new Date(d.t).toLocaleString('ru-RU', {{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}})),
      datasets: [{{
        label: 'Cumulative P&L',
        data: pnlData.map(d => d.cum),
        borderColor: pnlData[pnlData.length-1].cum >= 0 ? '#3B82F6' : '#EF4444',
        backgroundColor: 'transparent',
        tension: 0.3,
        pointRadius: 2,
        borderWidth: 2
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        x: {{ ticks: {{ color: '#6B7280', maxTicksLimit: 10 }}, grid: {{ color: '#1F2937' }} }},
        y: {{ ticks: {{ color: '#6B7280', callback: v => '$' + v.toFixed(2) }}, grid: {{ color: '#1F2937' }} }}
      }}
    }}
  }});
}}
</script>
</body></html>"""
  except Exception as e:
    log.error(f"[DASHBOARD] Arbitrage error: {e}", exc_info=True)
    return HTMLResponse(f"<h1>Arbitrage Error</h1><pre>{e}</pre>", status_code=500)


@app.get("/api")
async def api_stats():
    try:
        stats  = await _db.get_stats()
        open_  = await _db.get_open_positions()
        closed = await _db.get_closed_positions(limit=5)
        return JSONResponse({"stats": stats, "open": len(open_), "recent": len(closed)})
    except Exception as e:
        log.warning(f"[DASHBOARD] API error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/run-analysis")
async def run_analysis():
    """Run Sonnet analysis on demand via dashboard button."""
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
                wr = round(r['wins']/r['total']*100) if r['total'] > 0 else 0
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


@app.on_event("startup")
async def startup():
    global _db
    _db = Database(os.getenv("DATABASE_URL"))
    await _db.init()
    log.info(f"[DASHBOARD] Ready on port {os.getenv('PORT', '3000')}")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "3000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
