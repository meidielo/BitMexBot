"""
dashboard.py

Lightweight web dashboard for the BitMEX trading bot.
Run alongside main.py on the remote PC — reads the same SQLite DB and log file.

    python dashboard.py

Then open from any device on your Tailscale network:
    http://<tailscale-ip-of-remote-pc>:5000

Requires:  pip install flask
"""

import json
import os
import sqlite3
from datetime import date, datetime, timezone

from flask import Flask, jsonify, render_template_string

# ---------------------------------------------------------------------------
# Paths  — adjust LOG_PATH if you changed the NSSM stdout location
# ---------------------------------------------------------------------------
DB_PATH    = os.path.join("data", "trades.db")
LOG_PATH   = os.path.join("logs",  "bot.log")
DAILY_JSON = os.path.join("data",  "daily_loss.json")

MAX_DAILY_LOSS_USD = 50.0
try:
    from risk import MAX_DAILY_LOSS_USD
except ImportError:
    pass

LOG_TAIL   = 120
DASH_PORT  = 5000

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _query(sql: str, params: tuple = ()) -> list:
    if not os.path.exists(DB_PATH):
        return []
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _log_tail() -> list:
    if not os.path.exists(LOG_PATH):
        return [f"[No log file found at {LOG_PATH}]"]
    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
        return [ln.rstrip() for ln in lines[-LOG_TAIL:]]
    except Exception as e:
        return [f"[Error reading log: {e}]"]


def _collect() -> dict:
    today = str(date.today())

    today_rows  = _query("SELECT * FROM trades WHERE date(timestamp)=? ORDER BY id DESC", (today,))
    open_pos    = _query("SELECT * FROM trades WHERE exit_price IS NULL AND order_status='placed' ORDER BY id DESC")
    recent      = _query("SELECT * FROM trades WHERE exit_price IS NOT NULL ORDER BY id DESC LIMIT 20")
    all_closed  = _query("SELECT pnl_usd FROM trades WHERE exit_price IS NOT NULL")

    today_closed = [r for r in today_rows if r["exit_price"] is not None]
    today_pnl    = sum(r["pnl_usd"] or 0 for r in today_closed)
    today_wins   = sum(1 for r in today_closed if (r["pnl_usd"] or 0) > 0)

    total_closed = len(all_closed)
    total_pnl    = sum(r["pnl_usd"] or 0 for r in all_closed)
    total_wins   = sum(1 for r in all_closed if (r["pnl_usd"] or 0) > 0)
    win_rate     = (total_wins / total_closed * 100) if total_closed else 0.0

    daily_loss_usd = abs(today_pnl) if today_pnl < 0 else 0.0
    if os.path.exists(DAILY_JSON):
        try:
            with open(DAILY_JSON) as fh:
                d = json.load(fh)
            if d.get("date") == today:
                daily_loss_usd = float(d["loss_usd"])
        except Exception:
            pass
    daily_loss_pct = min(daily_loss_usd / MAX_DAILY_LOSS_USD * 100, 100.0)
    halted = daily_loss_usd >= MAX_DAILY_LOSS_USD

    bot_alive = False
    if os.path.exists(LOG_PATH):
        age_s = (datetime.now().timestamp() - os.path.getmtime(LOG_PATH))
        bot_alive = age_s < 2100   # 35 min — bot sleeps up to 15 min per loop

    return {
        "now":             datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "today":           today,
        "bot_alive":       bot_alive,
        "halted":          halted,
        "today_attempts":  len(today_rows),
        "today_closed":    len(today_closed),
        "today_wins":      today_wins,
        "today_losses":    len(today_closed) - today_wins,
        "today_pnl":       round(today_pnl, 2),
        "total_closed":    total_closed,
        "total_pnl":       round(total_pnl, 2),
        "win_rate":        round(win_rate, 1),
        "daily_loss_usd":  round(daily_loss_usd, 2),
        "daily_loss_pct":  round(daily_loss_pct, 1),
        "open_positions":  open_pos,
        "recent_trades":   recent,
        "log_lines":       _log_tail(),
    }


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BitMEX Bot Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d0d0d; color: #e0e0e0; font-family: 'Segoe UI', monospace; font-size: 14px; }
  .header { background: #1a1a2e; padding: 14px 24px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid #2a2a4a; }
  .header h1 { font-size: 18px; color: #fff; letter-spacing: 1px; }
  .status-badge { padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: bold; }
  .status-ok   { background: #1b5e20; color: #a5d6a7; }
  .status-warn { background: #b71c1c; color: #ef9a9a; }
  .status-halt { background: #6a1c1c; color: #ff8a80; border: 1px solid #ff5252; }
  .ts { font-size: 12px; color: #888; }
  .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
  .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px; }
  .card { background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px; padding: 16px; }
  .card-title { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }
  .card-value { font-size: 26px; font-weight: bold; color: #fff; }
  .card-sub   { font-size: 12px; color: #666; margin-top: 4px; }
  .pos { color: #66bb6a; } .neg { color: #ef5350; } .neu { color: #fff; }
  .bar-wrap { background: #111; border-radius: 4px; height: 10px; overflow: hidden; margin-top: 8px; }
  .bar-fill { height: 100%; border-radius: 4px; transition: width 0.4s; }
  .bar-ok { background: #388e3c; } .bar-warn { background: #f57f17; } .bar-crit { background: #c62828; }
  .card h3 { font-size: 13px; color: #aaa; margin-bottom: 12px; font-weight: normal; text-transform: uppercase; letter-spacing: 1px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { color: #666; font-weight: normal; text-align: left; padding: 4px 8px; border-bottom: 1px solid #222; font-size: 11px; text-transform: uppercase; }
  td { padding: 6px 8px; border-bottom: 1px solid #1a1a1a; }
  tr:hover td { background: #1f1f1f; }
  .tag { display: inline-block; padding: 2px 7px; border-radius: 4px; font-size: 11px; font-weight: bold; }
  .tag-short { background: #b71c1c33; color: #ef5350; border: 1px solid #b71c1c; }
  .tag-long  { background: #1b5e2033; color: #66bb6a; border: 1px solid #1b5e20; }
  .tag-tp    { background: #1b5e2033; color: #66bb6a; }
  .tag-sl    { background: #b71c1c33; color: #ef5350; }
  .empty { color: #444; font-style: italic; padding: 12px 8px; }
  .log-box { background: #0a0a0a; border: 1px solid #1e1e1e; border-radius: 6px; padding: 12px; height: 420px; overflow-y: auto; font-family: 'Consolas', monospace; font-size: 12px; line-height: 1.6; }
  .log-line { white-space: pre-wrap; word-break: break-all; }
  .log-short { color: #ef5350; } .log-long { color: #66bb6a; } .log-notrade { color: #555; }
  .log-warn  { color: #ffb74d; } .log-ok { color: #4fc3f7; } .log-ml { color: #ce93d8; }
  .log-loop  { color: #fff; font-weight: bold; }
  .footer { text-align: center; color: #444; font-size: 12px; padding: 16px; }
</style>
</head>
<body>
<div class="header">
  <h1>BitMEX Bot &mdash; Dashboard</h1>
  <div style="display:flex;align-items:center;gap:16px;">
    <span class="ts" id="ts">--</span>
    <span class="status-badge" id="status-badge">Loading...</span>
  </div>
</div>
<div class="container">
  <div class="grid-4">
    <div class="card"><div class="card-title">Today PnL</div><div class="card-value" id="today-pnl">--</div><div class="card-sub" id="today-sub">--</div></div>
    <div class="card"><div class="card-title">All-time PnL</div><div class="card-value" id="total-pnl">--</div><div class="card-sub" id="total-sub">-- trades</div></div>
    <div class="card"><div class="card-title">Win Rate</div><div class="card-value" id="win-rate">--</div><div class="card-sub">all closed trades</div></div>
    <div class="card">
      <div class="card-title">Daily Loss Limit</div>
      <div class="card-value" id="daily-loss">--</div>
      <div class="card-sub" id="daily-sub">--</div>
      <div class="bar-wrap"><div class="bar-fill bar-ok" id="loss-bar" style="width:0%"></div></div>
    </div>
  </div>
  <div class="card" style="margin-bottom:20px;">
    <h3>Open Positions</h3>
    <table>
      <thead><tr><th>Order ID</th><th>Dir</th><th>Entry</th><th>SL</th><th>TP</th><th>Size (BTC)</th><th>Opened (UTC)</th></tr></thead>
      <tbody id="open-body"><tr><td class="empty" colspan="7">No open positions.</td></tr></tbody>
    </table>
  </div>
  <div class="grid-2">
    <div class="card">
      <h3>Recent Closed Trades</h3>
      <table>
        <thead><tr><th>Time</th><th>Dir</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Reason</th></tr></thead>
        <tbody id="trades-body"><tr><td class="empty" colspan="6">No trades yet.</td></tr></tbody>
      </table>
    </div>
    <div class="card">
      <h3>Live Log</h3>
      <div class="log-box" id="log-box">Loading...</div>
    </div>
  </div>
</div>
<div class="footer">Auto-refreshes every 30s &nbsp;|&nbsp; next in <span id="countdown">30</span>s</div>
<script>
let countdown = 30;
const MAX_LOSS = """ + str(MAX_DAILY_LOSS_USD) + """;

function pnlClass(v) { return v > 0 ? 'pos' : v < 0 ? 'neg' : 'neu'; }
function fmt(v) { if (v===null||v===undefined) return '--'; return (v>=0?'+':'')+v.toFixed(2); }
function esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function colorLog(line) {
  if (line.includes('[SHORT]'))    return `<span class="log-short">${esc(line)}</span>`;
  if (line.includes('[LONG]'))     return `<span class="log-long">${esc(line)}</span>`;
  if (line.includes('[NO_TRADE]')) return `<span class="log-notrade">${esc(line)}</span>`;
  if (line.includes('[ML]'))       return `<span class="log-ml">${esc(line)}</span>`;
  if (line.includes('[WARN]')||line.includes('[ERROR]')) return `<span class="log-warn">${esc(line)}</span>`;
  if (line.includes('[OK]')||line.includes('[LOG]'))     return `<span class="log-ok">${esc(line)}</span>`;
  if (line.includes('Loop #'))     return `<span class="log-loop">${esc(line)}</span>`;
  return `<span>${esc(line)}</span>`;
}
async function refresh() {
  let d;
  try { d = await (await fetch('/api/data')).json(); }
  catch(e) { document.getElementById('status-badge').textContent='Fetch error'; return; }

  document.getElementById('ts').textContent = d.now;
  const badge = document.getElementById('status-badge');
  if (d.halted) { badge.textContent='HALTED'; badge.className='status-badge status-halt'; }
  else if (d.bot_alive) { badge.textContent='Bot Running'; badge.className='status-badge status-ok'; }
  else { badge.textContent='Bot Inactive'; badge.className='status-badge status-warn'; }

  document.getElementById('today-pnl').innerHTML = `<span class="${pnlClass(d.today_pnl)}">$${fmt(d.today_pnl)}</span>`;
  document.getElementById('today-sub').textContent = `${d.today_closed} closed (${d.today_wins}W / ${d.today_losses}L)`;
  document.getElementById('total-pnl').innerHTML = `<span class="${pnlClass(d.total_pnl)}">$${fmt(d.total_pnl)}</span>`;
  document.getElementById('total-sub').textContent = `${d.total_closed} trades`;
  document.getElementById('win-rate').textContent = d.win_rate + '%';
  document.getElementById('daily-loss').textContent = '$' + d.daily_loss_usd.toFixed(2);
  document.getElementById('daily-sub').textContent = d.daily_loss_pct + '% of $' + MAX_LOSS.toFixed(2);
  const bar = document.getElementById('loss-bar');
  bar.style.width = d.daily_loss_pct + '%';
  bar.className = 'bar-fill ' + (d.daily_loss_pct>=100?'bar-crit':d.daily_loss_pct>=70?'bar-warn':'bar-ok');

  const ob = document.getElementById('open-body');
  ob.innerHTML = d.open_positions.length ? d.open_positions.map(r=>`<tr>
    <td style="font-size:11px;font-family:monospace">${esc(r.order_id||'--')}</td>
    <td><span class="tag tag-${r.signal.toLowerCase()}">${r.signal}</span></td>
    <td>${r.entry_price.toFixed(2)}</td><td>${r.sl_price.toFixed(2)}</td><td>${r.tp_price.toFixed(2)}</td>
    <td>${r.position_size_btc.toFixed(8)}</td><td>${esc(r.timestamp)}</td></tr>`).join('')
    : '<tr><td class="empty" colspan="7">No open positions.</td></tr>';

  const tb = document.getElementById('trades-body');
  tb.innerHTML = d.recent_trades.length ? d.recent_trades.map(r=>{
    const reason=(r.exit_reason||'--').toUpperCase();
    return `<tr>
      <td style="font-size:11px;color:#666">${esc((r.timestamp||'').slice(0,16))}</td>
      <td><span class="tag tag-${(r.signal||'').toLowerCase()}">${r.signal}</span></td>
      <td>${r.entry_price.toFixed(2)}</td><td>${(r.exit_price||0).toFixed(2)}</td>
      <td class="${pnlClass(r.pnl_usd)}">$${fmt(r.pnl_usd)}</td>
      <td><span class="tag ${reason==='TP'?'tag-tp':reason==='SL'?'tag-sl':''}">${reason}</span></td></tr>`;
  }).join('') : '<tr><td class="empty" colspan="6">No trades yet.</td></tr>';

  const log = document.getElementById('log-box');
  const atBottom = log.scrollHeight - log.clientHeight <= log.scrollTop + 40;
  log.innerHTML = d.log_lines.map(colorLog).map(l=>`<div class="log-line">${l}</div>`).join('');
  if (atBottom) log.scrollTop = log.scrollHeight;
}
setInterval(()=>{ countdown--; if(countdown<=0){countdown=30;refresh();} document.getElementById('countdown').textContent=countdown; },1000);
refresh();
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(_HTML)


@app.route("/api/data")
def api_data():
    return jsonify(_collect())


if __name__ == "__main__":
    print(f"Dashboard running at http://0.0.0.0:{DASH_PORT}")
    print(f"  DB  : {os.path.abspath(DB_PATH)}")
    print(f"  Log : {os.path.abspath(LOG_PATH)}")
    app.run(host="0.0.0.0", port=DASH_PORT, debug=False)
