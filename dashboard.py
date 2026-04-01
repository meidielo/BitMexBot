"""
dashboard.py

Web dashboard for the BitMEX trading bot.
Run alongside main.py on the remote PC — reads the same SQLite DB and log file.

    python dashboard.py

Then open from any device on your Tailscale network:
    http://<tailscale-ip-of-remote-pc>:5000

Requires:  pip install flask
"""

import json
import os
import re
import sqlite3
import time as _time
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template_string

# ---------------------------------------------------------------------------
# Paths
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
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()
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


def _parse_latest_diagnostics() -> dict:
    """Parse the last loop's diagnostics from the log file."""
    diag = {
        "candle": {},
        "indicators": {},
        "s1_short": [], "s1_long": [],
        "s2_short": [], "s2_long": [],
        "s3": [],
        "s4": [],
        "signal": "NO_TRADE",
        "signal_reason": "",
        "balance": 0,
        "loop": 0,
        "loop_time": "",
    }

    if not os.path.exists(LOG_PATH):
        return diag

    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
    except Exception:
        return diag

    # Find the last loop start
    last_loop_idx = -1
    for i in range(len(lines) - 1, -1, -1):
        if "Loop #" in lines[i]:
            last_loop_idx = i
            break

    if last_loop_idx < 0:
        return diag

    block = lines[last_loop_idx:]
    block_text = "".join(block)

    # Loop number and time
    m = re.search(r"Loop #(\d+)\s+.+?(\d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC)", block_text)
    if m:
        diag["loop"] = int(m.group(1))
        diag["loop_time"] = m.group(2)

    # Candle OHLC
    m = re.search(r"Candle\s+O=([\d.]+)\s+H=([\d.]+)\s+L=([\d.]+)\s+C=([\d.]+)", block_text)
    if m:
        diag["candle"] = {"o": float(m.group(1)), "h": float(m.group(2)),
                          "l": float(m.group(3)), "c": float(m.group(4))}

    # Indicators
    m = re.search(r"EMA20=([\d.]+)\s+EMA50=([\d.]+)\s+RSI=([\d.]+)", block_text)
    if m:
        diag["indicators"]["ema20"] = float(m.group(1))
        diag["indicators"]["ema50"] = float(m.group(2))
        diag["indicators"]["rsi"] = float(m.group(3))

    m = re.search(r"BB upper=([\d.]+)\s+mid=([\d.]+)\s+lower=([\d.]+)", block_text)
    if m:
        diag["indicators"]["bb_upper"] = float(m.group(1))
        diag["indicators"]["bb_mid"] = float(m.group(2))
        diag["indicators"]["bb_lower"] = float(m.group(3))

    m = re.search(r"ADX=([\d.]+)", block_text)
    if m:
        diag["indicators"]["adx"] = float(m.group(1))

    # Trend ages
    m = re.search(r"SHORT trend age:\s*(\d+)\s+\|\s+LONG trend age:\s*(\d+)", block_text)
    if m:
        diag["indicators"]["short_trend_age"] = int(m.group(1))
        diag["indicators"]["long_trend_age"] = int(m.group(2))

    # Wicks
    m = re.search(r"Upper wick=([\d.]+)\s+Lower wick=([\d.]+)\s+\(min=([\d.]+)", block_text)
    if m:
        diag["indicators"]["upper_wick"] = float(m.group(1))
        diag["indicators"]["lower_wick"] = float(m.group(2))
        diag["indicators"]["wick_min"] = float(m.group(3))

    # Parse PASS/FAIL conditions
    def parse_conditions(pattern_start, count=4):
        conds = []
        in_section = False
        for line in block:
            if pattern_start in line:
                in_section = True
                continue
            if in_section and ("[PASS]" in line or "[FAIL]" in line):
                passed = "[PASS]" in line
                label = line.strip().replace("[PASS] ", "").replace("[FAIL] ", "")
                conds.append({"pass": passed, "label": label})
                if len(conds) >= count:
                    break
            elif in_section and line.strip() == "":
                break
        return conds

    diag["s1_short"] = parse_conditions("SHORT conditions:", 4)
    diag["s1_long"] = parse_conditions("LONG conditions:", 4)
    diag["s2_short"] = parse_conditions("BB SHORT conditions:", 4)
    diag["s2_long"] = parse_conditions("BB LONG conditions:", 4)

    # S3 conditions
    s3_conds = []
    in_s3 = False
    for line in block:
        if "[S3] EMA crossover" in line:
            in_s3 = True
            continue
        if in_s3 and ("[PASS]" in line or "[FAIL]" in line):
            passed = "[PASS]" in line
            label = line.strip().replace("[PASS] ", "").replace("[FAIL] ", "")
            s3_conds.append({"pass": passed, "label": label})
        elif in_s3 and ("[S4]" in line or "[NO_TRADE]" in line):
            break
    diag["s3"] = s3_conds

    # S4 conditions
    s4_conds = []
    in_s4 = False
    for line in block:
        if "[S4] RSI reversal" in line:
            in_s4 = True
            continue
        if in_s4 and ("[PASS]" in line or "[FAIL]" in line):
            passed = "[PASS]" in line
            label = line.strip().replace("[PASS] ", "").replace("[FAIL] ", "")
            s4_conds.append({"pass": passed, "label": label})
        elif in_s4 and "[NO_TRADE]" in line:
            break
    diag["s4"] = s4_conds

    # Signal result
    m = re.search(r"Signal\s+:\s+(\w+)", block_text)
    if m:
        diag["signal"] = m.group(1)

    # Balance
    m = re.search(r"Balance\s+:\s+\$([\d.]+)", block_text)
    if m:
        diag["balance"] = float(m.group(1))

    return diag


def _collect() -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

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
        age_s = (_time.time() - os.path.getmtime(LOG_PATH))
        bot_alive = age_s < 2100

    # PnL history for equity curve
    pnl_history = _query(
        "SELECT timestamp, pnl_usd FROM trades WHERE exit_price IS NOT NULL ORDER BY id ASC"
    )
    equity_curve = []
    running = 0
    for r in pnl_history:
        running += (r["pnl_usd"] or 0)
        equity_curve.append({"t": r["timestamp"], "pnl": round(running, 2)})

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
        "diagnostics":     _parse_latest_diagnostics(),
        "equity_curve":    equity_curve,
    }


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

_HTML = r"""<!DOCTYPE html>
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
  .container { max-width: 1500px; margin: 0 auto; padding: 20px; }

  /* Cards */
  .grid-5 { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 20px; }
  .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
  .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 20px; }
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px; }
  .grid-2-1 { display: grid; grid-template-columns: 2fr 1fr; gap: 16px; margin-bottom: 20px; }
  .card { background: #1a1a1a; border: 1px solid #2a2a2a; border-radius: 8px; padding: 16px; }
  .card-title { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }
  .card-value { font-size: 26px; font-weight: bold; color: #fff; }
  .card-value-sm { font-size: 20px; font-weight: bold; color: #fff; }
  .card-sub   { font-size: 12px; color: #666; margin-top: 4px; }
  .pos { color: #66bb6a; } .neg { color: #ef5350; } .neu { color: #fff; }
  .bar-wrap { background: #111; border-radius: 4px; height: 10px; overflow: hidden; margin-top: 8px; }
  .bar-fill { height: 100%; border-radius: 4px; transition: width 0.4s; }
  .bar-ok { background: #388e3c; } .bar-warn { background: #f57f17; } .bar-crit { background: #c62828; }
  .card h3 { font-size: 13px; color: #aaa; margin-bottom: 12px; font-weight: normal; text-transform: uppercase; letter-spacing: 1px; }

  /* Gauge */
  .gauge-row { display: flex; align-items: center; gap: 10px; margin-bottom: 10px; }
  .gauge-label { font-size: 11px; color: #888; width: 80px; text-transform: uppercase; }
  .gauge-track { flex: 1; height: 12px; background: #111; border-radius: 6px; position: relative; overflow: hidden; }
  .gauge-fill { height: 100%; border-radius: 6px; transition: width 0.5s; }
  .gauge-val { font-size: 13px; font-weight: bold; width: 60px; text-align: right; }
  .gauge-marker { position: absolute; top: 0; height: 100%; width: 2px; background: #fff; z-index: 2; }

  /* Strategy conditions */
  .strat-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-bottom: 16px; }
  .strat-card { background: #111; border-radius: 6px; padding: 10px; border: 1px solid #222; }
  .strat-header { font-size: 12px; font-weight: bold; color: #aaa; margin-bottom: 8px; display: flex; justify-content: space-between; }
  .strat-score { font-size: 11px; padding: 2px 6px; border-radius: 4px; }
  .strat-cond { font-size: 11px; padding: 3px 0; display: flex; align-items: center; gap: 6px; }
  .cond-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .cond-pass { background: #66bb6a; box-shadow: 0 0 4px #66bb6a; }
  .cond-fail { background: #444; }
  .cond-text { color: #999; font-size: 10px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

  /* Price display */
  .price-big { font-size: 32px; font-weight: bold; color: #fff; }
  .price-label { font-size: 11px; color: #888; margin-top: 2px; }

  /* Candle visualization */
  .candle-vis { display: flex; align-items: center; justify-content: center; height: 100px; padding: 10px; }
  .candle-stick { width: 2px; background: #888; position: relative; }
  .candle-body { width: 20px; position: absolute; left: -9px; border-radius: 2px; }
  .candle-green { background: #66bb6a; border: 1px solid #43a047; }
  .candle-red { background: #ef5350; border: 1px solid #c62828; }

  /* Tables */
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

  /* Log */
  .log-box { background: #0a0a0a; border: 1px solid #1e1e1e; border-radius: 6px; padding: 12px; height: 350px; overflow-y: auto; font-family: 'Consolas', monospace; font-size: 12px; line-height: 1.6; }
  .log-line { white-space: pre-wrap; word-break: break-all; }
  .log-short { color: #ef5350; } .log-long { color: #66bb6a; } .log-notrade { color: #555; }
  .log-warn  { color: #ffb74d; } .log-ok { color: #4fc3f7; } .log-ml { color: #ce93d8; }
  .log-loop  { color: #fff; font-weight: bold; }
  .log-pass  { color: #66bb6a; } .log-fail  { color: #ef5350; }
  .log-s2    { color: #42a5f5; } .log-s3 { color: #ab47bc; } .log-s4 { color: #ff7043; }

  /* Equity chart */
  .equity-chart { width: 100%; height: 120px; position: relative; }
  .equity-svg { width: 100%; height: 100%; }

  .footer { text-align: center; color: #444; font-size: 12px; padding: 16px; }

  @media (max-width: 900px) {
    .grid-5, .grid-4, .strat-grid { grid-template-columns: repeat(2, 1fr); }
    .grid-2, .grid-2-1 { grid-template-columns: 1fr; }
  }
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

  <!-- Row 1: Key metrics -->
  <div class="grid-5">
    <div class="card">
      <div class="card-title">Price</div>
      <div class="price-big" id="price">--</div>
      <div class="price-label" id="price-label">BTC/USDT</div>
    </div>
    <div class="card">
      <div class="card-title">Today PnL</div>
      <div class="card-value" id="today-pnl">--</div>
      <div class="card-sub" id="today-sub">--</div>
    </div>
    <div class="card">
      <div class="card-title">All-time PnL</div>
      <div class="card-value" id="total-pnl">--</div>
      <div class="card-sub" id="total-sub">-- trades</div>
    </div>
    <div class="card">
      <div class="card-title">Win Rate</div>
      <div class="card-value" id="win-rate">--</div>
      <div class="card-sub">all closed trades</div>
    </div>
    <div class="card">
      <div class="card-title">Balance</div>
      <div class="card-value" id="balance">--</div>
      <div class="card-sub" id="loop-info">--</div>
    </div>
  </div>

  <!-- Row 2: Indicator gauges + Daily loss -->
  <div class="grid-4">
    <div class="card">
      <div class="card-title">RSI (14)</div>
      <div class="gauge-row">
        <div class="gauge-track" style="position:relative">
          <div class="gauge-fill" id="rsi-fill" style="width:50%;background:#666"></div>
          <div style="position:absolute;top:0;left:30%;height:100%;width:1px;background:#ef535066"></div>
          <div style="position:absolute;top:0;left:70%;height:100%;width:1px;background:#66bb6a66"></div>
        </div>
        <div class="gauge-val" id="rsi-val">--</div>
      </div>
      <div class="card-sub">Oversold &lt;30 | Overbought &gt;70</div>
    </div>
    <div class="card">
      <div class="card-title">ADX (14)</div>
      <div class="gauge-row">
        <div class="gauge-track">
          <div class="gauge-fill" id="adx-fill" style="width:0%;background:#666"></div>
        </div>
        <div class="gauge-val" id="adx-val">--</div>
      </div>
      <div class="card-sub">Trending &gt;25 | Strong &gt;40</div>
    </div>
    <div class="card">
      <div class="card-title">EMA Trend</div>
      <div class="card-value-sm" id="ema-trend">--</div>
      <div class="card-sub" id="ema-detail">--</div>
    </div>
    <div class="card">
      <div class="card-title">Daily Loss Limit</div>
      <div class="card-value-sm" id="daily-loss">--</div>
      <div class="card-sub" id="daily-sub">--</div>
      <div class="bar-wrap"><div class="bar-fill bar-ok" id="loss-bar" style="width:0%"></div></div>
    </div>
  </div>

  <!-- Row 3: Strategy condition status -->
  <div class="card" style="margin-bottom:20px;">
    <h3>Strategy Conditions (Latest Candle)</h3>
    <div class="strat-grid">
      <div class="strat-card">
        <div class="strat-header">
          <span>S1: EMA Rejection</span>
          <span class="strat-score" id="s1-short-score" style="background:#222;color:#888">0/4</span>
        </div>
        <div style="font-size:10px;color:#ef5350;margin-bottom:4px">SHORT</div>
        <div id="s1-short-conds"></div>
        <div style="font-size:10px;color:#66bb6a;margin-top:6px;margin-bottom:4px">LONG</div>
        <div id="s1-long-conds"></div>
      </div>
      <div class="strat-card">
        <div class="strat-header">
          <span>S2: BB Bounce</span>
          <span class="strat-score" id="s2-short-score" style="background:#222;color:#888">0/4</span>
        </div>
        <div style="font-size:10px;color:#ef5350;margin-bottom:4px">SHORT</div>
        <div id="s2-short-conds"></div>
        <div style="font-size:10px;color:#66bb6a;margin-top:6px;margin-bottom:4px">LONG</div>
        <div id="s2-long-conds"></div>
      </div>
      <div class="strat-card">
        <div class="strat-header">
          <span>S3: EMA Cross</span>
          <span class="strat-score" id="s3-score" style="background:#222;color:#888">0/?</span>
        </div>
        <div id="s3-conds"></div>
      </div>
      <div class="strat-card">
        <div class="strat-header">
          <span>S4: RSI Reversal</span>
          <span class="strat-score" id="s4-score" style="background:#222;color:#888">0/?</span>
        </div>
        <div id="s4-conds"></div>
      </div>
    </div>
  </div>

  <!-- Row 4: Price context (BB position + wick info) -->
  <div class="grid-3">
    <div class="card">
      <div class="card-title">Bollinger Band Position</div>
      <div id="bb-visual" style="margin-top:8px">
        <div class="gauge-row">
          <div class="gauge-label">Upper</div>
          <div class="gauge-val" id="bb-upper-val" style="width:auto;color:#ef5350">--</div>
        </div>
        <div class="gauge-row">
          <div class="gauge-track" style="height:24px;position:relative;background:#111;border:1px solid #222">
            <div id="bb-price-marker" style="position:absolute;width:12px;height:12px;border-radius:50%;background:#4fc3f7;top:6px;left:50%;transform:translateX(-50%);box-shadow:0 0 6px #4fc3f7;z-index:2"></div>
            <div id="bb-mid-marker" style="position:absolute;top:0;height:100%;width:1px;background:#ffffff44;left:50%"></div>
          </div>
        </div>
        <div class="gauge-row">
          <div class="gauge-label">Lower</div>
          <div class="gauge-val" id="bb-lower-val" style="width:auto;color:#66bb6a">--</div>
        </div>
      </div>
      <div class="card-sub" id="bb-pct-label">--</div>
    </div>
    <div class="card">
      <div class="card-title">Wick Analysis</div>
      <div style="margin-top:8px">
        <div class="gauge-row">
          <div class="gauge-label">Upper</div>
          <div class="gauge-track"><div class="gauge-fill" id="uwick-fill" style="width:0%;background:#ef5350"></div></div>
          <div class="gauge-val" id="uwick-val">--</div>
        </div>
        <div class="gauge-row">
          <div class="gauge-label">Lower</div>
          <div class="gauge-track"><div class="gauge-fill" id="lwick-fill" style="width:0%;background:#66bb6a"></div></div>
          <div class="gauge-val" id="lwick-val">--</div>
        </div>
      </div>
      <div class="card-sub" id="wick-min-label">Min required: --</div>
    </div>
    <div class="card">
      <div class="card-title">Trend Age</div>
      <div style="margin-top:8px">
        <div class="gauge-row">
          <div class="gauge-label" style="color:#ef5350">Short</div>
          <div class="gauge-track"><div class="gauge-fill" id="sage-fill" style="width:0%;background:#ef5350"></div></div>
          <div class="gauge-val" id="sage-val">--</div>
        </div>
        <div class="gauge-row">
          <div class="gauge-label" style="color:#66bb6a">Long</div>
          <div class="gauge-track"><div class="gauge-fill" id="lage-fill" style="width:0%;background:#66bb6a"></div></div>
          <div class="gauge-val" id="lage-val">--</div>
        </div>
      </div>
      <div class="card-sub">Min required: 15 candles</div>
    </div>
  </div>

  <!-- Row 5: Equity curve -->
  <div class="card" style="margin-bottom:20px;">
    <h3>Equity Curve</h3>
    <div class="equity-chart" id="equity-chart">
      <div style="color:#444;font-style:italic;padding:30px;text-align:center" id="equity-placeholder">No closed trades yet. Equity curve will appear after first trade.</div>
      <svg class="equity-svg" id="equity-svg" style="display:none"></svg>
    </div>
  </div>

  <!-- Row 6: Open positions -->
  <div class="card" style="margin-bottom:20px;">
    <h3>Open Positions</h3>
    <table>
      <thead><tr><th>Order ID</th><th>Dir</th><th>Entry</th><th>SL</th><th>TP</th><th>Size (BTC)</th><th>Opened (UTC)</th></tr></thead>
      <tbody id="open-body"><tr><td class="empty" colspan="7">No open positions.</td></tr></tbody>
    </table>
  </div>

  <!-- Row 7: Trades + Log -->
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
  if (line.includes('[S2]'))         return `<span class="log-s2">${esc(line)}</span>`;
  if (line.includes('[S3]'))         return `<span class="log-s3">${esc(line)}</span>`;
  if (line.includes('[S4]'))         return `<span class="log-s4">${esc(line)}</span>`;
  if (line.includes('[SHORT]'))      return `<span class="log-short">${esc(line)}</span>`;
  if (line.includes('[LONG]'))       return `<span class="log-long">${esc(line)}</span>`;
  if (line.includes('[PASS]'))       return `<span class="log-pass">${esc(line)}</span>`;
  if (line.includes('[FAIL]'))       return `<span class="log-fail">${esc(line)}</span>`;
  if (line.includes('[NO_TRADE]'))   return `<span class="log-notrade">${esc(line)}</span>`;
  if (line.includes('[ML]'))         return `<span class="log-ml">${esc(line)}</span>`;
  if (line.includes('[WARN]')||line.includes('[ERROR]')) return `<span class="log-warn">${esc(line)}</span>`;
  if (line.includes('[OK]')||line.includes('[LOG]'))     return `<span class="log-ok">${esc(line)}</span>`;
  if (line.includes('Loop #'))       return `<span class="log-loop">${esc(line)}</span>`;
  return `<span>${esc(line)}</span>`;
}

function renderConds(el, conds) {
  if (!conds || !conds.length) { el.innerHTML = '<div style="color:#444;font-size:10px">No data</div>'; return 0; }
  let pass = 0;
  el.innerHTML = conds.map(c => {
    if (c.pass) pass++;
    return `<div class="strat-cond"><div class="cond-dot ${c.pass?'cond-pass':'cond-fail'}"></div><span class="cond-text">${esc(c.label)}</span></div>`;
  }).join('');
  return pass;
}

function renderEquity(data) {
  const svg = document.getElementById('equity-svg');
  const placeholder = document.getElementById('equity-placeholder');
  if (!data || !data.length) { svg.style.display = 'none'; placeholder.style.display = 'block'; return; }
  svg.style.display = 'block'; placeholder.style.display = 'none';

  const w = svg.clientWidth || 600, h = svg.clientHeight || 120;
  const vals = data.map(d => d.pnl);
  const mn = Math.min(0, ...vals), mx = Math.max(0, ...vals);
  const range = mx - mn || 1;
  const pad = 10;

  let pts = data.map((d, i) => {
    const x = pad + (i / Math.max(data.length - 1, 1)) * (w - 2*pad);
    const y = h - pad - ((d.pnl - mn) / range) * (h - 2*pad);
    return `${x},${y}`;
  }).join(' ');

  const zeroY = h - pad - ((0 - mn) / range) * (h - 2*pad);
  svg.innerHTML = `
    <line x1="${pad}" y1="${zeroY}" x2="${w-pad}" y2="${zeroY}" stroke="#333" stroke-dasharray="4"/>
    <polyline fill="none" stroke="#4fc3f7" stroke-width="2" points="${pts}"/>
    <text x="${w-pad}" y="${zeroY-4}" fill="#555" font-size="10" text-anchor="end">$0</text>
    <text x="${w-pad}" y="${h-2}" fill="#666" font-size="10" text-anchor="end">$${fmt(vals[vals.length-1])}</text>
  `;
}

async function refresh() {
  let d;
  try { d = await (await fetch('/api/data')).json(); }
  catch(e) { document.getElementById('status-badge').textContent='Fetch error'; return; }

  const diag = d.diagnostics || {};
  const ind = diag.indicators || {};

  // Header
  document.getElementById('ts').textContent = d.now;
  const badge = document.getElementById('status-badge');
  if (d.halted) { badge.textContent='HALTED'; badge.className='status-badge status-halt'; }
  else if (d.bot_alive) { badge.textContent='Bot Running'; badge.className='status-badge status-ok'; }
  else { badge.textContent='Bot Inactive'; badge.className='status-badge status-warn'; }

  // Price
  const price = (diag.candle || {}).c;
  document.getElementById('price').textContent = price ? '$' + price.toFixed(2) : '--';
  document.getElementById('price-label').textContent = price ? 'BTC/USDT 15m' : 'BTC/USDT';

  // PnL cards
  document.getElementById('today-pnl').innerHTML = `<span class="${pnlClass(d.today_pnl)}">$${fmt(d.today_pnl)}</span>`;
  document.getElementById('today-sub').textContent = `${d.today_closed} closed (${d.today_wins}W / ${d.today_losses}L)`;
  document.getElementById('total-pnl').innerHTML = `<span class="${pnlClass(d.total_pnl)}">$${fmt(d.total_pnl)}</span>`;
  document.getElementById('total-sub').textContent = `${d.total_closed} trades`;
  document.getElementById('win-rate').textContent = d.win_rate + '%';
  document.getElementById('balance').textContent = diag.balance ? '$' + diag.balance.toFixed(2) : '--';
  document.getElementById('loop-info').textContent = diag.loop ? `Loop #${diag.loop} | ${diag.loop_time}` : '--';

  // RSI gauge
  const rsi = ind.rsi || 50;
  document.getElementById('rsi-val').textContent = rsi.toFixed(1);
  document.getElementById('rsi-fill').style.width = rsi + '%';
  const rsiColor = rsi < 30 ? '#66bb6a' : rsi > 70 ? '#ef5350' : rsi < 40 ? '#81c784' : rsi > 60 ? '#e57373' : '#666';
  document.getElementById('rsi-fill').style.background = rsiColor;

  // ADX gauge
  const adx = ind.adx || 0;
  document.getElementById('adx-val').textContent = adx.toFixed(1);
  document.getElementById('adx-fill').style.width = Math.min(adx, 60) / 60 * 100 + '%';
  document.getElementById('adx-fill').style.background = adx >= 40 ? '#66bb6a' : adx >= 25 ? '#ffb74d' : '#666';

  // EMA trend
  const ema20 = ind.ema20 || 0, ema50 = ind.ema50 || 0;
  const trendEl = document.getElementById('ema-trend');
  if (ema20 > ema50) { trendEl.innerHTML = '<span class="pos">UPTREND</span>'; }
  else if (ema20 < ema50) { trendEl.innerHTML = '<span class="neg">DOWNTREND</span>'; }
  else { trendEl.innerHTML = '<span class="neu">NEUTRAL</span>'; }
  document.getElementById('ema-detail').textContent = ema20 ? `EMA20: ${ema20.toFixed(0)} | EMA50: ${ema50.toFixed(0)} | Gap: ${(ema20-ema50).toFixed(0)}` : '--';

  // Daily loss
  document.getElementById('daily-loss').textContent = '$' + d.daily_loss_usd.toFixed(2);
  document.getElementById('daily-sub').textContent = d.daily_loss_pct + '% of $' + MAX_LOSS.toFixed(2);
  const bar = document.getElementById('loss-bar');
  bar.style.width = d.daily_loss_pct + '%';
  bar.className = 'bar-fill ' + (d.daily_loss_pct>=100?'bar-crit':d.daily_loss_pct>=70?'bar-warn':'bar-ok');

  // BB position
  const bbU = ind.bb_upper || 0, bbM = ind.bb_mid || 0, bbL = ind.bb_lower || 0;
  document.getElementById('bb-upper-val').textContent = bbU ? bbU.toFixed(0) : '--';
  document.getElementById('bb-lower-val').textContent = bbL ? bbL.toFixed(0) : '--';
  if (price && bbU && bbL && bbU > bbL) {
    const pct = ((price - bbL) / (bbU - bbL)) * 100;
    document.getElementById('bb-price-marker').style.left = Math.max(2, Math.min(98, pct)) + '%';
    document.getElementById('bb-mid-marker').style.left = '50%';
    document.getElementById('bb-pct-label').textContent = `Price at ${pct.toFixed(0)}% of band width | Mid: ${bbM.toFixed(0)}`;
  }

  // Wick analysis
  const uw = ind.upper_wick || 0, lw = ind.lower_wick || 0, wm = ind.wick_min || 1;
  document.getElementById('uwick-val').textContent = uw.toFixed(1);
  document.getElementById('lwick-val').textContent = lw.toFixed(1);
  document.getElementById('uwick-fill').style.width = Math.min(uw / (wm * 3) * 100, 100) + '%';
  document.getElementById('lwick-fill').style.width = Math.min(lw / (wm * 3) * 100, 100) + '%';
  document.getElementById('uwick-fill').style.background = uw >= wm ? '#ef5350' : '#444';
  document.getElementById('lwick-fill').style.background = lw >= wm ? '#66bb6a' : '#444';
  document.getElementById('wick-min-label').textContent = `Min required: ${wm.toFixed(1)} pts (0.05% of close)`;

  // Trend age
  const sa = ind.short_trend_age || 0, la = ind.long_trend_age || 0;
  document.getElementById('sage-val').textContent = sa;
  document.getElementById('lage-val').textContent = la;
  document.getElementById('sage-fill').style.width = Math.min(sa / 30 * 100, 100) + '%';
  document.getElementById('lage-fill').style.width = Math.min(la / 30 * 100, 100) + '%';
  document.getElementById('sage-fill').style.background = sa >= 15 ? '#ef5350' : '#444';
  document.getElementById('lage-fill').style.background = la >= 15 ? '#66bb6a' : '#444';

  // Strategy conditions
  const s1sp = renderConds(document.getElementById('s1-short-conds'), diag.s1_short);
  const s1lp = renderConds(document.getElementById('s1-long-conds'), diag.s1_long);
  const s1best = Math.max(s1sp, s1lp);
  const s1sc = document.getElementById('s1-short-score');
  s1sc.textContent = `${s1best}/4`;
  s1sc.style.background = s1best >= 4 ? '#1b5e20' : s1best >= 3 ? '#33691e44' : '#222';
  s1sc.style.color = s1best >= 4 ? '#66bb6a' : s1best >= 3 ? '#aed581' : '#888';

  const s2sp = renderConds(document.getElementById('s2-short-conds'), diag.s2_short);
  const s2lp = renderConds(document.getElementById('s2-long-conds'), diag.s2_long);
  const s2best = Math.max(s2sp, s2lp);
  const s2sc = document.getElementById('s2-short-score');
  s2sc.textContent = `${s2best}/4`;
  s2sc.style.background = s2best >= 4 ? '#1b5e20' : s2best >= 3 ? '#33691e44' : '#222';
  s2sc.style.color = s2best >= 4 ? '#66bb6a' : s2best >= 3 ? '#aed581' : '#888';

  const s3p = renderConds(document.getElementById('s3-conds'), diag.s3);
  const s3t = (diag.s3 || []).length || '?';
  const s3sc = document.getElementById('s3-score');
  s3sc.textContent = `${s3p}/${s3t}`;
  s3sc.style.background = s3p >= s3t && s3t > 0 ? '#1b5e20' : '#222';
  s3sc.style.color = s3p >= s3t && s3t > 0 ? '#66bb6a' : '#888';

  const s4p = renderConds(document.getElementById('s4-conds'), diag.s4);
  const s4t = (diag.s4 || []).length || '?';
  const s4sc = document.getElementById('s4-score');
  s4sc.textContent = `${s4p}/${s4t}`;
  s4sc.style.background = s4p >= s4t && s4t > 0 ? '#1b5e20' : '#222';
  s4sc.style.color = s4p >= s4t && s4t > 0 ? '#66bb6a' : '#888';

  // Equity curve
  renderEquity(d.equity_curve);

  // Open positions
  const ob = document.getElementById('open-body');
  ob.innerHTML = d.open_positions.length ? d.open_positions.map(r=>`<tr>
    <td style="font-size:11px;font-family:monospace">${esc(r.order_id||'--')}</td>
    <td><span class="tag tag-${r.signal.toLowerCase()}">${r.signal}</span></td>
    <td>${r.entry_price.toFixed(2)}</td><td>${r.sl_price.toFixed(2)}</td><td>${r.tp_price.toFixed(2)}</td>
    <td>${r.position_size_btc.toFixed(8)}</td><td>${esc(r.timestamp)}</td></tr>`).join('')
    : '<tr><td class="empty" colspan="7">No open positions.</td></tr>';

  // Recent trades
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

  // Log
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
    bind_host = "0.0.0.0"
    try:
        import subprocess
        ts_ip = subprocess.check_output(
            ["tailscale", "ip", "-4"], text=True, timeout=5
        ).strip()
        bind_host = ts_ip
    except Exception:
        pass

    print(f"Dashboard running at http://{bind_host}:{DASH_PORT}")
    print(f"  DB  : {os.path.abspath(DB_PATH)}")
    print(f"  Log : {os.path.abspath(LOG_PATH)}")
    app.run(host=bind_host, port=DASH_PORT, debug=False)
