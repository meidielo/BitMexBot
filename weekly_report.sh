#!/bin/bash
# BitMexBot Weekly Report — runs via cron every Monday 9:00 AM
# Cron: 0 9 * * 1 /home/meidie/BitMexBot/weekly_report.sh >> /home/meidie/BitMexBot/logs/weekly_report.log 2>&1

cd /home/meidie/BitMexBot
source venv/bin/activate

DATE=$(date -u +"%Y-%m-%d %H:%M UTC")
STATE_FILE="data/weekly_report_state.json"

echo ""
echo "================================================================"
echo "  BitMexBot Weekly Update — $DATE"
echo "================================================================"

# Regime
echo ""
echo "## Regime Status"
python3 -c "
from bitmex_client import get_data_client
from fetch_data import fetch_recent_funding
import pandas as pd
e = get_data_client()
raw = e.fetch_ohlcv('BTC/USDT:USDT', '1d', limit=250)
df = pd.DataFrame(raw, columns=['ts','o','h','l','c','v'])
ema = df['c'].ewm(span=200, adjust=False).mean().iloc[-1]
close = df['c'].iloc[-1]
gap = (close - ema) / ema * 100
regime = 'BULL' if close > ema else 'BEAR'
print(f'  Close: \${close:,.0f} | EMA200: \${ema:,.0f} | Gap: {gap:+.1f}% | Regime: {regime}')
try:
    df_f = fetch_recent_funding(count=10)
    if df_f is not None and not df_f.empty:
        print(f'  Funding: {df_f[\"rate\"].iloc[-1]*100:+.4f}% (latest) | {df_f[\"rate\"].max()*100:+.4f}% (peak 10)')
except: print('  Funding: unavailable')
" 2>/dev/null

# Services
echo ""
echo "## Services"
if command -v docker >/dev/null 2>&1 && docker compose ps >/dev/null 2>&1; then
    for svc in bitmexbot bitmexdash; do
        CID=$(docker compose ps -q "$svc" 2>/dev/null)
        if [ -n "$CID" ]; then
            STATE=$(docker inspect -f '{{.State.Status}}' "$CID" 2>/dev/null)
            HEALTH=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "$CID" 2>/dev/null)
            echo "  ${svc}: docker ${STATE:-unknown} (${HEALTH:-unknown})"
        else
            echo "  ${svc}: docker not running"
        fi
    done
else
    for svc in bitmexbot bitmexdash; do
        STATUS=$(systemctl is-active ${svc}.service 2>/dev/null) || true
        echo "  ${svc}: systemd ${STATUS:-unknown}"
    done
fi
echo "  bitmexv4: retired"

# Container inventory
echo ""
echo "## Container Inventory"
if command -v docker >/dev/null 2>&1; then
    docker ps --format '  {{.Names}}: {{.Status}}' | sort
else
    echo "  Docker: unavailable"
fi

# Data pipeline
echo ""
echo "## Data Pipeline"
if [ -f coinalyze_collector.py ]; then
    if [ -f data/coinalyze_heartbeat.txt ]; then
        AGE_SEC=$(( $(date +%s) - $(stat -c %Y data/coinalyze_heartbeat.txt) ))
        AGE_MIN=$(( AGE_SEC / 60 ))
        if [ $AGE_MIN -gt 120 ]; then
            echo "  Coinalyze collector: STALE (${AGE_MIN}m old)"
        else
            echo "  Coinalyze collector: OK (${AGE_MIN}m old)"
        fi
    else
        echo "  Coinalyze collector: NO HEARTBEAT FILE"
    fi
else
    echo "  Coinalyze collector: retired or missing script"
fi

# Use Python for all SQLite queries (sqlite3 CLI not reliably in PATH)
python3 -c "
import sqlite3, os, json

state_file = '$STATE_FILE'
prev = {}
if os.path.exists(state_file):
    try:
        with open(state_file) as f:
            prev = json.load(f)
    except: pass

current = {}

# Condition log
if os.path.exists('data/condition_log.db'):
    conn = sqlite3.connect('data/condition_log.db', timeout=5)
    rows = conn.execute('SELECT COUNT(*) FROM condition_log').fetchone()[0]
    conn.close()
    prev_rows = prev.get('condition_log', 0)
    delta = rows - prev_rows
    delta_str = f' (+{delta})' if prev_rows > 0 and delta >= 0 else ''
    print(f'  Condition log: {rows:,} rows{delta_str}')
    current['condition_log'] = rows
else:
    print('  Condition log: not yet created')

# Coinalyze 15m
if os.path.exists('data/coinalyze.db'):
    conn = sqlite3.connect('data/coinalyze.db', timeout=5)
    oi = conn.execute('SELECT COUNT(*) FROM oi_15m_agg').fetchone()[0]
    liq = conn.execute('SELECT COUNT(*) FROM liquidations_15m_agg').fetchone()[0]
    conn.close()
    prev_oi = prev.get('oi_15m', 0)
    prev_liq = prev.get('liq_15m', 0)
    oi_d = f' (+{oi - prev_oi})' if prev_oi > 0 else ''
    liq_d = f' (+{liq - prev_liq})' if prev_liq > 0 else ''
    print(f'  Coinalyze 15m: {oi:,} OI bars{oi_d}, {liq:,} liq bars{liq_d}')
    current['oi_15m'] = oi
    current['liq_15m'] = liq

# Save state for next week's delta
with open(state_file, 'w') as f:
    json.dump(current, f)
" 2>/dev/null

# Codebase health
echo ""
echo "## Codebase Health"
UNIT_LOG=$(mktemp)
PYTEST_LOG=$(mktemp)
python -m unittest test_logger test_risk -v >"$UNIT_LOG" 2>&1
UNIT_STATUS=$?
python -m pytest test_signals.py -q >"$PYTEST_LOG" 2>&1
PYTEST_STATUS=$?
if [ "$UNIT_STATUS" -eq 0 ] && [ "$PYTEST_STATUS" -eq 0 ]; then
    UNIT_COUNT=$(grep -E '^Ran [0-9]+ tests' "$UNIT_LOG" | tail -1 | awk '{print $2}')
    PYTEST_OUT=$(tail -1 "$PYTEST_LOG")
    TEST_OUT="unittest ${UNIT_COUNT:-?} passed; pytest ${PYTEST_OUT}"
else
    TEST_OUT="FAILED: unittest exit ${UNIT_STATUS}, pytest exit ${PYTEST_STATUS}"
fi
rm -f "$UNIT_LOG" "$PYTEST_LOG"
echo "  Tests: $TEST_OUT"
COMMITS_7D=$(git log --oneline --since="7 days ago" 2>/dev/null | wc -l)
echo "  Commits (last 7d): $COMMITS_7D"
LESSONS=$(grep -c "^## L" tasks/lessons.md 2>/dev/null)
echo "  Lessons documented: $LESSONS"

# Strategy status
echo ""
echo "## Strategy Status"
echo "  V2 Funding Mean-Reversion: regime-silent (funding at baseline)"
echo "  V4 Cascade Dip-Buy: data-blocked at N=4"
echo "  Graveyard: 9 families killed (L01-L30)"
echo "  Active edge: none — waiting for regime change or new hypothesis"
python3 <<'PY' 2>/dev/null
import datetime as dt
import os
import sqlite3

trades = 'data/trades.db'
conditions = 'data/condition_log.db'
last_iso = '1970-01-01T00:00:00'

if os.path.exists(trades):
    conn = sqlite3.connect(trades)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        'SELECT timestamp, signal, entry_price, exit_price, pnl_usd '
        'FROM trades ORDER BY timestamp DESC LIMIT 1'
    ).fetchone()
    open_count = conn.execute(
        'SELECT COUNT(*) FROM trades WHERE exit_price IS NULL'
    ).fetchone()[0]
    conn.close()
    if row:
        last = dt.datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=dt.timezone.utc)
        last_iso = row['timestamp'].replace(' ', 'T')
        days = (dt.datetime.now(dt.timezone.utc) - last).total_seconds() / 86400
        timestamp = row['timestamp']
        signal = row['signal']
        entry_price = row['entry_price']
        print(f'  Last trade: {timestamp} UTC ({days:.1f} days ago), {signal} at {entry_price:,.1f}')
        print(f'  Trade log unclosed rows: {open_count} (historical log cleanup, not live exposure)')

try:
    from bitmex_client import get_client
    exchange = get_client()
    positions = exchange.fetch_positions(['XBTUSDT'])
    active = [p for p in positions if float(p.get('contracts') or 0) != 0]
    open_orders = exchange.fetch_open_orders('XBTUSDT')
    print(f'  Exchange live exposure: {len(active)} active positions, {len(open_orders)} open orders')
except Exception as exc:
    print(f'  Exchange live exposure: unavailable ({type(exc).__name__})')

if os.path.exists(conditions):
    conn = sqlite3.connect(conditions)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT condition_name,
               COUNT(*) AS checks,
               SUM(passed) AS passes,
               ROUND(100.0 * SUM(passed) / COUNT(*), 2) AS pass_rate,
               MAX(current_value) AS max_value,
               AVG(threshold) AS avg_threshold
        FROM condition_log
        WHERE timestamp >= ?
        GROUP BY condition_name
        ORDER BY pass_rate ASC, condition_name
        LIMIT 1
        """
    , (last_iso,)).fetchone()
    conn.close()
    if row:
        condition_name = row['condition_name']
        passes = row['passes']
        checks = row['checks']
        pass_rate = row['pass_rate']
        max_value = row['max_value']
        avg_threshold = row['avg_threshold']
        print(
            f'  Main blocker since last trade: {condition_name} '
            f'passed {passes}/{checks} checks '
            f'({pass_rate:.2f}%). '
            f'Max value {max_value:.6f}, threshold {avg_threshold:.6f}.'
        )
PY

# Git status
echo ""
echo "## Git"
DIRTY=$(git status --short 2>/dev/null | wc -l)
if [ "$DIRTY" -eq 0 ]; then
    echo "  Working tree: clean"
else
    echo "  Working tree: $DIRTY uncommitted changes"
fi
echo "  HEAD: $(git log --oneline -1 2>/dev/null)"

echo ""
echo "================================================================"
echo "  End of weekly report"
echo "================================================================"
echo ""
