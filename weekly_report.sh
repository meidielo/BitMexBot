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
for svc in bitmexbot bitmexdash bitmexv4; do
    STATUS=$(systemctl is-active ${svc}.service 2>/dev/null) || true
    echo "  ${svc}: ${STATUS:-unknown}"
done

# Data pipeline
echo ""
echo "## Data Pipeline"
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
TEST_OUT=$(python -m pytest test_risk.py test_signals.py test_v4_recovery.py -q 2>&1 | tail -1)
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
