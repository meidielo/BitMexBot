#!/bin/bash
# BitMexBot Weekly Report — runs via cron every Monday 9:00 AM
# Cron: 0 9 * * 1 /home/meidie/BitMexBot/weekly_report.sh >> /home/meidie/BitMexBot/logs/weekly_report.log 2>&1

cd /home/meidie/BitMexBot
source venv/bin/activate

DATE=$(date -u +"%Y-%m-%d %H:%M UTC")
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
echo "  bitmexbot:  $(systemctl is-active bitmexbot.service 2>/dev/null || echo 'unknown')"
echo "  bitmexdash: $(systemctl is-active bitmexdash.service 2>/dev/null || echo 'unknown')"
echo "  bitmexv4:   $(systemctl is-active bitmexv4.service 2>/dev/null || echo 'unknown')"

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

if [ -f data/condition_log.db ]; then
    COND_ROWS=$(sqlite3 data/condition_log.db "SELECT COUNT(*) FROM condition_log" 2>/dev/null)
    echo "  Condition log: ${COND_ROWS} rows"
else
    echo "  Condition log: not yet created"
fi

if [ -f data/coinalyze.db ]; then
    OI_ROWS=$(sqlite3 data/coinalyze.db "SELECT COUNT(*) FROM oi_15m_agg" 2>/dev/null)
    LIQ_ROWS=$(sqlite3 data/coinalyze.db "SELECT COUNT(*) FROM liquidations_15m_agg" 2>/dev/null)
    echo "  Coinalyze 15m: ${OI_ROWS} OI bars, ${LIQ_ROWS} liq bars"
fi

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
