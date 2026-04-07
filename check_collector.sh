#!/bin/bash
# check_collector.sh — Alert if Coinalyze collector has gone silent
#
# Checks heartbeat file mtime. If >30 min old, the pipeline is down.
# Run via cron: 0 * * * * ~/BitMexBot/check_collector.sh
#
# Alerts are logged to data/collector_alerts.log

HEARTBEAT="$HOME/BitMexBot/data/coinalyze_heartbeat.txt"
ALERT_LOG="$HOME/BitMexBot/data/collector_alerts.log"
MAX_AGE_SEC=1800  # 30 minutes

if [ ! -f "$HEARTBEAT" ]; then
    echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') [ALERT] Heartbeat file missing: $HEARTBEAT" >> "$ALERT_LOG"
    exit 1
fi

# File age in seconds
FILE_AGE=$(( $(date +%s) - $(stat -c %Y "$HEARTBEAT") ))

if [ "$FILE_AGE" -gt "$MAX_AGE_SEC" ]; then
    LAST_BEAT=$(cat "$HEARTBEAT" | head -1)
    echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') [ALERT] Collector stale: last heartbeat ${FILE_AGE}s ago ($LAST_BEAT)" >> "$ALERT_LOG"
    exit 1
fi
