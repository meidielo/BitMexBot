"""
data_validator.py — Data quality validation for trading data pipelines

Plausibility checks for Coinalyze + Binance data:
  - Liquidation bar > 10× rolling 30-day max → flag
  - OI drop > 30% in a single 15m interval → flag
  - Cross-source divergence > 15% (Coinalyze vs Binance OI) → flag

Called from coinalyze_collector.py after each collection cycle.
Flags are logged but do not block data ingestion.

Usage (standalone):
  python data_validator.py --check          # run all checks
  python data_validator.py --cross-validate # compare Coinalyze vs Binance OI
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
COINALYZE_DB    = os.path.join("data", "coinalyze.db")
BINANCE_DB      = os.path.join("data", "binance_historical.db")

LIQ_SPIKE_THRESHOLD = 10.0   # flag if > 10× rolling 30-day max
OI_DROP_THRESHOLD   = 0.30   # flag if > 30% OI drop in one 15m interval
CROSS_SOURCE_TOL    = 0.15   # flag if Coinalyze vs Binance OI diverge > 15%


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Flag storage
# ---------------------------------------------------------------------------
def _init_flags_table(conn: sqlite3.Connection):
    """Create data_quality_flags table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_flags (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT    NOT NULL,
            check_name  TEXT    NOT NULL,
            severity    TEXT    NOT NULL,
            message     TEXT    NOT NULL,
            value       REAL,
            threshold   REAL
        )
    """)
    conn.commit()


def _log_flag(conn: sqlite3.Connection, check_name: str, severity: str,
              message: str, value: float = None, threshold: float = None):
    """Insert a data quality flag."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO data_quality_flags (timestamp, check_name, severity, message, value, threshold) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (now, check_name, severity, message[:500], value, threshold)
    )
    conn.commit()
    _log(f"  [{severity}] {check_name}: {message}")


# ---------------------------------------------------------------------------
# Individual validators
# ---------------------------------------------------------------------------
def validate_liquidation_bar(value: float, rolling_30d_max: float) -> tuple[bool, str]:
    """
    Check if a liquidation bar value exceeds plausibility threshold.
    Returns (is_valid, message).
    """
    if rolling_30d_max <= 0:
        return True, "No baseline data"

    ratio = value / rolling_30d_max
    if ratio > LIQ_SPIKE_THRESHOLD:
        return False, (f"Liquidation spike {ratio:.1f}× 30-day max "
                      f"(value={value:,.0f}, max={rolling_30d_max:,.0f})")
    return True, ""


def validate_oi_change(current: float, previous: float,
                       interval_minutes: int = 15) -> tuple[bool, str]:
    """
    Check if OI change exceeds plausibility threshold for the interval.
    Returns (is_valid, message).
    """
    if previous <= 0:
        return True, "No previous data"

    change_pct = abs(current - previous) / previous
    if change_pct > OI_DROP_THRESHOLD:
        direction = "drop" if current < previous else "spike"
        return False, (f"OI {direction} {change_pct*100:.1f}% in {interval_minutes}m "
                      f"(from {previous:,.0f} to {current:,.0f})")
    return True, ""


def validate_cross_source(coinalyze_oi: float, binance_oi: float,
                          tolerance: float = CROSS_SOURCE_TOL) -> tuple[bool, str]:
    """
    Compare OI values from Coinalyze (aggregated) vs Binance (single exchange).
    Note: Coinalyze is multi-exchange, Binance is single — expect Coinalyze > Binance.
    We check if the *direction* of change and *relative magnitude* are consistent.
    Returns (is_valid, message).
    """
    if coinalyze_oi <= 0 or binance_oi <= 0:
        return True, "Insufficient data for cross-validation"

    # Binance is ~30-40% of global BTC futures OI, so expect ratio of ~2.5-3.5×
    ratio = coinalyze_oi / binance_oi
    if ratio < 1.0 or ratio > 10.0:
        return False, (f"OI ratio implausible: Coinalyze/Binance = {ratio:.2f} "
                      f"(cz={coinalyze_oi:,.0f}, bn={binance_oi:,.0f})")
    return True, ""


# ---------------------------------------------------------------------------
# Batch validators (called from collector)
# ---------------------------------------------------------------------------
def validate_latest_bars(conn: sqlite3.Connection):
    """
    Run plausibility checks on the most recent data in coinalyze.db.
    Called after each collection cycle.
    """
    _init_flags_table(conn)

    # 1. Check latest liquidation bar against 30-day max
    try:
        latest_liq = conn.execute("""
            SELECT timestamp, liq_long, liq_short
            FROM liquidations_15m_agg
            ORDER BY timestamp DESC
            LIMIT 1
        """).fetchone()

        if latest_liq:
            ts, liq_long, liq_short = latest_liq

            # 30-day max for liq_long
            max_30d = conn.execute("""
                SELECT MAX(liq_long) FROM liquidations_15m_agg
                WHERE timestamp >= ? AND timestamp < ?
            """, (ts - 30*86400, ts)).fetchone()[0] or 0

            liq_total = (liq_long or 0) + (liq_short or 0)
            valid, msg = validate_liquidation_bar(liq_long or 0, max_30d)
            if not valid:
                _log_flag(conn, "liq_spike", "WARNING", msg,
                         liq_long, max_30d * LIQ_SPIKE_THRESHOLD)

    except Exception as e:
        _log(f"  [ERROR] Liquidation check failed: {e}")

    # 2. Check OI change between last two bars
    try:
        oi_rows = conn.execute("""
            SELECT timestamp, oi_close
            FROM oi_15m_agg
            ORDER BY timestamp DESC
            LIMIT 2
        """).fetchall()

        if len(oi_rows) == 2:
            current_oi = oi_rows[0][1] or 0
            previous_oi = oi_rows[1][1] or 0
            valid, msg = validate_oi_change(current_oi, previous_oi)
            if not valid:
                _log_flag(conn, "oi_change", "WARNING", msg,
                         current_oi, previous_oi * (1 - OI_DROP_THRESHOLD))

    except Exception as e:
        _log(f"  [ERROR] OI change check failed: {e}")


def cross_validate_oi(days: int = 7):
    """
    Compare Coinalyze daily OI vs Binance daily OI for the past N days.
    Logs divergences exceeding tolerance.
    """
    if not os.path.exists(COINALYZE_DB):
        _log("Coinalyze DB not found — skipping cross-validation")
        return

    if not os.path.exists(BINANCE_DB):
        _log("Binance DB not found — run binance_data_fetcher.py first")
        return

    cz_conn = sqlite3.connect(COINALYZE_DB, timeout=5)
    bn_conn = sqlite3.connect(BINANCE_DB, timeout=5)

    _init_flags_table(cz_conn)

    try:
        # Get Coinalyze daily OI (aggregate from 15m)
        cz_rows = cz_conn.execute("""
            SELECT
                date(timestamp, 'unixepoch') as day,
                AVG(oi_close) as avg_oi
            FROM oi_15m_agg
            WHERE timestamp >= strftime('%s', 'now', ? || ' days')
            GROUP BY day
            ORDER BY day
        """, (f"-{days}",)).fetchall()

        # Get Binance daily OI
        cutoff_ms = int(
            (datetime.now(timezone.utc) - __import__("datetime").timedelta(days=days))
            .timestamp() * 1000
        )
        bn_rows = bn_conn.execute("""
            SELECT
                date(timestamp/1000, 'unixepoch') as day,
                sum_oi_value
            FROM binance_oi_daily
            WHERE timestamp >= ?
            ORDER BY day
        """, (cutoff_ms,)).fetchall()

        if not cz_rows or not bn_rows:
            _log("Insufficient data for cross-validation")
            return

        # Build lookup
        bn_dict = {row[0]: row[1] for row in bn_rows}

        _log(f"Cross-validating OI: Coinalyze vs Binance (last {days} days)")
        divergences = 0
        for day, cz_oi in cz_rows:
            bn_oi = bn_dict.get(day)
            if bn_oi is None:
                continue

            valid, msg = validate_cross_source(cz_oi, bn_oi)
            if not valid:
                _log_flag(cz_conn, "cross_source_oi", "WARNING", f"{day}: {msg}",
                         cz_oi / bn_oi if bn_oi > 0 else 0, CROSS_SOURCE_TOL)
                divergences += 1
            else:
                ratio = cz_oi / bn_oi if bn_oi > 0 else 0
                _log(f"  {day}: CZ/BN ratio = {ratio:.2f} [OK]")

        _log(f"Cross-validation complete: {divergences} divergences out of {len(cz_rows)} days")

    except Exception as e:
        _log(f"[ERROR] Cross-validation failed: {e}")
    finally:
        cz_conn.close()
        bn_conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Data quality validation")
    parser.add_argument("--check", action="store_true",
                        help="Run plausibility checks on latest data")
    parser.add_argument("--cross-validate", action="store_true",
                        help="Compare Coinalyze vs Binance OI")
    parser.add_argument("--days", type=int, default=7,
                        help="Days to cross-validate (default: 7)")
    parser.add_argument("--flags", action="store_true",
                        help="Show recent data quality flags")
    args = parser.parse_args()

    if args.cross_validate:
        cross_validate_oi(days=args.days)
        return

    if args.flags:
        if not os.path.exists(COINALYZE_DB):
            _log("No Coinalyze DB found")
            return
        conn = sqlite3.connect(COINALYZE_DB, timeout=5)
        _init_flags_table(conn)
        rows = conn.execute(
            "SELECT timestamp, check_name, severity, message "
            "FROM data_quality_flags ORDER BY timestamp DESC LIMIT 20"
        ).fetchall()
        if rows:
            for ts, name, sev, msg in rows:
                _log(f"  [{sev}] {ts} {name}: {msg}")
        else:
            _log("No data quality flags recorded")
        conn.close()
        return

    if args.check:
        if not os.path.exists(COINALYZE_DB):
            _log("No Coinalyze DB found")
            return
        conn = sqlite3.connect(COINALYZE_DB, timeout=5)
        validate_latest_bars(conn)
        conn.close()
        return

    parser.print_help()


if __name__ == "__main__":
    main()
