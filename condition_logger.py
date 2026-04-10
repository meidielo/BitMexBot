"""
condition_logger.py — Per-condition pass/fail logging

Logs every individual condition evaluation each loop iteration, even when
the full signal doesn't trigger. This captures "free data" about:
  - Base rate for each condition (how often does each pass?)
  - Which condition is the binding constraint
  - Forward data on partial setups
  - Early warning if market regime changes

Data stored in data/condition_log.db (SQLite, WAL mode).

Usage:
  from condition_logger import get_logger, log_condition, log_v4_conditions, log_v2_conditions

  conn = get_logger()
  log_condition(conn, "V4", "bull_regime", True, 85200.0, 82000.0)
  log_v4_conditions(conn, macro_state, micro_data)
"""

import os
import sqlite3
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = os.path.join("data", "condition_log.db")

# Singleton connection
_conn = None


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def get_logger() -> sqlite3.Connection:
    """Get or create the condition logger connection (singleton)."""
    global _conn
    if _conn is not None:
        return _conn

    os.makedirs("data", exist_ok=True)
    _conn = sqlite3.connect(DB_PATH, timeout=10)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA busy_timeout=5000")

    _conn.execute("""
        CREATE TABLE IF NOT EXISTS condition_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT    NOT NULL,
            strategy        TEXT    NOT NULL,
            condition_name  TEXT    NOT NULL,
            passed          INTEGER NOT NULL,
            current_value   REAL,
            threshold       REAL,
            distance_pct    REAL
        )
    """)

    # Index for efficient querying
    _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_condition_log_ts
        ON condition_log (timestamp, strategy)
    """)

    _conn.commit()
    return _conn


# ---------------------------------------------------------------------------
# Core logging
# ---------------------------------------------------------------------------
def log_condition(conn: sqlite3.Connection, strategy: str, condition_name: str,
                  passed: bool, current_value: float, threshold: float):
    """
    Log a single condition evaluation.

    distance_pct: how far from triggering.
      positive = not yet triggered (value hasn't reached threshold)
      negative = exceeded threshold (triggered)
      For "greater than" conditions: distance = (threshold - value) / threshold * 100
      For "less than" conditions: distance = (value - threshold) / threshold * 100
    """
    if threshold != 0:
        # distance > 0 means "not yet triggered"
        # distance < 0 means "already triggered / exceeded"
        distance_pct = (threshold - current_value) / abs(threshold) * 100
    else:
        distance_pct = 0

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO condition_log "
        "(timestamp, strategy, condition_name, passed, current_value, threshold, distance_pct) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (now, strategy, condition_name, 1 if passed else 0,
         current_value, threshold, round(distance_pct, 4))
    )
    conn.commit()


# ---------------------------------------------------------------------------
# V4 convenience — log all 4 conditions at once
# ---------------------------------------------------------------------------
def log_v4_conditions(conn: sqlite3.Connection,
                      close: float = None, ema200: float = None,
                      funding_peak: float = None, funding_threshold: float = None,
                      liq_ratio: float = None, liq_spike_mult: float = None,
                      liq_long_pct: float = None, liq_long_dom: float = None,
                      oi_delta_pct: float = None):
    """
    Log all V4 macro + micro conditions in one call.
    Pass whatever data is available; conditions with None values are skipped.
    """
    # C1: Bull regime (close > EMA200)
    if close is not None and ema200 is not None:
        log_condition(conn, "V4", "bull_regime",
                     close > ema200, close, ema200)

    # C2: Funding setup (peak > threshold)
    if funding_peak is not None and funding_threshold is not None:
        log_condition(conn, "V4", "funding_setup",
                     funding_peak > funding_threshold,
                     funding_peak, funding_threshold)

    # C3: Liquidation spike (ratio >= mult)
    if liq_ratio is not None and liq_spike_mult is not None:
        log_condition(conn, "V4", "liq_spike",
                     liq_ratio >= liq_spike_mult,
                     liq_ratio, liq_spike_mult)

    # C3b: Long dominance (long_pct >= threshold)
    if liq_long_pct is not None and liq_long_dom is not None:
        log_condition(conn, "V4", "liq_long_dominance",
                     liq_long_pct >= liq_long_dom,
                     liq_long_pct, liq_long_dom)

    # C4: OI confirmation (delta < 0)
    if oi_delta_pct is not None:
        log_condition(conn, "V4", "oi_confirm",
                     oi_delta_pct < 0,
                     oi_delta_pct, 0)


# ---------------------------------------------------------------------------
# V2 convenience — log funding rate signal conditions
# ---------------------------------------------------------------------------
def log_v2_conditions(conn: sqlite3.Connection,
                      funding_rate: float = None,
                      funding_threshold: float = None,
                      funding_24h: float = None,
                      funding_24h_threshold: float = None,
                      in_settlement_window: bool = None,
                      volume_ratio: float = None,
                      volume_threshold: float = None,
                      bearish_break: bool = None,
                      bullish_break: bool = None,
                      body_pct: float = None,
                      body_threshold: float = None):
    """Log all V2 funding rate signal conditions in one call."""

    # Setup: funding rate extreme
    if funding_rate is not None and funding_threshold is not None:
        extreme = abs(funding_rate) > funding_threshold
        log_condition(conn, "V2", "funding_extreme",
                     extreme, abs(funding_rate), funding_threshold)

    # Setup: cumulative 24h funding
    if funding_24h is not None and funding_24h_threshold is not None:
        extreme_24h = abs(funding_24h) > funding_24h_threshold
        log_condition(conn, "V2", "funding_24h_extreme",
                     extreme_24h, abs(funding_24h), funding_24h_threshold)

    # Time filter: settlement window
    if in_settlement_window is not None:
        log_condition(conn, "V2", "settlement_window",
                     in_settlement_window, 1 if in_settlement_window else 0, 1)

    # Trigger: volume spike
    if volume_ratio is not None and volume_threshold is not None:
        log_condition(conn, "V2", "volume_spike",
                     volume_ratio >= volume_threshold,
                     volume_ratio, volume_threshold)

    # Trigger: price break
    if bearish_break is not None:
        log_condition(conn, "V2", "bearish_break",
                     bearish_break, 1 if bearish_break else 0, 1)

    if bullish_break is not None:
        log_condition(conn, "V2", "bullish_break",
                     bullish_break, 1 if bullish_break else 0, 1)

    # Trigger: body confirmation
    if body_pct is not None and body_threshold is not None:
        log_condition(conn, "V2", "body_confirmation",
                     body_pct > body_threshold,
                     body_pct, body_threshold)


# ---------------------------------------------------------------------------
# Query helpers (for dashboard/analysis)
# ---------------------------------------------------------------------------
def get_condition_stats(conn: sqlite3.Connection = None,
                        strategy: str = None,
                        days: int = 30) -> dict:
    """
    Get pass rates for each condition over the last N days.
    Returns dict: {condition_name: {"total": N, "passed": N, "pass_rate": 0.X}}
    """
    if conn is None:
        conn = get_logger()

    where_clauses = ["timestamp >= datetime('now', ? || ' days')"]
    params = [f"-{days}"]

    if strategy:
        where_clauses.append("strategy = ?")
        params.append(strategy)

    where = " AND ".join(where_clauses)

    rows = conn.execute(f"""
        SELECT condition_name,
               COUNT(*) as total,
               SUM(passed) as passed,
               AVG(distance_pct) as avg_distance
        FROM condition_log
        WHERE {where}
        GROUP BY condition_name
        ORDER BY condition_name
    """, params).fetchall()

    stats = {}
    for name, total, passed, avg_dist in rows:
        stats[name] = {
            "total": total,
            "passed": passed,
            "pass_rate": round(passed / total, 4) if total > 0 else 0,
            "avg_distance": round(avg_dist, 4) if avg_dist else 0,
        }
    return stats
