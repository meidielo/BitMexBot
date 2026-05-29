"""Read-only research scanner for BitMexBot shadow training.

This script is intentionally outside the live execution path. It uses public
market data only, writes watch-only candidate signals to SQLite, and never
imports order execution, risk, or authenticated BitMEX clients.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import ccxt
import pandas as pd
import sys


DEFAULT_SYMBOLS = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
]

DB_PATH = os.path.join("data", "research_signals.db")
SCORECARD_PATH = os.path.join("data", "research_scorecard.md")
RAW_TIMEFRAME = "5m"
TARGET_TIMEFRAME = "15min"
ALLOWED_ACTIONS = {"WATCH_LONG", "WATCH_SHORT", "NO_SIGNAL"}


@dataclass(frozen=True)
class ResearchSignal:
    strategy: str
    symbol: str
    action: str
    score: float
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.action not in ALLOWED_ACTIONS:
            raise ValueError(
                f"Research scanner only allows watch actions, got {self.action!r}"
            )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        print("Handled exception in research_scanner.py:60", file=sys.stderr)
        return default
    if pd.isna(result):
        return default
    return result


def build_public_exchange() -> ccxt.Exchange:
    exchange = ccxt.bitmex(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )
    exchange.load_markets()
    return exchange


def normalize_ohlcv(raw: list[list[Any]]) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.DataFrame(
        raw,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.set_index("timestamp").sort_index()
    for column in ["open", "high", "low", "close", "volume"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["high"] = df[["high", "open", "close"]].max(axis=1)
    df["low"] = df[["low", "open", "close"]].min(axis=1)
    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df[(df["close"] > 0) & (df["high"] >= df["low"])]

    if df.empty:
        return df

    return (
        df.resample(TARGET_TIMEFRAME)
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .dropna()
    )


def fetch_candles(exchange: ccxt.Exchange, symbol: str, limit: int) -> pd.DataFrame:
    raw_limit = min(max(limit * 3, 90), 1000)
    raw = exchange.fetch_ohlcv(symbol, timeframe=RAW_TIMEFRAME, limit=raw_limit)
    return normalize_ohlcv(raw).tail(limit)


def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    return pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def evaluate_trend_breakout(df: pd.DataFrame, symbol: str) -> ResearchSignal:
    strategy = "btc_trend_breakout_vol_filter"
    if len(df) < 220:
        return ResearchSignal(
            strategy,
            symbol,
            "NO_SIGNAL",
            0.0,
            f"need 220 candles, got {len(df)}",
        )

    close = _number(df["close"].iloc[-1])
    ema200 = _number(df["close"].ewm(span=200, adjust=False).mean().iloc[-1])
    prior_high = _number(df["high"].iloc[-21:-1].max())
    prior_low = _number(df["low"].iloc[-21:-1].min())
    atr14 = _number(true_range(df).rolling(14).mean().iloc[-1])
    avg_volume = _number(df["volume"].iloc[-21:-1].mean())
    volume = _number(df["volume"].iloc[-1])

    atr_pct = atr14 / close if close else 0.0
    volume_ratio = volume / avg_volume if avg_volume else 0.0
    metadata = {
        "close": close,
        "ema200": ema200,
        "prior_high_20": prior_high,
        "prior_low_20": prior_low,
        "atr_pct": atr_pct,
        "volume_ratio": volume_ratio,
    }

    volatility_ok = 0.0015 <= atr_pct <= 0.06
    participation_ok = volume_ratio >= 1.05

    if close > prior_high and close > ema200 and volatility_ok and participation_ok:
        score = min(100.0, 45.0 + (close - prior_high) / close * 20000 + volume_ratio * 12)
        return ResearchSignal(
            strategy,
            symbol,
            "WATCH_LONG",
            round(score, 2),
            "20-bar upside breakout above EMA200 with volume participation",
            metadata,
        )

    if close < prior_low and close < ema200 and volatility_ok and participation_ok:
        score = min(100.0, 45.0 + (prior_low - close) / close * 20000 + volume_ratio * 12)
        return ResearchSignal(
            strategy,
            symbol,
            "WATCH_SHORT",
            round(score, 2),
            "20-bar downside breakout below EMA200 with volume participation",
            metadata,
        )

    return ResearchSignal(
        strategy,
        symbol,
        "NO_SIGNAL",
        0.0,
        "no trend breakout with volatility and volume confirmation",
        metadata,
    )


def evaluate_vol_spike_reversion(df: pd.DataFrame, symbol: str) -> ResearchSignal:
    strategy = "vol_spike_reversion_proxy"
    if len(df) < 50:
        return ResearchSignal(
            strategy,
            symbol,
            "NO_SIGNAL",
            0.0,
            f"need 50 candles, got {len(df)}",
        )

    latest = df.iloc[-1]
    close = _number(latest["close"])
    open_price = _number(latest["open"])
    high = _number(latest["high"])
    low = _number(latest["low"])
    volume = _number(latest["volume"])

    avg_volume = _number(df["volume"].iloc[-21:-1].mean())
    atr14 = _number(true_range(df).rolling(14).mean().iloc[-1])
    candle_range = max(high - low, 0.0)
    volume_ratio = volume / avg_volume if avg_volume else 0.0
    range_atr_ratio = candle_range / atr14 if atr14 else 0.0
    close_position = (close - low) / candle_range if candle_range else 0.5
    return_pct = (close - open_price) / open_price if open_price else 0.0

    metadata = {
        "close": close,
        "open": open_price,
        "range_atr_ratio": range_atr_ratio,
        "volume_ratio": volume_ratio,
        "close_position": close_position,
        "return_pct": return_pct,
    }

    shock_ok = volume_ratio >= 2.0 and range_atr_ratio >= 1.8
    if shock_ok and return_pct <= -0.006 and close_position <= 0.35:
        score = min(100.0, 40.0 + volume_ratio * 10 + range_atr_ratio * 8)
        return ResearchSignal(
            strategy,
            symbol,
            "WATCH_LONG",
            round(score, 2),
            "downside volume shock proxy, monitor for mean reversion",
            metadata,
        )

    if shock_ok and return_pct >= 0.006 and close_position >= 0.65:
        score = min(100.0, 40.0 + volume_ratio * 10 + range_atr_ratio * 8)
        return ResearchSignal(
            strategy,
            symbol,
            "WATCH_SHORT",
            round(score, 2),
            "upside volume shock proxy, monitor for mean reversion",
            metadata,
        )

    return ResearchSignal(
        strategy,
        symbol,
        "NO_SIGNAL",
        0.0,
        "no high-volume exhaustion candle",
        metadata,
    )


def evaluate_funding_watch(
    symbol: str,
    funding_rate: float | None,
    metadata: dict[str, Any] | None = None,
) -> ResearchSignal:
    strategy = "funding_extreme_watch"
    metadata = dict(metadata or {})
    metadata["funding_rate"] = funding_rate

    if funding_rate is None:
        return ResearchSignal(
            strategy,
            symbol,
            "NO_SIGNAL",
            0.0,
            "funding unavailable",
            metadata,
        )

    watch_threshold = 0.0003
    if funding_rate >= watch_threshold:
        score = min(100.0, abs(funding_rate) / watch_threshold * 35.0)
        return ResearchSignal(
            strategy,
            symbol,
            "WATCH_SHORT",
            round(score, 2),
            "positive funding is elevated, watch for long squeeze setup",
            metadata,
        )

    if funding_rate <= -watch_threshold:
        score = min(100.0, abs(funding_rate) / watch_threshold * 35.0)
        return ResearchSignal(
            strategy,
            symbol,
            "WATCH_LONG",
            round(score, 2),
            "negative funding is elevated, watch for short squeeze setup",
            metadata,
        )

    return ResearchSignal(
        strategy,
        symbol,
        "NO_SIGNAL",
        0.0,
        "funding below watch threshold",
        metadata,
    )


def fetch_funding(exchange: ccxt.Exchange, symbol: str) -> tuple[float | None, dict[str, Any]]:
    try:
        payload = exchange.fetch_funding_rate(symbol)
    except Exception as exc:
        print("Handled exception in research_scanner.py:320", file=sys.stderr)
        return None, {"error": type(exc).__name__}

    rate = payload.get("fundingRate")
    return (
        float(rate) if rate is not None else None,
        {
            "datetime": payload.get("datetime"),
            "next_funding_datetime": payload.get("nextFundingDatetime"),
            "mark_price": payload.get("markPrice"),
            "index_price": payload.get("indexPrice"),
        },
    )


def init_db(path: str = DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_runs (
            run_id TEXT PRIMARY KEY,
            started_at_utc TEXT NOT NULL,
            completed_at_utc TEXT,
            symbols_requested TEXT NOT NULL,
            symbols_scanned INTEGER NOT NULL DEFAULT 0,
            errors_json TEXT NOT NULL DEFAULT '[]'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            timestamp_utc TEXT NOT NULL,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            action TEXT NOT NULL CHECK(action IN ('WATCH_LONG', 'WATCH_SHORT', 'NO_SIGNAL')),
            score REAL NOT NULL,
            reason TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES research_runs(run_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_research_signals_time
        ON research_signals (timestamp_utc, action)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_research_signals_run
        ON research_signals (run_id, symbol, strategy)
        """
    )
    conn.commit()
    return conn


def insert_run(
    conn: sqlite3.Connection,
    run_id: str,
    started_at: str,
    symbols: list[str],
) -> None:
    conn.execute(
        """
        INSERT INTO research_runs (run_id, started_at_utc, symbols_requested)
        VALUES (?, ?, ?)
        """,
        (run_id, started_at, json.dumps(symbols)),
    )
    conn.commit()


def insert_signals(
    conn: sqlite3.Connection,
    run_id: str,
    timestamp_utc: str,
    signals: list[ResearchSignal],
) -> None:
    rows = [
        (
            run_id,
            timestamp_utc,
            signal.strategy,
            signal.symbol,
            signal.action,
            float(signal.score),
            signal.reason,
            json.dumps(signal.metadata, sort_keys=True),
        )
        for signal in signals
    ]
    conn.executemany(
        """
        INSERT INTO research_signals
        (run_id, timestamp_utc, strategy, symbol, action, score, reason, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def finish_run(
    conn: sqlite3.Connection,
    run_id: str,
    symbols_scanned: int,
    errors: list[dict[str, str]],
) -> None:
    conn.execute(
        """
        UPDATE research_runs
        SET completed_at_utc = ?, symbols_scanned = ?, errors_json = ?
        WHERE run_id = ?
        """,
        (utc_now_iso(), symbols_scanned, json.dumps(errors), run_id),
    )
    conn.commit()


def scan_once(
    symbols: list[str],
    limit: int,
    db_path: str = DB_PATH,
    scorecard_path: str = SCORECARD_PATH,
    quiet: bool = False,
) -> int:
    conn = init_db(db_path)
    run_id = str(uuid.uuid4())
    started_at = utc_now_iso()
    insert_run(conn, run_id, started_at, symbols)

    errors: list[dict[str, str]] = []
    scanned = 0
    signals: list[ResearchSignal] = []

    try:
        exchange = build_public_exchange()
    except Exception as exc:
        errors.append({"scope": "exchange", "error": type(exc).__name__, "detail": str(exc)})
        finish_run(conn, run_id, scanned, errors)
        write_scorecard(conn, scorecard_path, run_id)
        if not quiet:
            print("Research scanner could not initialize public BitMEX data client.")
        return 2

    markets = exchange.markets or {}
    timestamp = utc_now_iso()

    for symbol in symbols:
        if symbol not in markets:
            errors.append({"symbol": symbol, "error": "unknown_symbol"})
            continue

        try:
            candles = fetch_candles(exchange, symbol, limit)
            funding_rate, funding_metadata = fetch_funding(exchange, symbol)
        except Exception as exc:
            print("Handled exception in research_scanner.py:484", file=sys.stderr)
            errors.append({"symbol": symbol, "error": type(exc).__name__, "detail": str(exc)})
            continue

        scanned += 1
        symbol_signals = [
            evaluate_trend_breakout(candles, symbol),
            evaluate_vol_spike_reversion(candles, symbol),
            evaluate_funding_watch(symbol, funding_rate, funding_metadata),
        ]
        insert_signals(conn, run_id, timestamp, symbol_signals)
        signals.extend(symbol_signals)

    finish_run(conn, run_id, scanned, errors)
    write_scorecard(conn, scorecard_path, run_id)

    watch_count = sum(1 for signal in signals if signal.action != "NO_SIGNAL")
    if not quiet:
        print(
            f"Research scanner stored {len(signals)} rows across {scanned} symbols; "
            f"{watch_count} watch signal(s)."
        )
        if errors:
            print(f"Skipped {len(errors)} symbol/client issue(s).")

    return 0 if scanned else 1


def latest_run_id(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT run_id FROM research_runs ORDER BY started_at_utc DESC LIMIT 1"
    ).fetchone()
    return str(row[0]) if row else None


def scorecard_summary(db_path: str = DB_PATH, limit: int = 5) -> list[str]:
    if not os.path.exists(db_path):
        return ["  Research scanner: no shadow database yet"]

    conn = sqlite3.connect(db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    run = conn.execute(
        """
        SELECT run_id, started_at_utc, completed_at_utc, symbols_scanned, errors_json
        FROM research_runs
        ORDER BY started_at_utc DESC
        LIMIT 1
        """
    ).fetchone()
    if not run:
        conn.close()
        return ["  Research scanner: no runs yet"]

    since = (datetime.now(timezone.utc) - timedelta(days=7)).replace(microsecond=0).isoformat()
    total_7d = conn.execute(
        "SELECT COUNT(*) FROM research_signals WHERE timestamp_utc >= ?",
        (since,),
    ).fetchone()[0]
    watch_7d = conn.execute(
        """
        SELECT COUNT(*)
        FROM research_signals
        WHERE timestamp_utc >= ? AND action != 'NO_SIGNAL'
        """,
        (since,),
    ).fetchone()[0]
    latest_watch = conn.execute(
        """
        SELECT timestamp_utc, strategy, symbol, action, score, reason
        FROM research_signals
        WHERE action != 'NO_SIGNAL'
        ORDER BY timestamp_utc DESC, score DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    errors = json.loads(run["errors_json"] or "[]")
    lines = [
        f"  Latest run: {run['started_at_utc']} ({run['symbols_scanned']} symbols scanned)",
        f"  Shadow rows last 7d: {total_7d}, watch signals: {watch_7d}",
    ]
    if errors:
        lines.append(f"  Latest run skipped issues: {len(errors)}")
    if latest_watch:
        lines.append("  Latest watchlist:")
        for row in latest_watch:
            lines.append(
                "    "
                f"{row['timestamp_utc']} {row['symbol']} {row['strategy']} "
                f"{row['action']} score={row['score']:.1f}: {row['reason']}"
            )
    else:
        lines.append("  Latest watchlist: none")
    return lines


def scorecard_summary_from_conn(conn: sqlite3.Connection) -> list[str]:
    conn.row_factory = sqlite3.Row
    run = conn.execute(
        """
        SELECT started_at_utc, symbols_scanned, errors_json
        FROM research_runs
        ORDER BY started_at_utc DESC
        LIMIT 1
        """
    ).fetchone()
    if not run:
        return ["No runs yet."]

    since = (datetime.now(timezone.utc) - timedelta(days=7)).replace(microsecond=0).isoformat()
    total_7d = conn.execute(
        "SELECT COUNT(*) FROM research_signals WHERE timestamp_utc >= ?",
        (since,),
    ).fetchone()[0]
    watch_7d = conn.execute(
        """
        SELECT COUNT(*)
        FROM research_signals
        WHERE timestamp_utc >= ? AND action != 'NO_SIGNAL'
        """,
        (since,),
    ).fetchone()[0]
    errors = json.loads(run["errors_json"] or "[]")
    return [
        f"- Latest run: {run['started_at_utc']}",
        f"- Symbols scanned: {run['symbols_scanned']}",
        f"- Shadow rows last 7d: {total_7d}",
        f"- Watch signals last 7d: {watch_7d}",
        f"- Latest run skipped issues: {len(errors)}",
    ]


def write_scorecard(
    conn: sqlite3.Connection,
    path: str = SCORECARD_PATH,
    run_id: str | None = None,
) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    run_id = run_id or latest_run_id(conn)
    generated = utc_now_iso()
    lines = [
        "# Research Scanner Scorecard",
        "",
        f"Generated: {generated}",
        "",
        "This artifact is read-only shadow research. Actions are watch labels, not trade orders.",
        "",
        "## Summary",
        "",
    ]
    lines.extend(scorecard_summary_from_conn(conn))

    if run_id:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT strategy, symbol, action, score, reason
            FROM research_signals
            WHERE run_id = ?
            ORDER BY action != 'NO_SIGNAL' DESC, score DESC, symbol, strategy
            """,
            (run_id,),
        ).fetchall()
        lines.extend(["", "## Latest Run Signals", ""])
        lines.append("| Strategy | Symbol | Action | Score | Reason |")
        lines.append("| --- | --- | --- | ---: | --- |")
        for row in rows:
            reason = str(row["reason"]).replace("|", "\\|")
            lines.append(
                f"| {row['strategy']} | {row['symbol']} | {row['action']} | "
                f"{row['score']:.1f} | {reason} |"
            )

    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def parse_symbols(value: str) -> list[str]:
    symbols = [item.strip() for item in value.split(",") if item.strip()]
    return symbols or DEFAULT_SYMBOLS


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated ccxt symbols to scan.",
    )
    parser.add_argument("--limit", type=int, default=260, help="15m candles per symbol.")
    parser.add_argument("--db", default=DB_PATH, help="SQLite output path.")
    parser.add_argument("--scorecard", default=SCORECARD_PATH, help="Markdown scorecard path.")
    parser.add_argument("--once", action="store_true", help="Run one scan and exit.")
    parser.add_argument(
        "--scorecard-only",
        action="store_true",
        help="Print the latest stored scorecard summary without fetching data.",
    )
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.scorecard_only:
        for line in scorecard_summary(args.db):
            print(line)
        return 0

    symbols = parse_symbols(args.symbols)
    return scan_once(
        symbols=symbols,
        limit=max(args.limit, 50),
        db_path=args.db,
        scorecard_path=args.scorecard,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    raise SystemExit(main())
