import os
import pandas as pd
import pandas_ta as ta
from fetch_data import fetch_ohlcv

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "indicators.csv")


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Takes an OHLCV DataFrame (DatetimeIndex, columns: open high low close volume)
    and appends these indicator columns:
        ema_20, ema_50       — Exponential Moving Averages on close
        rsi_14               — Relative Strength Index (14-period) on close
        bb_upper, bb_mid,    — Bollinger Bands (20-period, 2 std dev) on close
        bb_lower
    Returns enriched DataFrame, or None on error.
    Note: the first N-1 rows of each indicator will be NaN (warm-up period).
    """
    try:
        df = df.copy()
    except Exception as e:
        print(f"[ERROR] Failed to copy DataFrame: {e}")
        return None

    # --- EMAs ---
    try:
        df["ema_20"] = ta.ema(df["close"], length=20)
        df["ema_50"] = ta.ema(df["close"], length=50)
    except Exception as e:
        print(f"[ERROR] Failed to compute EMAs: {e}")
        return None

    # --- RSI ---
    try:
        df["rsi_14"] = ta.rsi(df["close"], length=14)
    except Exception as e:
        print(f"[ERROR] Failed to compute RSI: {e}")
        return None

    # --- Bollinger Bands ---
    # ta.bbands column names vary by pandas-ta version (e.g. BBU_20_2.0 or BBU_20_2.0_2.0).
    # Resolve them dynamically to stay version-agnostic.
    try:
        bb = ta.bbands(df["close"], length=20, std=2)
        col_upper = next(c for c in bb.columns if c.startswith("BBU"))
        col_mid   = next(c for c in bb.columns if c.startswith("BBM"))
        col_lower = next(c for c in bb.columns if c.startswith("BBL"))
        df["bb_upper"] = bb[col_upper]
        df["bb_mid"]   = bb[col_mid]
        df["bb_lower"] = bb[col_lower]
    except Exception as e:
        print(f"[ERROR] Failed to compute Bollinger Bands: {e}")
        return None

    return df


def print_last_rows(df: pd.DataFrame, n: int = 10) -> None:
    """Pretty-print the last n rows of the enriched DataFrame."""

    def fv(val, width=10):
        """Format a float to fixed width, or 'NaN' if missing."""
        return f"{val:>{width}.2f}" if pd.notna(val) else f"{'NaN':>{width}}"

    col_w = {
        "ts":       22,
        "close":    10,
        "ema_20":   10,
        "ema_50":   10,
        "rsi":       8,
        "bb_upper": 10,
        "bb_mid":   10,
        "bb_lower": 10,
    }

    header = (
        f"{'Timestamp':<{col_w['ts']}}"
        f"{'Close':>{col_w['close']}}"
        f"{'EMA 20':>{col_w['ema_20']}}"
        f"{'EMA 50':>{col_w['ema_50']}}"
        f"{'RSI 14':>{col_w['rsi']}}"
        f"{'BB Upper':>{col_w['bb_upper']}}"
        f"{'BB Mid':>{col_w['bb_mid']}}"
        f"{'BB Lower':>{col_w['bb_lower']}}"
    )
    sep = "-" * len(header)

    print(sep)
    print(header)
    print(sep)

    for ts, row in df.tail(n).iterrows():
        print(
            f"{ts.strftime('%Y-%m-%d %H:%M UTC'):<{col_w['ts']}}"
            f"{fv(row['close'])}"
            f"{fv(row['ema_20'])}"
            f"{fv(row['ema_50'])}"
            f"{fv(row['rsi_14'], col_w['rsi'])}"
            f"{fv(row['bb_upper'])}"
            f"{fv(row['bb_mid'])}"
            f"{fv(row['bb_lower'])}"
        )

    print(sep)
    print(f"Showing last {n} of {len(df)} rows  |  "
          f"NaN rows (warm-up): ema_20={df['ema_20'].isna().sum()}  "
          f"ema_50={df['ema_50'].isna().sum()}  "
          f"rsi_14={df['rsi_14'].isna().sum()}  "
          f"bb={df['bb_mid'].isna().sum()}")


def save_to_csv(df: pd.DataFrame) -> None:
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out = df.copy()
        out.index = out.index.strftime("%Y-%m-%d %H:%M:%S")
        out.index.name = "timestamp_utc"
        out.to_csv(OUTPUT_FILE)
        print(f"\n[OK] Saved enriched DataFrame ({len(out)} rows) to {OUTPUT_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed to save indicators CSV: {e}")


add_indicators = compute_indicators  # alias for callers that import add_indicators

if __name__ == "__main__":
    print("Phase 2 — Computing indicators on XBTUSDT 15m candles\n")

    # --- Step 1: fetch OHLCV ---
    print("Fetching OHLCV data from BitMEX testnet...")
    df_raw = fetch_ohlcv()
    if df_raw is None:
        raise SystemExit("[ABORT] Could not fetch OHLCV data. Exiting.")
    print(f"[OK] Received {len(df_raw)} candles.\n")

    # --- Step 2: compute indicators ---
    print("Computing indicators...")
    df_enriched = compute_indicators(df_raw)
    if df_enriched is None:
        raise SystemExit("[ABORT] Indicator computation failed. Exiting.")
    print("[OK] Indicators computed.\n")

    # --- Step 3: print last 10 rows ---
    print_last_rows(df_enriched, n=10)

    # --- Step 4: save ---
    save_to_csv(df_enriched)
