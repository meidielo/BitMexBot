import os
import csv
from datetime import datetime
import pandas as pd
from bitmex_client import get_data_client

# BitMEX does not support 15m natively (valid: 1m, 5m, 1h, 1d).
# We fetch 300 x 5m candles and resample to get exactly 100 x 15m candles.
SYMBOL = "BTC/USDT:USDT"
RAW_TIMEFRAME = "5m"       # what we ask BitMEX for
TARGET_TIMEFRAME = "15min" # pandas resample rule
LIMIT = 100                # desired output candles
RAW_LIMIT = LIMIT * 3      # 300 x 5m → 100 x 15m
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "ohlcv.csv")


def fetch_ohlcv(exchange=None):
    if exchange is None:
        try:
            exchange = get_data_client()
        except Exception as e:
            print(f"[ERROR] Failed to initialise data client: {e}")
            return None

    try:
        raw = exchange.fetch_ohlcv(SYMBOL, timeframe=RAW_TIMEFRAME, limit=RAW_LIMIT)
    except Exception as e:
        print(f"[ERROR] Failed to fetch OHLCV data from BitMEX: {e}")
        return None

    if not raw:
        print("[ERROR] No candles returned from exchange.")
        return None

    # --- clean raw 5m candles BEFORE resampling ---
    # BitMEX often returns malformed candles (O > H, C > H, etc.)
    # for recently-closed or still-forming bars.  Drop them so that
    # the resample only uses clean data → proper wicks on 15m candles.
    try:
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.set_index("timestamp")

        # Drop 5m candles where OHLC constraints are violated
        bad_5m = (
            (df["high"] < df["open"]) |
            (df["high"] < df["close"]) |
            (df["low"]  > df["open"]) |
            (df["low"]  > df["close"]) |
            (df["high"] < df["low"])
        )
        n_bad_5m = int(bad_5m.sum())
        if n_bad_5m > 0:
            print(f"[INFO] Dropped {n_bad_5m} malformed 5m candle(s) before resample.")
            df = df[~bad_5m]

        if df.empty:
            print("[ERROR] All 5m candles failed sanity check — cannot proceed.")
            return None

        # --- resample 5m → 15m ---
        resampled = df.resample(TARGET_TIMEFRAME).agg({
            "open":   "first",
            "high":   "max",
            "low":    "min",
            "close":  "last",
            "volume": "sum",
        }).dropna()

        # Take the last LIMIT complete candles
        resampled = resampled.tail(LIMIT)

    except Exception as e:
        print(f"[ERROR] Failed to resample candles to 15m: {e}")
        return None

    return resampled


def format_and_print(df):
    header = f"{'Timestamp':<22} {'Open':>10} {'High':>10} {'Low':>10} {'Close':>10} {'Volume':>14}"
    separator = "-" * len(header)
    print(separator)
    print(header)
    print(separator)
    for ts, row in df.iterrows():
        ts_str = ts.strftime("%Y-%m-%d %H:%M UTC")
        print(f"{ts_str:<22} {row['open']:>10.2f} {row['high']:>10.2f} "
              f"{row['low']:>10.2f} {row['close']:>10.2f} {row['volume']:>14.4f}")
    print(separator)
    print(f"Total candles: {len(df)}")


def save_to_csv(df):
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df.index = df.index.strftime("%Y-%m-%d %H:%M:%S")
        df.index.name = "timestamp_utc"
        df.to_csv(OUTPUT_FILE)
        print(f"\n[OK] Saved {len(df)} candles to {OUTPUT_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed to save CSV: {e}")


get_candles = fetch_ohlcv  # alias for callers that import get_candles

if __name__ == "__main__":
    print(f"Fetching {RAW_LIMIT} x {RAW_TIMEFRAME} candles for {SYMBOL} from BitMEX mainnet,")
    print(f"resampling to {LIMIT} x 15m candles...\n")
    df = fetch_ohlcv()
    if df is not None:
        format_and_print(df)
        save_to_csv(df)
