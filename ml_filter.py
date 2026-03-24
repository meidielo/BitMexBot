"""
ml_filter.py

Train a RandomForestClassifier on backtest_trades.csv to predict
whether a SHORT signal will hit TP (label=1) or SL (label=0).

Usage
-----
    python ml_filter.py --retrain

Loads data/backtest_trades.csv, retrains the model, saves:
    data/ml_filter.pkl       trained RandomForestClassifier (joblib)
    data/ml_features.json    ordered feature name list

score_signal(signal_dict) -> float
    Called by signals.py to get a TP probability at live entry time.
    Returns probability in [0.0, 1.0].

Feature notes
-------------
signal_rr    -- signal R:R at entry time  (available at entry)
direction    -- 1=SHORT, 0=LONG           (available at entry)
hour_of_day  -- UTC hour of entry_ts      (available at entry)
duration_min -- minutes to exit           (NOT at entry; set to 0 for live scoring)
actual_r     -- realised R-multiple       (NOT at entry; set to 0 for live scoring)

duration_min and actual_r are included as training features because they
exist in the CSV, but they must be set to 0.0 for any live prediction.
This introduces some training bias; given the small sample size the model
is treated as a soft filter only (see MIN_RELIABLE_SAMPLES warning).
"""

import argparse
import datetime
import json
import os
import warnings

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR      = "data"
TRADES_CSV    = os.path.join(DATA_DIR, "backtest_trades.csv")
MODEL_PATH    = os.path.join(DATA_DIR, "ml_filter.pkl")
FEATURES_PATH = os.path.join(DATA_DIR, "ml_features.json")

# ---------------------------------------------------------------------------
# Feature / label config
# ---------------------------------------------------------------------------
FEATURES = ["signal_rr", "direction", "hour_of_day", "duration_min", "actual_r"]
LABEL    = "label"

MIN_RELIABLE_SAMPLES = 200   # below this count, print reliability warning


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_data(path: str = TRADES_CSV) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"[ERROR] Training data not found: {path}\n"
            f"Run backtest.py first to generate it."
        )
    return pd.read_csv(path)


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract model features and label from the raw trades DataFrame.
    Drops OPEN trades (outcome unknown — cannot be labelled).
    Returns a DataFrame with columns = FEATURES + [LABEL].
    """
    resolved = df[df["outcome"].isin(["TP", "SL"])].copy()

    out = pd.DataFrame(index=resolved.index)
    out["signal_rr"]   = resolved["signal_rr"].astype(float)
    out["direction"]   = (resolved["direction"] == "SHORT").astype(int)
    out["hour_of_day"] = pd.to_datetime(
        resolved["entry_ts"].str.replace(" UTC", "", regex=False)
    ).dt.hour.astype(int)
    out["duration_min"] = resolved["duration_min"].astype(float)
    out["actual_r"]     = resolved["actual_r"].astype(float)
    out[LABEL]          = (resolved["outcome"] == "TP").astype(int)

    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train(path: str = TRADES_CSV) -> None:
    """Load CSV, train model chronologically, print report, save artefacts."""
    raw  = _load_data(path)
    data = _build_features(raw)

    n      = len(data)
    n_win  = int(data[LABEL].sum())
    n_loss = n - n_win

    print(f"\nTraining on {n} resolved trades  (TP={n_win}, SL={n_loss})")

    if n < MIN_RELIABLE_SAMPLES:
        print(
            f"\nWARNING: Only {n} training samples. Model predictions are unreliable "
            f"below {MIN_RELIABLE_SAMPLES} samples. Use as soft filter only."
        )

    # Chronological 80 / 20 split — no shuffle, no future leakage
    split   = int(n * 0.8)
    X_train = data.iloc[:split][FEATURES]
    y_train = data.iloc[:split][LABEL]
    X_test  = data.iloc[split:][FEATURES]
    y_test  = data.iloc[split:][LABEL]

    print(f"Chronological split: train={len(X_train)}  test={len(X_test)}\n")

    clf = RandomForestClassifier(
        n_estimators=100,
        class_weight="balanced",
        random_state=42,
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        clf.fit(X_train, y_train)

    # Evaluation
    if len(X_test) > 0 and len(y_test.unique()) > 1:
        y_pred = clf.predict(X_test)
        print("Classification report (held-out test set):")
        print(classification_report(y_test, y_pred, target_names=["SL (0)", "TP (1)"]))
    else:
        print(
            "[WARN] Test set too small or single-class — "
            "skipping classification report.\n"
        )

    # Feature importances
    importances = sorted(
        zip(FEATURES, clf.feature_importances_),
        key=lambda x: x[1],
        reverse=True,
    )
    print("Top 5 feature importances:")
    for fname, imp in importances[:5]:
        bar = "#" * int(imp * 40)
        print(f"  {fname:<16} {imp:.4f}  {bar}")

    # Save artefacts
    os.makedirs(DATA_DIR, exist_ok=True)
    joblib.dump(clf, MODEL_PATH)
    with open(FEATURES_PATH, "w", encoding="utf-8") as fh:
        json.dump(FEATURES, fh)

    print(f"\n[OK] Model saved  -> {MODEL_PATH}")
    print(f"[OK] Features saved -> {FEATURES_PATH}")


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------

def score_signal(signal_dict: dict) -> float:
    """
    Predict probability that a SHORT signal will hit TP.

    Parameters extracted from signal_dict:
        signal_rr   <- signal_dict["rr"]
        direction   <- 1 (always SHORT — LONG is disabled)
        hour_of_day <- current UTC hour (entry time = now for live signals)

    Parameters set to 0.0 (unknown at entry time):
        duration_min
        actual_r

    Returns
    -------
    float in [0.0, 1.0]  — probability of TP outcome

    Raises
    ------
    FileNotFoundError if model pkl does not exist.
    """
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}. "
            f"Run:  python ml_filter.py --retrain"
        )

    clf = joblib.load(MODEL_PATH)

    direction = 1 if signal_dict.get("signal") == "SHORT" else 0
    signal_rr = float(signal_dict.get("rr") or 0.0)
    hour      = datetime.datetime.utcnow().hour   # live: current hour = entry hour

    row = pd.DataFrame([{
        "signal_rr":    signal_rr,
        "direction":    direction,
        "hour_of_day":  hour,
        "duration_min": 0.0,
        "actual_r":     0.0,
    }])[FEATURES]

    prob = float(clf.predict_proba(row)[0][1])
    return prob


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train / retrain the ML signal filter."
    )
    parser.add_argument(
        "--retrain",
        action="store_true",
        help="Retrain from data/backtest_trades.csv and overwrite saved model.",
    )
    args = parser.parse_args()

    if args.retrain:
        train()
    else:
        parser.print_help()
        print(
            "\nNo action taken. Pass --retrain to train the model.\n"
            "Example:  python ml_filter.py --retrain"
        )
