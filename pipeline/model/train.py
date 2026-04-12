"""Model training for PokeDelta prediction engine.

Trains three LightGBM quantile regression models (median, lower, upper)
on historical card data. Evaluates with walk-forward validation and
persists model artifacts + report card metrics.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import r2_score

from pipeline.model.features import FEATURE_COLUMNS, build_training_dataset

logger = logging.getLogger("pipeline.model.train")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Hyperparameters
# ---------------------------------------------------------------------------

BASE_PARAMS = {
    "num_leaves": 31,
    "learning_rate": 0.05,
    "n_estimators": 500,
    "min_child_samples": 20,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "verbose": -1,
}

HOLDOUT_MONTHS = 12  # last 12 months for validation


def train_model(
    db,
    version_tag: Optional[str] = None,
) -> Dict[str, Any]:
    """Train the full model suite and return report card metrics.

    Steps:
    1. Build training dataset from DB
    2. Split train/test chronologically
    3. Train median + quantile models
    4. Evaluate on holdout
    5. Save artifacts (LightGBM native format)
    6. Write report card to DB

    Returns dict with model_version, metrics, and paths.
    """
    date_str = dt.date.today().isoformat()
    version = version_tag or f"v1_{date_str}"

    # Step 1: Build dataset
    df = build_training_dataset(db)
    if df.empty or len(df) < 100:
        logger.warning("Insufficient training data: %d samples", len(df))
        return {"error": "insufficient_data", "samples": len(df)}

    logger.info("Training dataset: %d samples, %d features",
                len(df), len(FEATURE_COLUMNS))

    # Step 2: Chronological split
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    cutoff = df["anchor_date"].max() - pd.DateOffset(months=HOLDOUT_MONTHS)

    train = df[df["anchor_date"] < cutoff]
    test = df[df["anchor_date"] >= cutoff]

    if len(train) < 50 or len(test) < 20:
        logger.warning("Train/test split too small: train=%d test=%d",
                        len(train), len(test))
        return {"error": "insufficient_split", "train": len(train), "test": len(test)}

    logger.info("Train: %d samples (before %s), Test: %d samples (after)",
                len(train), cutoff.date(), len(test))

    X_train = train[FEATURE_COLUMNS].values
    y_train = train["target_return_90d"].values
    X_test = test[FEATURE_COLUMNS].values
    y_test = test["target_return_90d"].values

    # Step 3: Train three models
    models = {}
    for quantile, name in [(0.50, "median"), (0.25, "lower"), (0.75, "upper")]:
        params = {
            **BASE_PARAMS,
            "objective": "quantile",
            "alpha": quantile,
        }
        model = lgb.LGBMRegressor(**params)
        model.fit(X_train, y_train)
        models[name] = model
        logger.info("Trained %s model (alpha=%.2f)", name, quantile)

    # Step 4: Evaluate
    pred_median = models["median"].predict(X_test)
    pred_lower = models["lower"].predict(X_test)
    pred_upper = models["upper"].predict(X_test)

    r2_oos = r2_score(y_test, pred_median)
    spearman_oos, _ = spearmanr(y_test, pred_median)

    # Decile analysis
    decile_idx = np.argsort(pred_median)
    n = len(decile_idx)
    decile_size = n // 10
    decile_returns = []
    for d in range(10):
        start = d * decile_size
        end = (d + 1) * decile_size if d < 9 else n
        idx = decile_idx[start:end]
        decile_returns.append({
            "decile": d + 1,
            "mean_predicted": float(np.mean(pred_median[idx])),
            "mean_actual": float(np.mean(y_test[idx])),
            "count": int(end - start),
        })

    top_decile = decile_returns[-1]["mean_actual"]
    bottom_decile = decile_returns[0]["mean_actual"]
    decile_spread = top_decile - bottom_decile

    # Hit rate: of cards predicted positive, what % actually went up?
    predicted_positive = pred_median > 0
    if predicted_positive.sum() > 0:
        hit_rate = float((y_test[predicted_positive] > 0).mean())
    else:
        hit_rate = 0.0

    # Feature importance
    importance = dict(zip(
        FEATURE_COLUMNS,
        [float(x) for x in models["median"].feature_importances_],
    ))
    # Normalize to percentages
    total = sum(importance.values())
    if total > 0:
        importance = {k: round(v / total * 100, 1) for k, v in importance.items()}
    importance = dict(sorted(importance.items(), key=lambda x: -x[1]))

    metrics = {
        "model_version": version,
        "total_samples": len(df),
        "train_samples": len(train),
        "test_samples": len(test),
        "r_squared_oos": round(r2_oos, 4),
        "spearman_oos": round(spearman_oos, 4),
        "mean_return_top_decile": round(top_decile, 4),
        "mean_return_bottom_decile": round(bottom_decile, 4),
        "decile_spread": round(decile_spread, 4),
        "hit_rate_positive": round(hit_rate, 4),
        "feature_importance": importance,
        "decile_analysis": decile_returns,
    }

    logger.info("=== Model Report Card ===")
    logger.info("R-squared (OOS): %.4f", r2_oos)
    logger.info("Spearman (OOS): %.4f", spearman_oos)
    logger.info("Top decile avg return: %.2f%%", top_decile * 100)
    logger.info("Bottom decile avg return: %.2f%%", bottom_decile * 100)
    logger.info("Decile spread: %.2f%%", decile_spread * 100)
    logger.info("Hit rate (predicted +): %.1f%%", hit_rate * 100)
    logger.info("Top features: %s", list(importance.items())[:5])

    # Step 5: Save artifacts using LightGBM native format (safe, no pickle)
    for name, model in models.items():
        model_path = MODELS_DIR / f"{name}_{version}.lgb"
        model.booster_.save_model(str(model_path))
        logger.info("Saved %s model to %s", name, model_path)

    importance_path = MODELS_DIR / f"feature_importance_{version}.json"
    with open(importance_path, "w") as f:
        json.dump(importance, f, indent=2)

    # Save version pointer for inference
    latest_path = MODELS_DIR / "latest_version.txt"
    latest_path.write_text(version)

    # Step 6: Write report card to DB
    db.execute(
        """INSERT OR REPLACE INTO model_report_card
           (model_version, as_of, horizon_days, total_samples,
            r_squared_oos, spearman_oos, mean_return_top_decile,
            mean_return_bottom_decile, decile_spread, hit_rate_positive,
            calibration_json, feature_importance_json)
           VALUES (?, ?, 90, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            version, date_str, len(df),
            r2_oos, spearman_oos, top_decile, bottom_decile,
            decile_spread, hit_rate,
            json.dumps(decile_returns),
            json.dumps(importance),
        ),
    )

    metrics["model_dir"] = str(MODELS_DIR)
    return metrics
