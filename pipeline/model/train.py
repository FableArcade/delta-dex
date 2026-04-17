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

from pipeline.model.features import (
    FEATURE_COLUMNS,
    HORIZON_DAYS,
    TARGET_COL,
    build_training_dataset,
)

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


def _rolling_origin_cv(
    df: pd.DataFrame,
    n_folds: int = 3,
    holdout_months: int = 12,
) -> Dict[str, Any]:
    """Rolling-origin cross-validation (FPP3 §5.10 / Vehtari CV-FAQ).

    Splits the training window into `n_folds` expanding-origin folds, each
    holding out `holdout_months` at a time. Returns aggregated mean/std on
    Spearman and MASE across folds so report_card metrics reflect time-slice
    stability, not a single lucky cutoff.

    Light-weight: fits only the median quantile per fold (not the full
    5-model suite). The promotion gate still uses its full 46-fold
    walkforward for ship/no-ship decisions; this is a sanity signal.
    """
    if df.empty or len(df) < 200:
        return {"skipped": True, "reason": "insufficient_data"}

    df = df.sort_values("anchor_date").reset_index(drop=True)
    end = df["anchor_date"].max()

    spearmans, mases, n_tests = [], [], []
    for k in range(n_folds, 0, -1):
        fold_cutoff = end - pd.DateOffset(months=holdout_months * k)
        fold_test_end = end - pd.DateOffset(months=holdout_months * (k - 1))
        tr = df[df["anchor_date"] < fold_cutoff]
        te = df[(df["anchor_date"] >= fold_cutoff) & (df["anchor_date"] < fold_test_end)]
        if len(tr) < 50 or len(te) < 20:
            continue

        y_tr = np.log1p(np.clip(tr[TARGET_COL].values, -0.999, None))
        y_te = te[TARGET_COL].values
        m = lgb.LGBMRegressor(
            **{**BASE_PARAMS, "objective": "quantile", "alpha": 0.50},
        )
        m.fit(tr[FEATURE_COLUMNS].values, y_tr)
        pred = np.expm1(m.predict(te[FEATURE_COLUMNS].values))

        sp, _ = spearmanr(y_te, pred)
        mae_model = float(np.mean(np.abs(y_te - pred)))
        mae_naive = float(np.mean(np.abs(y_te - 0.0)))
        mase = mae_model / mae_naive if mae_naive > 0 else float("nan")

        spearmans.append(float(sp) if sp == sp else 0.0)  # NaN-safe
        mases.append(mase if mase == mase else 0.0)
        n_tests.append(int(len(te)))

    if not spearmans:
        return {"skipped": True, "reason": "all_folds_too_small"}

    return {
        "n_folds": len(spearmans),
        "spearman_mean": round(float(np.mean(spearmans)), 4),
        "spearman_std": round(float(np.std(spearmans)), 4),
        "mase_mean": round(float(np.mean(mases)), 4),
        "mase_std": round(float(np.std(mases)), 4),
        "fold_sizes": n_tests,
    }


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
    version = version_tag or f"v1_3_{date_str}"

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
    y_train = train[TARGET_COL].values
    X_test = test[FEATURE_COLUMNS].values
    y_test = test[TARGET_COL].values

    # v1.3: log1p target transform — variance-stabilizing, reduces outlier
    # pull on the loss. Predictions come out in log-space; we invert with
    # expm1 for evaluation and storage. Clip at -0.999 so log1p is finite.
    y_train_log = np.log1p(np.clip(y_train, -0.999, None))

    # Step 3: Train quantile models (in log-return space).
    # We ship both a 50% IQR (P25/P75) — used today for HIGH/MED/LOW bucketing —
    # and an 80% interval (P10/P90) which is the canonical sizing band per
    # FPP3 §5.5 ("point forecasts without intervals are ~worthless"). Training
    # both lets the UI present a decision-sized band without recalibrating
    # existing thresholds.
    models = {}
    for quantile, name in [
        (0.50, "median"),
        (0.25, "lower"),
        (0.75, "upper"),
        (0.10, "lower_80"),
        (0.90, "upper_80"),
    ]:
        params = {
            **BASE_PARAMS,
            "objective": "quantile",
            "alpha": quantile,
        }
        model = lgb.LGBMRegressor(**params)
        model.fit(X_train, y_train_log)
        models[name] = model
        logger.info("Trained %s model (alpha=%.2f)", name, quantile)

    # Step 4: Evaluate — predictions are in log-return space; invert to linear
    pred_median_log = models["median"].predict(X_test)
    pred_lower_log = models["lower"].predict(X_test)
    pred_upper_log = models["upper"].predict(X_test)
    pred_lower80_log = models["lower_80"].predict(X_test)
    pred_upper80_log = models["upper_80"].predict(X_test)
    pred_median = np.expm1(pred_median_log)
    pred_lower = np.expm1(pred_lower_log)
    pred_upper = np.expm1(pred_upper_log)
    pred_lower80 = np.expm1(pred_lower80_log)
    pred_upper80 = np.expm1(pred_upper80_log)

    # Empirical coverage checks (pinball-loss quantiles are not guaranteed
    # to be calibrated; we at least measure reality). P1 next-session fix
    # is to wrap these with conformal prediction for a real coverage guarantee.
    cov_50 = float(((y_test >= pred_lower) & (y_test <= pred_upper)).mean())
    cov_80 = float(((y_test >= pred_lower80) & (y_test <= pred_upper80)).mean())

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

    # Scaled accuracy (FPP3 §5.8): MASE against naive-predict-zero baseline.
    # MASE = mean(|y - ŷ|) / mean(|y - ŷ_naive|). MAPE explodes near zero;
    # MASE is unit-free, works across low-liquidity cards, and has a hard
    # interpretation: ≥1 means you're beaten by the naive baseline.
    mae_model = float(np.mean(np.abs(y_test - pred_median)))
    mae_naive = float(np.mean(np.abs(y_test - 0.0)))  # naive = predict zero return
    mase = mae_model / mae_naive if mae_naive > 0 else float("nan")

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

    # Rolling-origin cross-validation (FPP3 §5.10). The training phase runs
    # single-fold for speed; the promotion gate uses a separate 46-fold
    # walkforward. We ship a light 3-fold rolling-origin CV here so
    # report_card metrics include both single-fold and rolling views, and
    # users see how stable the model is across time slices rather than
    # relying on one lucky cutoff.
    rolling_metrics = _rolling_origin_cv(
        df, n_folds=3, holdout_months=HOLDOUT_MONTHS,
    )

    metrics = {
        "model_version": version,
        "total_samples": len(df),
        "train_samples": len(train),
        "test_samples": len(test),
        "r_squared_oos": round(r2_oos, 4),
        "spearman_oos": round(spearman_oos, 4),
        "mase": round(mase, 4),
        "coverage_50": round(cov_50, 4),
        "coverage_80": round(cov_80, 4),
        "mean_return_top_decile": round(top_decile, 4),
        "mean_return_bottom_decile": round(bottom_decile, 4),
        "decile_spread": round(decile_spread, 4),
        "hit_rate_positive": round(hit_rate, 4),
        "feature_importance": importance,
        "decile_analysis": decile_returns,
        "rolling_origin_cv": rolling_metrics,
    }

    logger.info("=== Model Report Card ===")
    logger.info("R-squared (OOS): %.4f", r2_oos)
    logger.info("Spearman (OOS): %.4f", spearman_oos)
    logger.info("MASE (vs naive=0): %.4f (<1 beats naive)", mase)
    logger.info("Coverage 50%% band: %.1f%% (target 50%%)", cov_50 * 100)
    logger.info("Coverage 80%% band: %.1f%% (target 80%%)", cov_80 * 100)
    logger.info("Top decile avg return: %.2f%%", top_decile * 100)
    logger.info("Bottom decile avg return: %.2f%%", bottom_decile * 100)
    logger.info("Decile spread: %.2f%%", decile_spread * 100)
    logger.info("Hit rate (predicted +): %.1f%%", hit_rate * 100)
    logger.info("Rolling-origin CV (3 folds): spearman=%s MASE=%s",
                rolling_metrics.get("spearman_mean"), rolling_metrics.get("mase_mean"))
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
    # Single-fold metrics go into canonical columns for schema continuity.
    # Rolling-origin CV + MASE + coverage are folded into calibration_json
    # so no schema migration is needed to land this upgrade.
    decile_returns_payload = {
        "decile_analysis": decile_returns,
        "mase": mase,
        "coverage_50": cov_50,
        "coverage_80": cov_80,
        "rolling_origin_cv": rolling_metrics,
    }
    db.execute(
        """INSERT OR REPLACE INTO model_report_card
           (model_version, as_of, horizon_days, total_samples,
            r_squared_oos, spearman_oos, mean_return_top_decile,
            mean_return_bottom_decile, decile_spread, hit_rate_positive,
            calibration_json, feature_importance_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            version, date_str, HORIZON_DAYS, len(df),
            r2_oos, spearman_oos, top_decile, bottom_decile,
            decile_spread, hit_rate,
            json.dumps(decile_returns_payload),
            json.dumps(importance),
        ),
    )

    metrics["model_dir"] = str(MODELS_DIR)
    return metrics
