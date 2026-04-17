"""Two-stage prediction model: filter then rank.

Architecture designed around the investor's actual decision:
  1. Filter: "Will this card be net-positive after friction over 180d?"
     Binary classifier with isotonic calibration. Eliminates cards
     likely to keep declining.
  2. Rank: "Among the survivors, which will return the most?"
     Quantile regressor with asymmetric sample weights — penalizes
     ranking a loser highly 2x more than missing a winner.

Key insight: a card at a deep dip (high peak_discount) with returning
demand (positive net_flow) and tight supply (low saturation) is a
reversal candidate. The model has these features — the two-stage
architecture lets it learn the reversal pattern separately from the
"will it be positive at all" question.

Drops `psa_10_vs_raw_pct` (ablation-confirmed collider at conviction tip).

Usage:
    from pipeline.model.two_stage import train_two_stage
    metrics = train_two_stage(db, version_tag="v2_0_2026-04-16")
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
from pathlib import Path
from typing import Any, Dict, Optional

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import r2_score

from pipeline.model.features import (
    FEATURE_COLUMNS,
    HORIZON_DAYS,
    TARGET_COL,
    build_training_dataset,
)

logger = logging.getLogger("pipeline.model.two_stage")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# Drop the ablation-confirmed collider
FEATURES_V2 = [f for f in FEATURE_COLUMNS if f != "psa_10_vs_raw_pct"]

HOLDOUT_MONTHS = 12
CAL_FRACTION = 0.2  # 20% of train for isotonic calibration

# --- Stage 1: Survival classifier ---
CLF_PARAMS = {
    "objective": "binary",
    "num_leaves": 31,
    "learning_rate": 0.05,
    "n_estimators": 400,
    "min_child_samples": 20,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "is_unbalance": True,  # most cards lose after friction — auto-rebalance
    "verbose": -1,
}

# --- Stage 2: Conviction regressor ---
REG_PARAMS = {
    "objective": "quantile",
    "alpha": 0.50,
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

# Asymmetric sample weights for Stage 2:
# Winners get 2x weight — the model should be MORE accurate at ranking
# cards that actually return positive, because those are the only ones
# the investor acts on. Getting the ordering wrong among losers is cheap;
# getting the ordering wrong among winners is expensive.
WINNER_WEIGHT = 2.0
LOSER_WEIGHT = 1.0

# Reversal premium: cards at a deep dip (peak_discount > 30%) that
# successfully reversed (y > 0) get extra weight. This teaches the model
# to pay special attention to the buy-low-sell-high pattern — correctly
# distinguishing "dipped and will recover" from "dipped and will keep
# falling" is the highest-value skill for an investor.
REVERSAL_PREMIUM = 1.5  # multiplied on top of WINNER_WEIGHT
DEEP_DIP_THRESHOLD = 0.30

# Survival probability threshold: only cards above this pass to Stage 2
# at inference. Tunable — lower = more picks, higher = fewer but safer.
SURVIVAL_THRESHOLD = 0.40


def _build_weights(y: np.ndarray, peak_discounts: np.ndarray) -> np.ndarray:
    """Compute asymmetric sample weights with reversal premium.

    Weight matrix:
      - Loser (y <= 0):                       1.0
      - Winner (y > 0):                       2.0
      - Winner from deep dip (peak > 30%):    2.0 × 1.5 = 3.0

    The reversal premium teaches the model: when you see a deep dip,
    get REALLY good at distinguishing recoveries from continued declines.
    """
    weights = np.where(y > 0, WINNER_WEIGHT, LOSER_WEIGHT)
    reversal_mask = (y > 0) & (peak_discounts > DEEP_DIP_THRESHOLD)
    weights[reversal_mask] *= REVERSAL_PREMIUM
    return weights


def train_two_stage(
    db,
    version_tag: Optional[str] = None,
    n_bootstrap: int = 30,
) -> Dict[str, Any]:
    """Train the two-stage filter+rank model suite.

    Returns metrics dict with both stage performances.
    """
    date_str = dt.date.today().isoformat()
    version = version_tag or f"v2_0_{date_str}"

    logger.info("Training two-stage model %s", version)

    # Build dataset
    df = build_training_dataset(db)
    if df.empty or len(df) < 200:
        return {"error": "insufficient_data", "samples": len(df)}

    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    cutoff = df["anchor_date"].max() - pd.DateOffset(months=HOLDOUT_MONTHS)

    train_full = df[df["anchor_date"] < cutoff]
    test = df[df["anchor_date"] >= cutoff]

    if len(train_full) < 100 or len(test) < 30:
        return {"error": "insufficient_split"}

    logger.info("Train: %d, Test: %d, Features: %d (collider dropped)",
                len(train_full), len(test), len(FEATURES_V2))

    # Split train into train + calibration for isotonic
    cal_n = int(len(train_full) * CAL_FRACTION)
    train_full = train_full.sort_values("anchor_date")
    cal_df = train_full.tail(cal_n)
    train_df = train_full.head(len(train_full) - cal_n)

    X_train = train_df[FEATURES_V2].values
    y_train = train_df[TARGET_COL].values
    X_cal = cal_df[FEATURES_V2].values
    y_cal = cal_df[TARGET_COL].values
    X_test = test[FEATURES_V2].values
    y_test = test[TARGET_COL].values

    y_train_log = np.log1p(np.clip(y_train, -0.999, None))

    # Peak discount for reversal weighting
    peak_disc_idx = FEATURES_V2.index("peak_discount")
    peak_disc_train = X_train[:, peak_disc_idx]

    # ================================================================
    # STAGE 1: Survival classifier — "will this card be net-positive?"
    # ================================================================
    y_binary_train = (y_train > 0).astype(int)
    y_binary_cal = (y_cal > 0).astype(int)

    clf = lgb.LGBMClassifier(**CLF_PARAMS)
    clf.fit(X_train, y_binary_train)

    # Isotonic calibration on held-out calibration slice
    proba_cal_raw = clf.predict_proba(X_cal)[:, 1]
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(proba_cal_raw, y_binary_cal)

    # Evaluate Stage 1 on test
    proba_test_raw = clf.predict_proba(X_test)[:, 1]
    proba_test = iso.predict(proba_test_raw)
    survival_mask = proba_test >= SURVIVAL_THRESHOLD

    stage1_metrics = {
        "survival_threshold": SURVIVAL_THRESHOLD,
        "test_pass_rate": float(survival_mask.mean()),
        "test_precision_at_threshold": float(
            (y_test[survival_mask] > 0).mean() if survival_mask.sum() > 0 else 0
        ),
        "test_recall_positive": float(
            survival_mask[y_test > 0].mean() if (y_test > 0).sum() > 0 else 0
        ),
    }
    logger.info("Stage 1: pass_rate=%.1f%% precision=%.1f%% recall=%.1f%%",
                stage1_metrics["test_pass_rate"] * 100,
                stage1_metrics["test_precision_at_threshold"] * 100,
                stage1_metrics["test_recall_positive"] * 100)

    # ================================================================
    # STAGE 2: Conviction ranker with asymmetric weights + reversal premium
    # ================================================================
    sample_weights = _build_weights(y_train, peak_disc_train)

    # Bootstrap ensemble for Stage 2 (variance reduction on the ranking)
    rng = np.random.default_rng(seed=42)
    models = []
    for i in range(n_bootstrap):
        idx = rng.integers(0, len(X_train), size=len(X_train))
        params = {**REG_PARAMS, "seed": 42 + i}
        model = lgb.LGBMRegressor(**params)
        model.fit(X_train[idx], y_train_log[idx], sample_weight=sample_weights[idx])
        models.append(model)
        if (i + 1) % 10 == 0:
            logger.info("  Stage 2: trained %d/%d bootstrap models", i + 1, n_bootstrap)

    # Predict on test — median across ensemble, then expm1 back to linear
    preds_per_model = np.array([
        np.expm1(m.predict(X_test)) for m in models
    ])
    pred_median = np.median(preds_per_model, axis=0)

    # Combined score: Stage 2 ranking × Stage 1 survival probability
    # This naturally suppresses cards with high predicted return but
    # low survival probability (the "moonshot that usually tanks" pattern).
    combined_score = pred_median * proba_test

    # Full test metrics (before filter — for comparison to v1_3)
    r2_full = float(r2_score(y_test, pred_median))
    sp_full = float(spearmanr(y_test, pred_median).statistic)

    # Filtered test metrics (after Stage 1 filter — what investor sees)
    if survival_mask.sum() > 10:
        y_filt = y_test[survival_mask]
        pred_filt = pred_median[survival_mask]
        sp_filt = float(spearmanr(y_filt, pred_filt).statistic)
        hit_filt = float((y_filt > 0).mean())
    else:
        sp_filt = 0.0
        hit_filt = 0.0

    # Top-2% and top-1% on combined score (whole test set)
    n_top2 = max(1, int(round(0.02 * len(y_test))))
    n_top1 = max(1, int(round(0.01 * len(y_test))))
    top2_idx = np.argsort(combined_score)[-n_top2:]
    top1_idx = np.argsort(combined_score)[-n_top1:]

    top2_net = float(np.mean(y_test[top2_idx]))
    top2_hit = float((y_test[top2_idx] > 0).mean())
    top1_net = float(np.mean(y_test[top1_idx]))
    top1_hit = float((y_test[top1_idx] > 0).mean())

    # Reversal-specific metrics: among cards with peak_discount > 0.3,
    # what's the hit rate of our top picks?
    peak_disc_test = X_test[:, peak_disc_idx]
    dip_mask = peak_disc_test > DEEP_DIP_THRESHOLD
    if dip_mask.sum() > 0 and survival_mask.sum() > 0:
        dip_surv = dip_mask & survival_mask
        dip_hit = float((y_test[dip_surv] > 0).mean()) if dip_surv.sum() > 0 else 0
    else:
        dip_hit = 0.0

    metrics = {
        "model_version": version,
        "architecture": "two_stage_filter_rank",
        "features_dropped": ["psa_10_vs_raw_pct"],
        "n_features": len(FEATURES_V2),
        "n_bootstrap": n_bootstrap,
        "total_samples": len(df),
        "train_samples": len(train_df),
        "cal_samples": len(cal_df),
        "test_samples": len(test),
        "stage1": stage1_metrics,
        "stage2": {
            "winner_weight": WINNER_WEIGHT,
            "loser_weight": LOSER_WEIGHT,
            "reversal_premium": REVERSAL_PREMIUM,
            "deep_dip_threshold": DEEP_DIP_THRESHOLD,
        },
        "full_test": {
            "r_squared_oos": round(r2_full, 4),
            "spearman_oos": round(sp_full, 4),
        },
        "filtered_test": {
            "spearman_filtered": round(sp_filt, 4),
            "hit_rate_filtered": round(hit_filt, 4),
            "n_survivors": int(survival_mask.sum()),
        },
        "conviction": {
            "top2_net": round(top2_net, 4),
            "top2_hit": round(top2_hit, 4),
            "n_top2": int(n_top2),
            "top1_net": round(top1_net, 4),
            "top1_hit": round(top1_hit, 4),
            "n_top1": int(n_top1),
        },
        "reversal": {
            "deep_dip_survivors_hit_rate": round(dip_hit, 4),
            "deep_dip_n": int(dip_mask.sum()),
            "deep_dip_survived_n": int((dip_mask & survival_mask).sum()),
        },
    }

    logger.info("=== Two-Stage Report Card ===")
    logger.info("Full: R²=%.4f Spearman=%.4f", r2_full, sp_full)
    logger.info("Filtered: Spearman=%.4f hit=%.1f%% n=%d",
                sp_filt, hit_filt * 100, survival_mask.sum())
    logger.info("Top-2%%: net=%.2f%% hit=%.1f%%", top2_net * 100, top2_hit * 100)
    logger.info("Top-1%%: net=%.2f%% hit=%.1f%%", top1_net * 100, top1_hit * 100)
    logger.info("Reversal (dip survivors): hit=%.1f%%", dip_hit * 100)

    # Save artifacts
    clf_path = MODELS_DIR / f"clf_{version}.lgb"
    clf.booster_.save_model(str(clf_path))

    iso_path = MODELS_DIR / f"iso_{version}.json"
    iso_path.write_text(json.dumps({
        "X_thresholds": iso.X_thresholds_.tolist() if hasattr(iso, 'X_thresholds_') else [],
        "y_thresholds": iso.y_thresholds_.tolist() if hasattr(iso, 'y_thresholds_') else [],
    }))

    for i, m in enumerate(models):
        m_path = MODELS_DIR / f"two_stage_reg_{version}_{i:03d}.lgb"
        m.booster_.save_model(str(m_path))

    meta = {
        "version": version,
        "architecture": "two_stage_filter_rank",
        "features": FEATURES_V2,
        "n_bootstrap": n_bootstrap,
        "clf_params": CLF_PARAMS,
        "reg_params": REG_PARAMS,
        "survival_threshold": SURVIVAL_THRESHOLD,
        "winner_weight": WINNER_WEIGHT,
        "reversal_premium": REVERSAL_PREMIUM,
        "trained_at": dt.datetime.utcnow().isoformat(),
    }
    meta_path = MODELS_DIR / f"two_stage_meta_{version}.json"
    meta_path.write_text(json.dumps(meta, indent=2, default=str))

    metrics_path = MODELS_DIR / f"two_stage_metrics_{version}.json"
    metrics_path.write_text(json.dumps(metrics, indent=2, default=str))

    logger.info("Saved: clf + iso + %d regressors + meta + metrics", n_bootstrap)
    return metrics
