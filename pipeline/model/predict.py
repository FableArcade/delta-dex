"""Inference module for PokeDelta prediction engine.

Loads trained models and generates projections for all active cards,
storing results in the model_projections table.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from typing import List

import lightgbm as lgb
import numpy as np

from pipeline.model.features import FEATURE_COLUMNS, HORIZON_DAYS, build_live_features
from pipeline.model.liquid_universe import filter_to_liquid_universe
from pipeline.model.promotion_gate import is_promoted
from pipeline.model.provenance import feature_hash, load_training_cutoff

logger = logging.getLogger("pipeline.model.predict")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"


def _load_ensemble(version: str) -> List[lgb.Booster]:
    """Load the full bootstrap ensemble for a given version.

    The walkforward evaluates ensemble median predictions — deployment
    must do the same (Analytics Taste P15). Returns all ensemble_{version}_NNN.lgb
    artifacts sorted by index.
    """
    boosters = []
    i = 0
    while True:
        path = MODELS_DIR / f"ensemble_{version}_{i:03d}.lgb"
        if not path.exists():
            break
        boosters.append(lgb.Booster(model_file=str(path)))
        i += 1
    if not boosters:
        raise FileNotFoundError(
            f"No ensemble models found for version={version}. "
            f"Expected ensemble_{version}_000.lgb ... in {MODELS_DIR}"
        )
    logger.info("Loaded %d ensemble models for version %s", len(boosters), version)
    return boosters


def _get_latest_version() -> str:
    """Read the latest ENSEMBLE model version from the pointer file.

    Reads latest_ensemble_version.txt (canonical for v2_0+). Falls back
    to latest_version.txt for backward compatibility with single-fit
    deployments — but promotion gate flow writes latest_ensemble_version.txt.
    """
    ens_path = MODELS_DIR / "latest_ensemble_version.txt"
    if ens_path.exists():
        return ens_path.read_text().strip()
    path = MODELS_DIR / "latest_version.txt"
    if not path.exists():
        raise FileNotFoundError(
            "No trained model found. Run training first: "
            "python -m scripts.train_v1_3_ensemble"
        )
    return path.read_text().strip()


def generate_projections(
    db,
    version: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate 90-day return projections for all active cards.

    Loads the model, builds live feature vectors, runs inference,
    computes feature contributions, and writes to model_projections.

    Returns summary stats.
    """
    version = version or _get_latest_version()
    date_str = dt.date.today().isoformat()

    logger.info("Generating projections with model %s for %s", version, date_str)

    # Promotion gate: refuse to write projections for unpromoted models.
    # This keeps untested/regressed models out of the UI. We still log a
    # warning so the pipeline call site knows why no rows were produced.
    if not is_promoted(db, version):
        logger.warning(
            "BLOCKED by promotion gate: model %s has no 'promoted' status. "
            "Run scripts/walkforward_backtest.py then re-evaluate. "
            "Skipping writes to model_projections.",
            version,
        )
        # Audit trail: insert a single marker row at the bogus card_id '_gate_block_'
        # only if that card exists; otherwise just log and return.
        return {
            "cards_projected": 0,
            "model_version": version,
            "date": date_str,
            "blocked": True,
            "reason": "model_not_promoted",
        }

    # Load ensemble (30 bootstrap models)
    ensemble = _load_ensemble(version)
    feat_hash = feature_hash(FEATURE_COLUMNS)
    training_cutoff = load_training_cutoff(version, MODELS_DIR) or ""

    # Build features, then filter to the investable universe.
    # Training and inference must share the same card universe (Analytics
    # Taste P15 — eval corpus matches deployment corpus). Cards below the
    # price threshold simply get no projection row, which the UI already
    # handles (card detail shows empty state, leaderboards skip the card).
    features_df = build_live_features(db)
    if features_df.empty:
        logger.warning("No cards with valid features for projection")
        return {"cards_projected": 0}

    n_before = len(features_df)
    features_df = filter_to_liquid_universe(features_df)
    logger.info("Liquid filter at inference: %d -> %d cards",
                n_before, len(features_df))
    if features_df.empty:
        logger.warning("No cards in liquid universe at inference time")
        return {"cards_projected": 0}

    X = features_df[FEATURE_COLUMNS].values
    card_ids = features_df.index.tolist()

    # Run inference — each bootstrap model predicts independently in log-return
    # space; we take the median across all 30 for the point estimate and
    # use the p25/p75 across the ensemble as the confidence band. This is
    # what the walkforward evaluated, so it's what we deploy.
    preds_per_model = np.array([
        np.expm1(m.predict(X)) for m in ensemble
    ])  # shape: (n_ensemble, n_cards)
    pred_median = np.median(preds_per_model, axis=0)
    pred_lower = np.quantile(preds_per_model, 0.25, axis=0)
    pred_upper = np.quantile(preds_per_model, 0.75, axis=0)

    # SHAP contributions — aggregate across the ensemble by averaging in
    # log-space contributions. Each model contributes its own explanation;
    # averaging gives a stable driver attribution.
    try:
        shap_sum = None
        for m in ensemble:
            sv = m.predict(X, pred_contrib=True)
            shap_sum = sv if shap_sum is None else shap_sum + sv
        shap_values = shap_sum / len(ensemble)
        # shap_values has shape (n_samples, n_features + 1) where last col is bias
        has_shap = True
    except Exception as e:
        logger.warning("SHAP computation failed, skipping contributions: %s", e)
        has_shap = False

    # Write projections
    inserted = 0
    for i, card_id in enumerate(card_ids):
        projected_return = float(pred_median[i])
        conf_low = float(pred_lower[i])
        conf_high = float(pred_upper[i])
        conf_width = conf_high - conf_low

        # Feature contributions
        if has_shap:
            contribs = {}
            for j, feat_name in enumerate(FEATURE_COLUMNS):
                val = float(shap_values[i][j])
                if abs(val) > 0.001:  # only include meaningful contributions
                    contribs[feat_name] = round(val, 4)
            # Sort by absolute contribution
            contribs = dict(sorted(contribs.items(), key=lambda x: -abs(x[1])))
            contribs_json = json.dumps(contribs)
        else:
            contribs_json = "{}"

        db.execute(
            """INSERT OR REPLACE INTO model_projections
               (card_id, as_of, horizon_days, projected_return,
                confidence_low, confidence_high, confidence_width,
                feature_contributions, model_version,
                training_cutoff, feature_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                card_id, date_str, HORIZON_DAYS, projected_return,
                conf_low, conf_high, conf_width,
                contribs_json, version,
                training_cutoff, feat_hash,
            ),
        )
        inserted += 1

    # Cleanup pass: remove stale projections from prior model versions
    # OR from cards no longer in the liquid universe. Filtering by
    # model_version avoids SQLite's ~999-parameter limit we'd hit with
    # a card-id IN clause, and cleanly guarantees the table reflects
    # only today's deployed model.
    stale = db.execute(
        "DELETE FROM model_projections "
        "WHERE horizon_days = ? AND model_version != ?",
        (HORIZON_DAYS, version),
    ).rowcount
    if stale:
        logger.info("Cleared %d stale projections from prior model versions", stale)
    db.commit()

    logger.info("Generated projections for %d cards", inserted)

    # Summary stats
    return {
        "cards_projected": inserted,
        "model_version": version,
        "date": date_str,
        "median_projection": float(np.median(pred_median)),
        "mean_projection": float(np.mean(pred_median)),
        "positive_count": int((pred_median > 0).sum()),
        "negative_count": int((pred_median <= 0).sum()),
        "avg_confidence_width": float(np.mean(pred_upper - pred_lower)),
    }
