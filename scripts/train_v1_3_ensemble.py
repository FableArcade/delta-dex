"""Train a bootstrap-aggregated ensemble of the v1_3 regressor.

The existing v1_3 median/lower/upper models are single-fit LightGBM
learners. Median-of-ensemble typically tightens projections and raises
R² / Spearman by 10-20% because bagging reduces the variance of the
decision-tree splits.

Design:

  - N_BOOTSTRAP = 30 models.
  - Each model trains on a bootstrap resample (sample with replacement)
    of the 180d training set, with a distinct random seed.
  - Same LightGBM hyperparameters as train.py (median quantile, alpha=0.5).
  - Saves 30 artifacts + an ensemble metadata JSON.
  - Writes latest_ensemble_version.txt so predict.py can pick it up.

Run:   python -m scripts.train_v1_3_ensemble
Output: data/models/ensemble_v1_3_<YYYY-MM-DD>_NNN.lgb (30 files)
        data/models/ensemble_meta_v1_3_<date>.json
        data/models/latest_ensemble_version.txt
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import lightgbm as lgb
import numpy as np

from db.connection import get_db
from pipeline.model.features import FEATURE_COLUMNS, TARGET_COL, build_training_dataset
from pipeline.model.liquid_universe import filter_to_liquid_universe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("train_v1_3_ensemble")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

BASE_PARAMS = {
    "objective": "quantile",
    "alpha": 0.5,
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

N_BOOTSTRAP = 30


def main() -> None:
    date_str = dt.date.today().isoformat()
    # v2_1 — v2_0 + 3 catalyst features (set-release recency): 28 features.
    # Signal Class 4 (cultural/meta catalyst). Uses existing set release
    # dates, no new data dependency. See pipeline/model/catalyst.py.
    version = f"v2_1_{date_str}"
    logger.info("Training ensemble %s with %d bootstrap models", version, N_BOOTSTRAP)

    with get_db() as db:
        df_full = build_training_dataset(db)
    if df_full.empty:
        raise SystemExit("No training samples.")
    logger.info("Full dataset: %d samples × %d features", len(df_full), len(FEATURE_COLUMNS))

    # Filter to investable universe (median PSA 10 ≥ $100)
    df = filter_to_liquid_universe(df_full)
    logger.info("Liquid dataset: %d samples, %d cards",
                len(df), df["card_id"].nunique())

    X = df[FEATURE_COLUMNS].values
    y_raw = df[TARGET_COL].values
    y = np.log1p(np.clip(y_raw, -0.999, None))    # matches train.py

    n = len(X)
    rng = np.random.default_rng(seed=42)

    for i in range(N_BOOTSTRAP):
        idx = rng.integers(0, n, size=n)           # bootstrap resample
        Xb, yb = X[idx], y[idx]
        params = {**BASE_PARAMS, "seed": 42 + i}
        model = lgb.LGBMRegressor(**params)
        model.fit(Xb, yb)
        out = MODELS_DIR / f"ensemble_{version}_{i:03d}.lgb"
        model.booster_.save_model(str(out))
        logger.info("  [%02d/%d] saved %s", i + 1, N_BOOTSTRAP, out.name)

    meta = {
        "ensemble_version": version,
        "n_bootstrap_models": N_BOOTSTRAP,
        "training_samples": int(n),
        "features": FEATURE_COLUMNS,
        "horizon_days": 180,
        "target_transform": "log1p",
        "trained_at": dt.datetime.utcnow().isoformat(),
    }
    meta_path = MODELS_DIR / f"ensemble_meta_{version}.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    latest = MODELS_DIR / "latest_ensemble_version.txt"
    latest.write_text(version)
    logger.info("Wrote %s + %s", meta_path.name, latest.name)
    logger.info("Done: ensemble %s ready for inference.", version)


if __name__ == "__main__":
    main()
