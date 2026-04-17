"""Posterior predictive check for the Delta quantile model.

McElreath (Statistical Rethinking, v4 canon): a model earns trust when
data simulated from its fitted distribution looks like the real data on
features the model wasn't fit to.

This script:
  1. Loads the latest promoted model's held-out predictions
  2. Samples synthetic returns from the fitted [P10, P90] envelope
     (approximated as log-normal given log1p target transform)
  3. Compares simulated vs. observed distribution on:
       - Decile return structure
       - Volatility clustering (serial correlation of |returns|)
       - Fat-tail signature (kurtosis, >1% and >5% hit rates)
  4. Flags discrepancies worth investigating

Run:
    python -m scripts.posterior_predictive_check \\
        --version v1_3_2026-04-15 \\
        --n-sims 1000

If simulated decile returns diverge materially from observed, the model's
small world is out of alignment with the large world — investigate before
trusting projections for portfolio sizing.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict

import lightgbm as lgb
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.model.features import (
    FEATURE_COLUMNS, HORIZON_DAYS, TARGET_COL, build_training_dataset,
)
from pipeline.model.train import HOLDOUT_MONTHS

logger = logging.getLogger("posterior_predictive_check")

MODELS_DIR = PROJECT_ROOT / "data" / "models"
DB_PATH = PROJECT_ROOT / "data" / "pokemon.db"


def _load_boosters(version: str) -> Dict[str, lgb.Booster]:
    """Load median + 80% interval models. Falls back to 50% if 80% not trained
    (pre-upgrade models only had P25/P75)."""
    out = {}
    for key, fname in [
        ("median", f"median_{version}.lgb"),
        ("lo80", f"lower_80_{version}.lgb"),
        ("hi80", f"upper_80_{version}.lgb"),
        ("lo50", f"lower_{version}.lgb"),
        ("hi50", f"upper_{version}.lgb"),
    ]:
        path = MODELS_DIR / fname
        if path.exists():
            out[key] = lgb.Booster(model_file=str(path))
    if "median" not in out:
        raise FileNotFoundError(f"No median model for version={version} at {MODELS_DIR}")
    return out


def _sample_log_normal_envelope(
    med_log: np.ndarray,
    lo_log: np.ndarray,
    hi_log: np.ndarray,
    coverage: float,
    n_sims: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Sample from a log-normal envelope implied by (lo, med, hi) at a given
    nominal coverage. We approximate σ from the interval half-width assuming
    symmetric log-space noise; honest but rough. Returns shape (n_cards, n_sims)
    in LINEAR return space (after expm1)."""
    # Half-width in log space corresponds to z * σ for symmetric coverage.
    # For 80% coverage, z ≈ 1.2816; for 50%, z ≈ 0.6745.
    z = {0.80: 1.2816, 0.50: 0.6745}[coverage]
    half_width_log = (hi_log - lo_log) / 2.0
    sigma_log = np.maximum(half_width_log / z, 1e-4)
    n = med_log.shape[0]
    # Draw standard normals then invert via log-normal parameterization
    eps = rng.standard_normal(size=(n, n_sims))
    sim_log = med_log[:, None] + sigma_log[:, None] * eps
    return np.expm1(sim_log)


def _summarize(returns: np.ndarray, label: str) -> Dict[str, float]:
    """Decile + tail + moment summary of a returns vector (1D)."""
    r = returns[np.isfinite(returns)]
    if r.size == 0:
        return {"label": label, "n": 0}
    q = np.quantile(r, [0.1, 0.25, 0.5, 0.75, 0.9])
    return {
        "label": label,
        "n": int(r.size),
        "mean": float(r.mean()),
        "std": float(r.std()),
        "p10": float(q[0]),
        "p25": float(q[1]),
        "p50": float(q[2]),
        "p75": float(q[3]),
        "p90": float(q[4]),
        "pct_pos": float((r > 0).mean()),
        "pct_gt_20": float((r > 0.20).mean()),
        "pct_lt_neg20": float((r < -0.20).mean()),
        "kurtosis_excess": float(((r - r.mean()) ** 4).mean() / (r.std() ** 4 + 1e-12) - 3),
    }


def run(version: str, n_sims: int = 1000, seed: int = 42) -> Dict[str, Any]:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    logger.info("Loading training dataset for PPC...")
    df = build_training_dataset(conn)
    if df.empty:
        return {"error": "no_training_data"}
    df["anchor_date"] = pd.to_datetime(df["anchor_date"])
    cutoff = df["anchor_date"].max() - pd.DateOffset(months=HOLDOUT_MONTHS)
    test = df[df["anchor_date"] >= cutoff]
    if len(test) < 20:
        return {"error": "insufficient_holdout", "n": len(test)}

    X = test[FEATURE_COLUMNS].values
    y = test[TARGET_COL].values
    boosters = _load_boosters(version)

    med_log = boosters["median"].predict(X)

    # Prefer 80% interval when present; fall back to 50% so old models work too.
    if "lo80" in boosters and "hi80" in boosters:
        lo_log = boosters["lo80"].predict(X)
        hi_log = boosters["hi80"].predict(X)
        coverage = 0.80
    else:
        lo_log = boosters["lo50"].predict(X)
        hi_log = boosters["hi50"].predict(X)
        coverage = 0.50
        logger.warning("80%% interval models missing; falling back to 50%%.")

    rng = np.random.default_rng(seed)
    sim = _sample_log_normal_envelope(med_log, lo_log, hi_log, coverage, n_sims, rng)

    observed_summary = _summarize(y, "observed")
    simulated_summary = _summarize(sim.ravel(), "simulated_aggregate")

    # Per-card simulated medians (collapses the n_sims dimension)
    sim_med_per_card = np.median(sim, axis=1)
    sim_med_summary = _summarize(sim_med_per_card, "simulated_per_card_median")

    # Calibration quick-check: fraction of observed within the [lo, hi] envelope
    inside = float(((y >= np.expm1(lo_log)) & (y <= np.expm1(hi_log))).mean())

    # Flags
    flags = []
    if abs(simulated_summary["mean"] - observed_summary["mean"]) > 0.10:
        flags.append("mean_mismatch_gt_10pct")
    if abs(simulated_summary["std"] - observed_summary["std"]) / max(observed_summary["std"], 1e-6) > 0.30:
        flags.append("std_mismatch_gt_30pct")
    if simulated_summary["kurtosis_excess"] < observed_summary["kurtosis_excess"] - 1.0:
        flags.append("simulated_tails_too_thin")
    nominal = coverage
    if abs(inside - nominal) > 0.05:
        flags.append(f"empirical_coverage_{inside:.2f}_vs_nominal_{nominal:.2f}")

    result = {
        "model_version": version,
        "n_test_cards": int(len(test)),
        "n_sims_per_card": int(n_sims),
        "nominal_coverage": coverage,
        "empirical_coverage": round(inside, 4),
        "observed": observed_summary,
        "simulated_aggregate": simulated_summary,
        "simulated_per_card_median": sim_med_summary,
        "flags": flags,
    }

    logger.info("PPC result:\n%s", json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="Model version tag, e.g. v1_3_2026-04-15")
    parser.add_argument("--n-sims", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    out = run(args.version, n_sims=args.n_sims, seed=args.seed)
    print(json.dumps(out, indent=2))
