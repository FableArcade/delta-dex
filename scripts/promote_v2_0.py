"""Run the promotion gate on the liquid-universe walkforward metrics.

Takes the most recent walkforward_liquid_*.json and feeds its metrics
into promotion_gate.evaluate_and_record(). Writes the decision to
model_promotion_log + flips model_report_card.promotion_status.

Usage: python -m scripts.promote_v2_0 --version v2_0_2026-04-16
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection import get_db
from pipeline.model.promotion_gate import evaluate_and_record

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("promote_v2_0")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True,
                        help="Model version to promote, e.g. v2_0_2026-04-16")
    parser.add_argument("--walkforward", default=None,
                        help="Path to walkforward JSON (default: latest walkforward_liquid_*)")
    args = parser.parse_args()

    if args.walkforward:
        wf_path = Path(args.walkforward)
    else:
        candidates = sorted(MODELS_DIR.glob("walkforward_liquid_*.json"), reverse=True)
        if not candidates:
            raise SystemExit("No walkforward_liquid_*.json found")
        wf_path = candidates[0]
    logger.info("Using walkforward results from: %s", wf_path)

    with open(wf_path) as f:
        wf = json.load(f)

    # Remap walkforward_liquid output keys to promotion_gate expected keys.
    # walkforward_liquid writes top2_net_return, top2_hit_rate, top2_sharpe,
    # spearman_oos, n_top2; promotion_gate reads those directly.
    metrics = {
        "top2_hit_rate": wf["top2_hit_rate"],
        "top2_net_return": wf["top2_net_return"],
        "top2_sharpe": wf["top2_sharpe"],
        "spearman_oos": wf["spearman_oos"],
        "n_top2": wf["n_top2"],
        "top_decile_hit_rate": wf["top_decile_hit_rate"],
        "top_decile_net_return": wf["top_decile_net_return"],
        "top_decile_sharpe": wf["top_decile_sharpe"],
        "sharpe": wf["top_decile_sharpe"],  # legacy alias
        "n_predictions": wf["n_predictions"],
        "universe": wf.get("universe", "top_1000_by_median_psa10_price"),
        "collider_dropped": wf.get("collider_dropped", "psa_10_vs_raw_pct"),
        "liquidity_diagnostics": wf.get("liquidity_diagnostics", {}),
    }

    # Ensure the model_report_card row exists for this version, so the
    # promotion_status update in record_decision actually lands on a row.
    with get_db() as db:
        row = db.execute(
            "SELECT model_version FROM model_report_card "
            "WHERE model_version = ? LIMIT 1",
            (args.version,),
        ).fetchone()
        if not row:
            # Insert a minimal report_card row so promotion can record against it.
            import datetime as dt
            db.execute(
                """INSERT INTO model_report_card
                   (model_version, as_of, horizon_days, total_samples,
                    r_squared_oos, spearman_oos, mean_return_top_decile,
                    mean_return_bottom_decile, decile_spread, hit_rate_positive,
                    calibration_json, feature_importance_json,
                    promotion_status, promotion_reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    args.version, dt.date.today().isoformat(), 180,
                    wf.get("n_predictions", 0),
                    wf.get("r_squared_oos", 0.0),
                    wf.get("spearman_oos", 0.0),
                    wf.get("top_decile_net_return", 0.0),
                    0.0,  # bottom decile — not in walkforward output
                    0.0,  # decile spread
                    wf.get("hit_rate", 0.0),
                    json.dumps({}), json.dumps({}),
                    "pending", "pre-gate insert",
                ),
            )
            db.commit()
            logger.info("Inserted model_report_card row for %s", args.version)

        decision = evaluate_and_record(db, metrics, args.version)

    print("\n" + "=" * 70)
    print(f"PROMOTION GATE — {args.version}")
    print("=" * 70)
    print(f"DECISION: {decision.decision.upper()}")
    print(f"REASON:   {decision.reason}")
    print()
    print("Metrics:")
    print(f"  top-2% hit rate:   {decision.top2_hit_rate}")
    print(f"  top-2% net:        {decision.top2_net}")
    print(f"  top-2% Sharpe:     {decision.top2_sharpe}")
    print(f"  Spearman OOS:      {decision.spearman_oos}")
    print(f"  N top-2%:          {decision.n_top2}")
    print()

    if decision.decision == "promoted":
        latest = MODELS_DIR / "latest_ensemble_version.txt"
        latest.write_text(args.version)
        print(f"✓ Updated {latest.name} → {args.version}")
        print()
        print("Next: redeploy Docker image for HF Spaces + run pipeline/model/predict.py")
    else:
        print("✗ Model NOT promoted. latest_ensemble_version.txt unchanged.")
    print()


if __name__ == "__main__":
    main()
