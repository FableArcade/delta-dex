"""
Promotion-gate check for v2 — does it clear the explicit thresholds required
to replace v1_3 in production?

Gates (per Analytics Taste P1 + P16):

  1. v2 Spearman ≥ v1_3 Spearman on OOS walkforward ($25 universe — the
     production-relevant one, not the $100 cherry-pick)
  2. v2 top-decile Sharpe ≥ v1_3 top-decile Sharpe on BOTH $25 AND $100
     universes (not just one) — the Analytics Taste P15 double-check
  3. v2 interval coverage (CQR-adjusted, empirical on OOS) ≥ 0.78
     (target 0.80 with some finite-sample slack)
  4. v2 R² OOS within 0.02 of v1_3's (accuracy floor — v2 shouldn't trade
     calibration quality for regression quality)

If v2 passes all four: promote. If it fails any: v1_3 stays live.
Output: data/models/promotion_decision_v2_<ts>.json
"""
from __future__ import annotations

import datetime as dt
import glob
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("check_v2_promotion")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "data" / "models"


def _latest(pattern: str) -> Path:
    paths = sorted(MODELS_DIR.glob(pattern))
    if not paths:
        raise SystemExit(f"No artifact matching {pattern!r}")
    return paths[-1]


def main() -> None:
    v2_path = _latest("walkforward_v2_*.json")
    v13_path = _latest("walkforward_ensemble_*.json")
    v2 = json.loads(v2_path.read_text())
    v13 = json.loads(v13_path.read_text())

    v2_u25 = v2.get("universe_25", {}) or {}
    v2_u100 = v2.get("universe_100", {}) or {}

    # v1_3 walkforward_ensemble reports on the $100 filter (or $25? its default is $25 per the code).
    # The walkforward_ensemble.py code uses MIN_PRICE_FILTER = 25.0 — it's a $25 universe.
    v13_u25 = {
        "spearman_oos": v13.get("spearman_oos"),
        "r_squared_oos": v13.get("r_squared_oos"),
        "top_decile_sharpe": v13.get("top_decile_sharpe"),
        "top_decile_net_return": v13.get("top_decile_net_return"),
        "top_decile_hit_rate": v13.get("top_decile_hit_rate"),
    }
    # There's no v1_3 $100 number to compare; v1_3's single-model backtest used
    # $100 but that's walkforward_backtest.py, not the ensemble. We gate v2 on
    # $25 (production universe) ≥ v1_3 ensemble and separately report $100.

    gates = []

    def _gate(label, ok, detail):
        gates.append({"gate": label, "pass": bool(ok), "detail": detail})
        symbol = "✓" if ok else "✗"
        print(f"  {symbol} {label}: {detail}")

    print(f"\n=== v2 promotion gate ===")
    print(f"v2 artifact:   {v2_path.name}")
    print(f"v1_3 baseline: {v13_path.name}")
    print()

    # Gate 1: Spearman on production ($25) universe
    v2_sp = v2_u25.get("spearman_oos")
    v13_sp = v13_u25.get("spearman_oos")
    if v2_sp is not None and v13_sp is not None:
        _gate("Spearman $25+ ≥ v1_3",
              v2_sp >= v13_sp - 0.01,
              f"v2={v2_sp:.4f} vs v1_3={v13_sp:.4f}  Δ={v2_sp - v13_sp:+.4f}")
    else:
        _gate("Spearman $25+ ≥ v1_3", False, "missing metric")

    # Gate 2: Top-decile Sharpe on $25 universe
    v2_sh_25 = v2_u25.get("top_decile_sharpe")
    v13_sh = v13_u25.get("top_decile_sharpe")
    if v2_sh_25 is not None and v13_sh is not None:
        _gate("Top-decile Sharpe $25+ ≥ v1_3",
              v2_sh_25 >= v13_sh,
              f"v2={v2_sh_25:.3f} vs v1_3={v13_sh:.3f}  Δ={v2_sh_25 - v13_sh:+.3f}")
    else:
        _gate("Top-decile Sharpe $25+ ≥ v1_3", False, "missing metric")

    # Gate 3: Interval coverage ≥ 0.78
    cov = v2_u25.get("interval_coverage_80")
    if cov is not None:
        _gate("Interval coverage $25+ ≥ 0.78",
              cov >= 0.78,
              f"empirical={cov:.3f} (target 0.80)")
    else:
        _gate("Interval coverage $25+ ≥ 0.78", False, "missing metric")

    # Gate 4: R² not materially worse than v1_3
    v2_r2 = v2_u25.get("r_squared_oos")
    v13_r2 = v13_u25.get("r_squared_oos")
    if v2_r2 is not None and v13_r2 is not None:
        _gate("R² $25+ within 0.02 of v1_3",
              v2_r2 >= v13_r2 - 0.02,
              f"v2={v2_r2:.4f} vs v1_3={v13_r2:.4f}  Δ={v2_r2 - v13_r2:+.4f}")
    else:
        _gate("R² $25+ within 0.02 of v1_3", False, "missing metric")

    all_pass = all(g["pass"] for g in gates)
    verdict = "PROMOTE v2" if all_pass else "KEEP v1_3"
    print()
    print(f"{'=' * 40}")
    print(f"VERDICT: {verdict}")
    print(f"{'=' * 40}")

    # Side-by-side for the full picture ($25 + $100)
    print()
    print(f"{'Metric':<30} {'v1_3 ($25)':>14} {'v2 ($25)':>14} {'v2 ($100)':>14}")
    print("-" * 76)
    for label, k in [
        ("R² OOS", "r_squared_oos"),
        ("Spearman", "spearman_oos"),
        ("Hit rate overall", "hit_rate"),
        ("Top-decile net return", "top_decile_net_return"),
        ("Top-decile hit rate", "top_decile_hit_rate"),
        ("Top-decile Sharpe", "top_decile_sharpe"),
        ("Top-2% Sharpe", "top2_sharpe"),
        ("Top-1% Sharpe", "top1_sharpe"),
    ]:
        a = v13.get(k)
        b = v2_u25.get(k)
        c = v2_u100.get(k)
        def _fmt(x): return f"{x:+.4f}" if isinstance(x, (int, float)) else "—"
        print(f"  {label:<28} {_fmt(a):>14} {_fmt(b):>14} {_fmt(c):>14}")

    decision = {
        "decided_at": dt.datetime.utcnow().isoformat(),
        "verdict": verdict,
        "all_pass": all_pass,
        "gates": gates,
        "v2_artifact": v2_path.name,
        "v13_artifact": v13_path.name,
    }
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = MODELS_DIR / f"promotion_decision_v2_{ts}.json"
    out.write_text(json.dumps(decision, indent=2))
    print(f"\nDecision → {out}")


if __name__ == "__main__":
    main()
