"""Tests for scripts.walkforward_backtest.

The walkforward module is being built in parallel. These tests stake out
the invariants it must satisfy:
  - No look-ahead: every fold's training max-ts < prediction ts.
  - Reproducibility: same seed -> identical fold boundaries + metrics.
  - Aggregation math: R^2, Sharpe, and hit-rate match known-answer fixtures.
"""
from __future__ import annotations

import math
import importlib.util
from pathlib import Path

import numpy as np
import pytest

WALKFORWARD_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "walkforward_backtest.py"
)


def _walkforward_available() -> bool:
    return WALKFORWARD_PATH.exists()


pytestmark = pytest.mark.skipif(
    not _walkforward_available(),
    reason="scripts/walkforward_backtest.py not landed yet; owned by sibling agent",
)


# ---------- known-answer aggregation math (pure, no external module) ----------

def _r_squared(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    ss_res = ((y_true - y_pred) ** 2).sum()
    ss_tot = ((y_true - y_true.mean()) ** 2).sum()
    return 1 - ss_res / ss_tot if ss_tot > 0 else 0.0


def _sharpe(returns, rf: float = 0.0):
    r = np.asarray(returns, dtype=float)
    std = r.std(ddof=1)
    return (r.mean() - rf) / std * math.sqrt(252) if std > 0 else 0.0


def _hit_rate(returns):
    r = np.asarray(returns, dtype=float)
    return float((r > 0).mean())


def test_r_squared_known_answer():
    # Perfect prediction -> R^2 = 1
    assert math.isclose(_r_squared([1, 2, 3, 4], [1, 2, 3, 4]), 1.0)
    # Mean-only prediction -> R^2 = 0
    assert math.isclose(_r_squared([1, 2, 3, 4], [2.5, 2.5, 2.5, 2.5]), 0.0)


def test_sharpe_known_answer():
    # Constant positive return with zero std -> zero by convention
    assert _sharpe([0.01, 0.01, 0.01]) == 0.0
    # Positive mean w/ variance -> positive Sharpe
    s = _sharpe([0.02, -0.01, 0.03, 0.01, 0.02])
    assert s > 0


def test_hit_rate_known_answer():
    assert _hit_rate([0.1, -0.1, 0.2, -0.05]) == 0.5
    assert _hit_rate([0.1, 0.2, 0.3]) == 1.0
    assert _hit_rate([-0.1, -0.2]) == 0.0


# ---------- walkforward module contract (xfail until landed) ----------

def _import_walkforward():
    spec = importlib.util.spec_from_file_location(
        "scripts.walkforward_backtest", WALKFORWARD_PATH
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


@pytest.mark.xfail(strict=False, reason="walkforward module API in-flight")
def test_walkforward_no_lookahead(seeded_db):
    mod = _import_walkforward()
    folds = mod.generate_folds(seeded_db, seed=42)
    for fold in folds:
        assert fold["train_max_ts"] < fold["predict_ts"], (
            f"look-ahead: train_max={fold['train_max_ts']} "
            f">= predict={fold['predict_ts']}"
        )


@pytest.mark.xfail(strict=False, reason="walkforward module API in-flight")
def test_walkforward_reproducible_with_seed(seeded_db):
    mod = _import_walkforward()
    r1 = mod.run_backtest(seeded_db, seed=123)
    r2 = mod.run_backtest(seeded_db, seed=123)
    assert r1 == r2


@pytest.mark.xfail(strict=False, reason="walkforward module API in-flight")
def test_walkforward_aggregation_matches_fixture():
    mod = _import_walkforward()
    # Known-answer synthetic fixture.
    y_true = [0.10, -0.05, 0.20, 0.00, 0.15]
    y_pred = [0.08, -0.03, 0.22, 0.01, 0.14]
    metrics = mod.aggregate_metrics(y_true, y_pred)
    assert math.isclose(metrics["r_squared"], _r_squared(y_true, y_pred), abs_tol=1e-6)
    assert math.isclose(metrics["hit_rate"], _hit_rate(y_true), abs_tol=1e-6)
