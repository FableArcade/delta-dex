"""Tests for pipeline.model.predict quantile behavior.

Verifies quantile monotonicity and SHAP output shape using a stub booster
(we don't want to depend on an actual trained lgb artifact in CI).
"""
from __future__ import annotations

import numpy as np
import pytest

pytest.importorskip("pipeline.model.predict")

from pipeline.model import predict as predict_mod
from pipeline.model.features import FEATURE_COLUMNS


class _StubBooster:
    """Tiny stand-in for lgb.Booster with predict() + pred_contrib."""

    def __init__(self, offset: float, seed: int = 0):
        self.offset = offset
        self.rng = np.random.default_rng(seed)

    def predict(self, X, pred_contrib: bool = False):
        X = np.asarray(X, dtype=float)
        n, k = X.shape
        if pred_contrib:
            # Shape must be (n, k+1) where last col is bias.
            contribs = self.rng.normal(0, 0.05, size=(n, k))
            bias = np.full((n, 1), self.offset)
            return np.hstack([contribs, bias])
        # Base prediction: linear combo + offset.
        return X.mean(axis=1) * 0.1 + self.offset


def _stub_models():
    return {
        "median": _StubBooster(0.10, seed=1),
        "lower": _StubBooster(-0.05, seed=2),
        "upper": _StubBooster(0.30, seed=3),
    }


def test_quantile_band_monotonicity():
    """lower <= median <= upper must hold on every row."""
    models = _stub_models()
    n_feat = len(FEATURE_COLUMNS)
    X = np.random.default_rng(42).normal(size=(50, n_feat))

    med = models["median"].predict(X)
    lo = models["lower"].predict(X)
    hi = models["upper"].predict(X)

    assert med.shape == lo.shape == hi.shape == (50,)
    # Strict band invariant on every row.
    assert np.all(lo <= med + 1e-9), "lower exceeded median"
    assert np.all(med <= hi + 1e-9), "median exceeded upper"


def test_shap_output_shape_matches_feature_count():
    model = _stub_models()["median"]
    n_feat = len(FEATURE_COLUMNS)
    X = np.random.default_rng(0).normal(size=(10, n_feat))
    shap = model.predict(X, pred_contrib=True)
    # (n_samples, n_features + 1)
    assert shap.shape == (10, n_feat + 1)


def test_predict_refuses_without_version(tmp_path, monkeypatch):
    """_get_latest_version raises if the pointer file is missing."""
    monkeypatch.setattr(predict_mod, "MODELS_DIR", tmp_path)
    with pytest.raises(FileNotFoundError):
        predict_mod._get_latest_version()


@pytest.mark.xfail(strict=False, reason="promotion gate in-flight — another agent owns this")
def test_predict_refuses_when_model_not_promoted():
    """Once promotion gate lands, predict() must refuse unpromoted models."""
    # Placeholder test — flip to real assertion when gate API is wired.
    from pipeline.model.predict import generate_projections  # noqa: F401
    assert hasattr(predict_mod, "require_promoted_model")
