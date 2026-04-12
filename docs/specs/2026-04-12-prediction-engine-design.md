# PokeDelta Prediction Engine — Design Spec

**Date:** 2026-04-12
**Goal:** Replace hand-tuned heuristic scores with a data-driven prediction model that projects 90-day returns with confidence bands, making every recommendation auditable and trustworthy.

## Problem

The current platform uses hardcoded weights (15% cultural, 25% demand, etc.) to score cards. These weights are opinions, not measurements. There is no proof they predict anything. An investor has no reason to trust a "Must Buy score of 73" because nobody can say whether cards that scored 73 historically outperformed cards that scored 40.

## Solution

Train a gradient-boosted regression model (LightGBM) on 5+ years of historical price data to learn which features actually predict 90-day PSA 10 price returns. Output per-card projections with confidence bands. Power all tabs from the model's projections.

## Branding

- Title: "PokeDelta" (replaces "PokeMetrics" in header/title)
- Badge: "Delta Edition" in top-right corner of every page
- Same Win98 aesthetic, same layout structure

---

## Architecture

### New Module: `pipeline/model/`

```
pipeline/model/
  __init__.py
  features.py       # Feature engineering from DB → training matrix
  train.py           # Model training + evaluation + persistence
  predict.py         # Inference: generate projections for all cards
  backtest.py        # Walk-forward validation + report card generation
```

### New DB Table: `model_projections`

```sql
CREATE TABLE IF NOT EXISTS model_projections (
    card_id         TEXT NOT NULL,
    as_of           TEXT NOT NULL,       -- date projection was generated
    horizon_days    INTEGER NOT NULL,    -- 90
    projected_return REAL,               -- median predicted return (e.g. 0.18 = +18%)
    confidence_low  REAL,                -- 25th percentile
    confidence_high REAL,                -- 75th percentile
    confidence_width REAL,               -- high - low (convenience)
    feature_contributions TEXT,          -- JSON: {"demand_momentum": 0.08, "peak_discount": 0.06, ...}
    model_version   TEXT,                -- e.g. "v1.0_2026-04-12"
    PRIMARY KEY (card_id, as_of, horizon_days)
);
```

### New DB Table: `model_report_card`

```sql
CREATE TABLE IF NOT EXISTS model_report_card (
    model_version       TEXT NOT NULL,
    as_of               TEXT NOT NULL,
    horizon_days        INTEGER NOT NULL,
    total_samples       INTEGER,
    r_squared_oos       REAL,            -- out-of-sample R-squared
    spearman_oos        REAL,            -- out-of-sample rank correlation
    mean_return_top_decile    REAL,      -- avg return of cards model liked most
    mean_return_bottom_decile REAL,      -- avg return of cards model liked least
    decile_spread       REAL,            -- top - bottom
    hit_rate_positive   REAL,            -- % of "projected positive" that were actually positive
    calibration_json    TEXT,            -- JSON: per-decile actual vs predicted
    feature_importance_json TEXT,        -- JSON: top features + importance scores
    PRIMARY KEY (model_version, as_of, horizon_days)
);
```

---

## Feature Engineering

### Training Target
For each card `c` at historical date `t`:
```
target = (psa_10_price[t+90] - psa_10_price[t]) / psa_10_price[t]
```
Only cards where both `t` and `t+90` have non-null PSA 10 prices >= $20.

### Feature Vector (at time `t`, no future leakage)

**Price-derived (8 features):**
- `ret_30d`: 30-day trailing return
- `ret_90d`: 90-day trailing return
- `ret_365d`: 365-day trailing return
- `peak_discount`: (max_1y - current) / max_1y
- `trough_recovery`: (current - min_1y) / min_1y
- `volatility`: (max_1y - min_1y) / current
- `ma_distance`: (avg(30d_ago, 90d_ago, 365d_ago) - current) / avg(...)
- `log_price`: log10(current PSA 10 price)

**Demand/Supply (6 features):**
- `net_flow_pct_7d`: 7-day net flow as % of active listings
- `net_flow_pct_30d`: 30-day net flow as % of active listings
- `demand_pressure_7d`: demand pressure (7-day window)
- `demand_pressure_30d`: demand pressure (30-day window)
- `supply_saturation_index`: 7d/30d active listings ratio
- `ds_ratio`: demand_pressure / supply_pressure

**Scarcity (3 features):**
- `gem_pct`: PSA 10 gem rate
- `psa_10_pop`: PSA 10 population count
- `psa_10_vs_raw_pct`: PSA 10 premium over raw %

**Categorical (2 features, label-encoded):**
- `cultural_score`: existing iconic name + rarity bonus (0-1)
- `rarity_tier`: rarity_code mapped to numeric tier

**Coverage (1 feature):**
- `history_days`: number of price observations in trailing 365d

**Total: 20 features**

### Historical Feature Assembly

Walk through price_history monthly (first observation per month per card):
1. For each card-month anchor where anchor has >= 90 days trailing AND >= 90 days forward:
   - Compute all 20 features using only data at-or-before anchor date
   - Compute target using price at anchor+90d
2. Trim top/bottom 1% of targets (outlier removal)
3. Require minimum 6 months trailing history per card

This reuses the methodology from the existing `backtest_wishlist_scorer.py` script.

---

## Model Training

### Algorithm: LightGBM

- Gradient-boosted decision trees
- Quantile regression for confidence bands (alpha=0.25, 0.50, 0.75)
- Three models trained: median, lower bound, upper bound

### Train/Test Split

Chronological: train on all data up to 12 months before latest date, test on most recent 12 months. No random shuffle (prevents future leakage).

### Hyperparameters (starting point)

```python
params = {
    "objective": "quantile",
    "alpha": 0.50,          # varies: 0.25, 0.50, 0.75
    "num_leaves": 31,
    "learning_rate": 0.05,
    "n_estimators": 500,
    "min_child_samples": 20,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
}
```

### Output

- `data/models/model_v{version}.pkl` — serialized model
- `data/models/feature_importance_v{version}.json`
- Populated `model_report_card` table

---

## Inference Pipeline

Runs after daily pipeline compute stage:

1. Load latest model from `data/models/`
2. For each active card, build feature vector from current DB state
3. Run median model → `projected_return`
4. Run quantile 0.25 model → `confidence_low`
5. Run quantile 0.75 model → `confidence_high`
6. Compute SHAP values → `feature_contributions` JSON
7. INSERT OR REPLACE into `model_projections`

### Retraining Schedule

Weekly (Sunday, after weekly pipeline). Model version = `v{n}_{date}`.

---

## Frontend Integration

### All Pages: PokeDelta Branding

- Page title: "PokeDelta" (was "PokeMetrics")
- Top-right corner badge on every page: "Delta Edition" styled as a Win98 badge/chip
- Same Win98 CSS, same layout, same color palette

### Card Detail Page (`card.html`)

New section: **"Model Projection"** below price chart:
- Projected 90-day return: "+18.2%"
- Confidence band: "+8.1% to +28.3%"
- Visual: horizontal bar showing the range, with median marked
- Feature waterfall: "Why this projection" — top 5 contributing features with signed contributions
- Model confidence label: "High" (band < 15%), "Medium" (15-30%), "Low" (> 30%)

### Must Buy Now (`card_leaderboard.html`, Must Buy tab)

**Replace heuristic score with model-driven ranking:**
- Sort by: `projected_return` WHERE `confidence_low > 0` (even pessimistic case is positive)
- Display columns: Rank, Card, Current Price, Projected Return, Confidence Band, Top Signal
- Hard gates remain: PSA 10 >= $20, not sealed, sufficient history
- Cultural/demand/scarcity gates REMOVED — the model decides what matters

### Watchlist (`wishlist.html`)

**Fit score becomes: projected_return x budget_fit x confidence_factor**
- `confidence_factor` = 1.0 - (confidence_width / 2) clamped to [0.3, 1.0]
- Budget fit logic unchanged (piecewise linear)
- Rationale shows feature contributions from model

### Long-Term Holds

Keep existing momentum-based scoring (the model is tuned for 90d, holds are longer horizon). Add model projection as supplementary column.

### Demand Surge

Keep existing net-flow ranking. Add model projection as supplementary column to validate whether surges translate to actual projected returns.

### Best Grading Play

Keep existing EV arithmetic (it's deterministic math, not prediction). No model changes.

### Report Card Section (new, on About page or dedicated page)

- "Over the last 12 months, cards projected at +15% or higher actually returned X% on average"
- Decile chart: 10 bars showing predicted vs actual returns per decile
- Feature importance bar chart: which signals matter most
- Model version + last retrained date
- Updated weekly after retraining

---

## Pipeline Integration

### Modified `daily_pipeline.py`

Add new stage after existing compute:
```
Stage: predict
  - Load model
  - Generate feature vectors for all active cards
  - Run inference (median + quantile models)
  - Store projections in model_projections table
```

### Modified `weekly_pipeline.py`

Add new stage before predict:
```
Stage: train
  - Assemble historical training dataset
  - Train 3 models (median, q25, q75)
  - Run walk-forward backtest
  - Store report card metrics
  - Save model artifacts
```

---

## Dependencies

Add to `requirements.txt`:
```
lightgbm>=4.0.0
shap>=0.43.0
scikit-learn>=1.3.0
```

---

## Success Criteria

1. Out-of-sample R-squared > 0.05 (respectable for asset returns)
2. Top-decile vs bottom-decile spread > 10% (model discriminates winners from losers)
3. Hit rate > 55% for "projected positive" cards (better than coin flip)
4. Feature importance reveals non-obvious signals (model isn't just restating price momentum)
5. Stable across price tiers and cultural tiers (not concentrated in one segment)
