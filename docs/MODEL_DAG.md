# Delta Prediction Model — Feature DAG

Written 2026-04-15 as part of the Analytics Taste v4 audit.

**Why this doc exists:** [McElreath v4](https://speakerdeck.com/rmcelreath/statistical-rethinking-2023-lecture-05) — "causal salad is a sin." Conditioning on descendants or colliders biases inference even when predictive accuracy looks fine. Before adding any feature, classify it against this DAG.

## Target

**Y = net-of-cost 180-day return on PSA 10 sale**, computed as:

```
target = (psa_10_price_t+180 × (1 - 0.13 FVF) - $5 shipping) / raw_price_t - 1
```

i.e. the realistic outcome an investor sees after listing, shipping, and eBay fees.

**T = anchor date (today)**

Everything in the feature vector must be known at time T. Anything computed from prices ≥ T+1 is forbidden and would be a leak.

## Feature classification

For each feature, one of four roles wrt Y:

- **Parent** — directly influences Y, valid predictor
- **Descendant** — caused by Y (or by same upstream as Y), conditioning on it is the collider / post-treatment trap
- **Confounder** — influences both a parent and Y, valid to include
- **Proxy** — stands in for an unobserved parent, valid but flag-worthy

| Feature | Role | Rationale | Watch |
|---|---|---|---|
| `log_price` | Proxy | Current price as proxy for card desirability / grade hierarchy. Feeds Y as part of the return calculation, but the snapshot-at-T is fine. | Ratio features derived from `log_price` need separate review. |
| `ret_30d`, `ret_90d`, `ret_365d` | Parent (momentum) | Prior returns are causally prior to future returns in a momentum/mean-reversion market. Valid. | These are descendants of PRICE_PATH, not Y. OK to condition on. |
| `peak_discount` | Parent | Current price vs. all-time peak — a valuation signal. | Fine. |
| `trough_recovery` | Parent | Current price vs. all-time trough. | Fine. |
| `volatility` | Parent | Historical vol is the risk half of the return/risk trade-off. | Fine. |
| `ma_distance` | Parent | Distance from moving average — mean-reversion signal. | Fine. |
| `net_flow_pct_7d`, `net_flow_pct_30d` | Parent | Net listing flow in the last N days — demand/supply snapshot at T. | Fine. |
| `demand_pressure_7d`, `demand_pressure_30d` | Parent | Observed sales volume. | Fine. |
| `supply_saturation_index` | Parent | Open listings relative to recent sales. | Fine. |
| `ds_ratio` | Parent | Demand / supply ratio. | Fine. |
| `gem_pct` | Confounder | PSA 10 gem rate influences both current PSA 10 price (Y-parent via raw_price and grading supply) AND future PSA 10 price. | Including is correct. |
| `psa_10_pop` | Confounder | Graded population — supply-side. | Fine. |
| ~~`psa_10_vs_raw_pct`~~ | 🗑️ **PERMANENTLY REMOVED (2026-04-16)** | Ratio of two loosely-coupled markets (PSA 10 = investor/collector pool, raw = player pool) with hidden condition variance on raw side + temporal misalignment (today's PSA 10 vs. weeks-old raw) + missing-coded-as-zero bias. Ablation showed +18% relative Sharpe on top-2% from removal. Signal it tried to capture is already cleanly present via `gem_pct` + `psa_10_pop` + `log_price`. | **Do not reintroduce** unless a cleaner formulation with temporal alignment + missing-as-missing is proposed AND re-ablated. |
| `cultural_score`, `cultural_tier`, `pokemon_peak_log`, `pokemon_peak_ratio` | Parent | Franchise-level demand ceiling. Valid. | Fine. |
| `rarity_tier` | Parent | Encoded rarity. Valid categorical. | Flag: should be native LightGBM categorical, not integer tier (see v4 gap 6). |
| `history_days` | Proxy (data quality) | Length of price history as proxy for listing maturity. Valid but weak. | Fine. |
| ~~LIQUIDITY columns~~ | 🗑️ **REMOVED (2026-04-16) — dead features** | All 5 columns (`sales_per_day_30d`, `sell_through_30d`, `ask_bid_proxy_30d`, `new_listings_per_day_30d`, `thin_market_flag`) have **zero variance** across the 110,895-sample training dataset. Unique-value count = 1 for every column. The underlying `ebay_history` table isn't populated at feature-compute time, so `compute_liquidity_at_date()` returns defaults for every row. LightGBM can't learn from a constant; they were wasted model capacity + silent-failure "liquidity-aware" logic. | **Do not reintroduce until `ebay_history` pipeline is fixed.** Re-verify variance after re-ingest. |
| REPRINT columns | Parent | Heuristic reprint-risk scoring. | Flag: heuristic, not model-derived. Upgrade to set-release-calendar signal next session. |

## Known collider / descendant risks

1. ~~**`psa_10_vs_raw_pct`**~~ — **RESOLVED 2026-04-16: permanently removed.** Ablation (`scripts/ablate_collider.py --drop psa_10_vs_raw_pct --n-ensemble 5`) showed: Spearman dropped 0.009 (negligible), but top-2% Sharpe jumped +0.57 (3.13 → 3.70) and top-2% net return +0.35%. Classic collider signature — helps the middle of the ranking, hurts the conviction tip where actual trades live. Beyond the collider issue, the feature was methodologically broken (ratio of two loosely-coupled markets, hidden condition variance on raw side, temporal misalignment, missing-coded-as-zero). Replaced cleanly by `gem_pct` + `psa_10_pop` + `log_price`.

2. **LIQUIDITY columns (5 features)** — **REMOVED 2026-04-16: dead features.** Variance check across 110,895 training samples showed every liquidity feature has exactly 1 unique value (sales_per_day_30d=0, sell_through_30d=0, ask_bid_proxy_30d=10, new_listings_per_day_30d=0, thin_market_flag=1). The `compute_liquidity_at_date()` function in features.py returns defaults because `ebay_history` rows aren't present for most cards at feature-compute time. **Data pipeline bug, not a modeling bug** — the features are engineered correctly but the source data never flowed. Fix path: audit `pipeline/collectors/ebay.py` + reconcile `ebay_history` table population against the set of cards that appear in `price_history`. Until then, features stay dropped.

   **UPDATE 2026-04-16 (late):** eBay Production API keys now wired (pipeline/collectors/ebay.py with OAuth2). `scripts/populate_ebay_liquid.py` successfully fetched 1,000 liquid-universe cards with 0 errors, populating real liquidity data TODAY. However: historical training samples still see empty ebay_history (data source only populates forward). Liquidity features will become trainable once we have ~180+ days of accumulated history. Until then, they remain OUT of FEATURE_COLUMNS but the collector runs daily via `scripts/refresh_new_signals.sh`.

3. **TOURNAMENT columns (4 features)** — **HELD BACK 2026-04-16: temporal mismatch.** Collected and flowing at live inference (UI surfaces tournament play signal on card detail pages for investors), but OUT of training `FEATURE_COLUMNS`. Reason: tournament data (from limitlesstcg.com API) covers last 180 days only, training anchor range ends 2025-10-01 (needs 180d forward buffer), so **zero overlap** — training samples all see `tournament_apps_90d = 0`. Variance check at training confirmed 0.00% nonzero rows. Extension of P19: **variance audit must apply to training dataset, not just live features**. Re-add to FEATURE_COLUMNS once tournament data accumulates to span the trainable anchor range (~6+ months of forward collection).

2. **Return features (`ret_30d/90d/365d`)** — NOT descendants of Y (they're from price path before T), but descendants of earlier PRICE. Since PRICE is not the target (net-of-cost RETURN is the target), these are valid parents — though in a naive formulation you could accidentally create a post-treatment problem if the return definition overlapped the feature window. Delta's window is clean: features at T use price history ≤ T, target uses future window T+1 to T+180.

3. **No feature in the current set is computed from data ≥ T+1.** ✅ Forbidden conditioning on post-treatment is avoided.

## Missing features to add (Signal Classes 3+4)

All of these are **valid parents** of Y — they influence demand without being caused by Y:

- Reddit r/pkmntcg post volume (rolling z-score)
- Google Trends for card/pokemon name
- Tournament top-8 decklist presence
- Anime episode airings (dummy + windowed decay)
- New-set release calendar (reprint pressure)

Not included; known structural gap per earlier audit. 4-6 weeks to wire up.

## Review policy

Any new feature added to `FEATURE_COLUMNS` must:
1. Be listed in this doc with a classified role.
2. Pass an ablation test: including it should not flip the sign of other parent coefficients.
3. Be computable from data strictly before time T.

If any of those fail, the feature does not ship.

## See also

- `pipeline/model/features.py` — feature computation
- `pipeline/model/features_v2.py` — current iteration
- `pipeline/model/train.py` — training path (now logs coverage_50, coverage_80, MASE, rolling-origin CV)
- `scripts/posterior_predictive_check.py` — model-vs-reality calibration check
- Taste Layer — Analytics Taste v4 (McElreath, FPP3, Vehtari)
