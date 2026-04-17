---
title: Delta Dex
emoji: 📊
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
license: mit
---

# Delta Dex

Data-driven Pokemon TCG investment analytics. A snapshot deployment of the
full pipeline — model projections, Must Buy picks, Budget Fit ranking, set
explorer — backed by 800K+ rows of PSA 10 price history, PSA pop, and
eBay listings.

## What's inside

- **v1_3 model** — gradient-boosted regressor with 31 features, promoted via
  a top-2% conviction gate (82% hit-rate on top picks, +17% net 180d,
  Sharpe 4.17 on walkforward).
- **Report Card** — R² 0.20, Spearman 0.53, 5-of-6 green metrics.
- **1,268 English Black Star promos** (basep through svp), 50 Korean-exclusive
  promos, 57 Japanese promos, all with PC price history where available.
- **/model/picks endpoint** — queryable top-2% / top-1% conviction pick list.

## Scoring models

- **Pure ROI** — model projection × confidence. The raw "what the model
  thinks" score.
- **Must Buy v3.2** — additive composite: 35 pts model + 15 cultural +
  15 demand + 15 scarcity + 10 momentum + 10 grading + 0-10 setup-pattern
  kicker. All signals bounded, no multiplicative cascades.
- **Budget Fit v3.2** — Must Buy's components + 15-pt budget-sizing
  dimension. "Best model picks under your budget cap."

## This is a snapshot

The live pipeline (eBay OAuth, PriceCharting scrapes, PSA pop) runs
separately. This HF Space deploys a frozen DB state — great for showing
people the platform, but projections don't refresh until redeploy.
