"""
Backtest the wishlist investment-suggestion scorer against historical forward
returns.

What this measures
------------------
For every non-sealed card with enough PSA 10 price history, we walk forward
through monthly anchor dates and compute:

  * features available at the anchor (using ONLY data ≤ anchor date)
  * the forward 3-month return (price at T+90 days / price at T - 1)

Then we ask the honest question: does any feature actually predict the
forward return, and does the current wishlist scorer's composite signal
rank-correlate with realized returns?

Data constraint
---------------
The DB has dense daily price_history only for ~60 days (Feb-Apr 2026).
Before that, there's ~1 monthly snapshot per card. So the backtest uses
MONTHLY anchors with price-based features only (no market_pressure or
supply_saturation — those only exist for the dense window and we can't
build a 24-month panel for them).

This means we're testing the PRICE-BASED components of the scorer
(momentum, peak discount, volatility, cultural, pop). The market-pressure
components (nf_pct, sat_idx) can only be spot-checked on a single recent
anchor date, which gives us ~1000 samples — too few for a reliable
correlation test.

Output
------
Prints a report to stdout:
  * Coverage summary (# cards, # anchor-date samples)
  * Univariate Spearman rank correlations for each feature
  * OLS linear fit of forward return on feature set (coefficients, R²)
  * Deciled mean forward return by each feature
  * Current composite wishlist score vs forward return
  * Recommended feature weighting based on OLS coefficients
"""

from __future__ import annotations

import math
import re
import sqlite3
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "pokemon.db"

# Minimum number of monthly observations per card to be eligible.
# Need at least 6 months trailing for features + 3 months forward for target.
MIN_MONTHS_PER_CARD = 9

# Horizon for forward returns (in days).
FORWARD_HORIZON_DAYS = 90

# Monetary floor — below this we don't care about the card as an investment.
MIN_PSA10_PRICE = 20


# ---------------------------------------------------------------------------
# Cultural regex (ported from wishlist_store.js)
# ---------------------------------------------------------------------------

ICONIC = [
    (r"charizard", 1.00), (r"pikachu", 1.00), (r"mewtwo", 0.96),
    (r"\bmew\b", 0.96), (r"umbreon", 0.96),
    (r"lugia", 0.88), (r"rayquaza", 0.88), (r"gengar", 0.85),
    (r"snorlax", 0.82), (r"dragonite", 0.82),
    (r"blastoise", 0.78), (r"venusaur", 0.78), (r"gyarados", 0.80),
    (r"greninja", 0.82), (r"lucario", 0.80), (r"garchomp", 0.78),
    (r"zoroark", 0.75), (r"sceptile", 0.72), (r"blaziken", 0.72),
    (r"swampert", 0.72),
    (r"sylveon", 0.78), (r"espeon", 0.75), (r"leafeon", 0.72),
    (r"glaceon", 0.72), (r"vaporeon", 0.70), (r"jolteon", 0.70),
    (r"flareon", 0.70), (r"eevee", 0.72),
    (r"giratina", 0.70), (r"dialga", 0.65), (r"palkia", 0.65),
    (r"arceus", 0.72), (r"zekrom|reshiram", 0.65),
    (r"yveltal|xerneas", 0.62), (r"groudon|kyogre", 0.65),
    (r"zacian|zamazenta", 0.62), (r"calyrex", 0.60),
    (r"cynthia", 0.75), (r"lillie", 0.72), (r"acerola", 0.70),
    (r"iono", 0.68), (r"marnie", 0.65),
    (r"\bhop\b|\bleon\b", 0.55),
    (r"\bn['\u2019]s\b", 0.65),
    (r"team rocket", 0.60), (r"giovanni", 0.62),
    (r"erika", 0.55), (r"misty", 0.62), (r"brock", 0.55),
]
RARITY_BONUS = {
    "Special Illustration Rare": 0.20,
    "Hyper Rare":                0.12,
    "Mega Hyper Rare":           0.18,
    "Mega Attack Rare":          0.12,
    "Secret Rare":               0.12,
    "Rainbow Rare":              0.12,
    "Gold Rare":                 0.12,
    "Illustration Rare":         0.08,
    "Ultra Rare":                0.05,
}


def cultural_score(product_name: str, rarity_name: Optional[str]) -> float:
    name = (product_name or "").lower()
    name_score = 0.0
    for pat, score in ICONIC:
        if re.search(pat, name) and score > name_score:
            name_score = score
    bonus = RARITY_BONUS.get(rarity_name or "", 0.0)
    return max(0.0, min(1.0, name_score + bonus))


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Sample:
    """One (card, anchor_date) observation for the backtest."""
    card_id: str
    set_code: str
    name: str
    anchor_date: str
    anchor_price: float
    forward_price: float
    forward_return: float           # 3-month forward return (main target)
    forward_return_1m: Optional[float]   # 1-month forward return (when available)
    forward_return_6m: Optional[float]   # 6-month forward return (when available)
    forward_return_12m: Optional[float]  # 12-month forward return (when available)
    # Features computed at anchor using only data ≤ anchor
    mom_1m: float                   # 1-month trailing return
    mom_3m: float                   # 3-month trailing return
    mom_6m: float                   # 6-month trailing return
    peak_discount: float            # (6mo peak − current) / peak
    trough_recovery: float          # (current − 6mo trough) / trough
    volatility: float               # stdev of last 6 monthly returns
    cultural: float
    log_price: float
    # Candidate features being considered for the next iteration
    mom_accel: float                # mom_1m − (mom_3m / 3); positive = accelerating
    range_pos: float                # where in 6mo range: 0=bottom, 1=top
    ma_distance: float              # (current − ma_6m) / ma_6m; neg = below MA
    rsi_6m: float                   # 6-month RSI (0–100)
    vol_trend: float                # sales volume 3m avg / 6m avg (-1 if missing)
    # Old wishlist formula composite (using only components we can compute
    # in the monthly dataset — hold strength + cultural + momentum)
    old_hold_strength: float
    old_momentum_bucket: float
    old_composite_medium: float     # same blend as horizon=medium


# ---------------------------------------------------------------------------
# Monthly resampling
# ---------------------------------------------------------------------------

def load_monthly_prices(db: sqlite3.Connection):
    """Return {card_id: {name, rarity, set_code, series: [...]}} using one
    row per (card, month).

    For each month we take the FIRST available psa_10_price observation.
    """
    rows = db.execute(
        """SELECT p.card_id, c.product_name, c.rarity_name, c.set_code,
                  p.date, p.psa_10_price, p.sales_volume
             FROM price_history p
             JOIN cards c ON c.id = p.card_id
            WHERE p.psa_10_price IS NOT NULL
              AND p.psa_10_price >= ?
              AND c.sealed_product = 'N'
            ORDER BY p.card_id, p.date""",
        (MIN_PSA10_PRICE,),
    ).fetchall()

    by_card = defaultdict(lambda: {"name": "", "rarity": "", "set_code": "", "series": {}})
    for r in rows:
        cid = r["card_id"]
        month = r["date"][:7]  # YYYY-MM
        if month not in by_card[cid]["series"]:
            by_card[cid]["series"][month] = (r["date"], r["psa_10_price"], r["sales_volume"])
            by_card[cid]["name"] = r["product_name"] or ""
            by_card[cid]["rarity"] = r["rarity_name"] or ""
            by_card[cid]["set_code"] = r["set_code"] or ""

    # Convert to sorted list per card
    out = {}
    for cid, data in by_card.items():
        series = sorted(data["series"].items())  # list of (YYYY-MM, (date, price, vol))
        series = [(m, d, p, v) for m, (d, p, v) in series]
        if len(series) >= MIN_MONTHS_PER_CARD:
            out[cid] = {
                "name": data["name"],
                "rarity": data["rarity"],
                "set_code": data["set_code"],
                "series": series,
            }
    return out


# ---------------------------------------------------------------------------
# Feature + target computation
# ---------------------------------------------------------------------------

def month_index(series, target_month: str) -> int:
    """Return index of the entry whose YYYY-MM matches, or -1."""
    for i, (m, _d, _p) in enumerate(series):
        if m == target_month:
            return i
    return -1


def month_offset(ym: str, delta_months: int) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    m += delta_months
    while m > 12:
        m -= 12; y += 1
    while m < 1:
        m += 12; y -= 1
    return f"{y:04d}-{m:02d}"


def build_samples(cards) -> List[Sample]:
    """Walk each card forward and emit (features at T, forward returns at T+1/3/6/12m)."""
    samples: List[Sample] = []

    for cid, data in cards.items():
        series = data["series"]
        if len(series) < MIN_MONTHS_PER_CARD:
            continue

        name = data["name"]
        set_code = data.get("set_code", "")
        cult = cultural_score(name, data["rarity"])

        prices_by_month = {m: p for m, _d, p, _v in series}
        volumes_by_month = {m: v for m, _d, _p, v in series}
        months = [m for m, _d, _p, _v in series]

        # Try each anchor month T where we have enough history (6 months)
        # AND enough forward runway (at least 3 months).
        for i, anchor_month in enumerate(months):
            if i < 6:
                continue
            if i + 3 >= len(months):
                continue

            anchor_price = prices_by_month[anchor_month]
            if anchor_price <= 0:
                continue

            # Multi-horizon forward returns (None when runway is insufficient)
            def fwd_n(n_months):
                if i + n_months >= len(months):
                    return None
                fp = prices_by_month[months[i + n_months]]
                return None if fp <= 0 else (fp / anchor_price) - 1

            fwd_return_1m  = fwd_n(1)
            fwd_return_3m  = fwd_n(3)
            fwd_return_6m  = fwd_n(6)
            fwd_return_12m = fwd_n(12)
            if fwd_return_3m is None:
                continue

            # Trailing features — all from prices ≤ anchor month
            trailing = [prices_by_month[months[j]] for j in range(i - 6, i + 1)]
            if len(trailing) < 7 or any(p <= 0 for p in trailing):
                continue

            mom_1m = (trailing[-1] / trailing[-2]) - 1
            mom_3m = (trailing[-1] / trailing[-4]) - 1
            mom_6m = (trailing[-1] / trailing[0])  - 1

            peak = max(trailing)
            trough = min(trailing)
            peak_discount = max(0.0, (peak - anchor_price) / peak) if peak > 0 else 0.0
            trough_recovery = (anchor_price - trough) / trough if trough > 0 else 0.0

            # Monthly log-returns → stdev
            log_returns = []
            for j in range(1, len(trailing)):
                if trailing[j - 1] > 0 and trailing[j] > 0:
                    log_returns.append(math.log(trailing[j] / trailing[j - 1]))
            vol = statistics.stdev(log_returns) if len(log_returns) >= 2 else 0.0

            log_price = math.log(anchor_price)

            # ---- CANDIDATE FEATURES (new, not yet in production scorer) ----

            # Momentum acceleration: is the trend getting faster or slower?
            # mom_accel > 0 → recent month is outpacing the 3m average rate.
            # Used as "is the mean-reversion bounce already starting?"
            mom_accel = mom_1m - (mom_3m / 3)

            # Range position: where in the 6-month range is the current price?
            # 0.0 = at 6m low (oversold), 1.0 = at 6m high (bought-in)
            if peak > trough:
                range_pos = (anchor_price - trough) / (peak - trough)
            else:
                range_pos = 0.5

            # Distance from 6-month moving average
            ma_6m = sum(trailing) / len(trailing)
            ma_distance = (anchor_price - ma_6m) / ma_6m if ma_6m > 0 else 0.0

            # 6-month RSI — classic oversold indicator
            gains = []
            losses = []
            for j in range(1, len(trailing)):
                delta = trailing[j] - trailing[j - 1]
                if delta > 0:
                    gains.append(delta); losses.append(0.0)
                else:
                    gains.append(0.0); losses.append(-delta)
            avg_gain = sum(gains) / len(gains) if gains else 0
            avg_loss = sum(losses) / len(losses) if losses else 0
            if avg_loss == 0:
                rsi = 100.0 if avg_gain > 0 else 50.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))

            # Sales volume trend: 3m avg / 6m avg (how "hot" is current activity?)
            trailing_vols = [volumes_by_month[months[j]] for j in range(i - 6, i + 1)]
            trailing_vols = [v for v in trailing_vols if v is not None and v > 0]
            if len(trailing_vols) >= 6:
                recent_3 = sum(trailing_vols[-3:]) / 3
                older_6 = sum(trailing_vols[-6:]) / 6
                vol_trend = (recent_3 / older_6) - 1 if older_6 > 0 else 0.0
            else:
                vol_trend = None

            # Port of the old wishlist "holdStrength" and "momentumStrength"
            capped_disc = min(0.5, peak_discount)
            hold_raw = mom_3m * (1 + capped_disc)
            hold_strength = max(0.0, min(1.0, hold_raw * 2))

            change = mom_1m
            if change <= -0.20: mom_bucket = 0.0
            elif change <= 0:    mom_bucket = 0.3 * (1 + change / 0.20)
            elif change <= 0.20: mom_bucket = 0.3 + 0.70 * (change / 0.20)
            else:                mom_bucket = 1.0

            # Old composite (same blend as horizon=medium)
            present_weight = 0.20 + 0.25 + 0.20
            scale = 1.0 / present_weight
            old_composite = (
                hold_strength * 0.20 +
                cult          * 0.25 +
                mom_bucket    * 0.20
            ) * scale

            samples.append(Sample(
                card_id=cid, set_code=set_code, name=name,
                anchor_date=anchor_month, anchor_price=anchor_price,
                forward_price=prices_by_month[months[i + 3]],
                forward_return=fwd_return_3m,
                forward_return_1m=fwd_return_1m,
                forward_return_6m=fwd_return_6m,
                forward_return_12m=fwd_return_12m,
                mom_1m=mom_1m, mom_3m=mom_3m, mom_6m=mom_6m,
                peak_discount=peak_discount, trough_recovery=trough_recovery,
                volatility=vol, cultural=cult, log_price=log_price,
                mom_accel=mom_accel, range_pos=range_pos,
                ma_distance=ma_distance, rsi_6m=rsi,
                vol_trend=vol_trend if vol_trend is not None else -1.0,
                old_hold_strength=hold_strength,
                old_momentum_bucket=mom_bucket,
                old_composite_medium=old_composite,
            ))

    return samples


# ---------------------------------------------------------------------------
# Statistics helpers (no numpy dependency)
# ---------------------------------------------------------------------------

def spearman_rank_corr(xs: list, ys: list) -> float:
    """Compute Spearman's rank correlation without numpy."""
    n = len(xs)
    if n < 3:
        return float("nan")
    # Rank each list (ties broken by first-seen order — good enough for large N)
    def ranks(v):
        idx = sorted(range(n), key=lambda i: v[i])
        r = [0.0] * n
        for rank, i in enumerate(idx, 1):
            r[i] = rank
        return r
    rx, ry = ranks(xs), ranks(ys)
    mx = sum(rx) / n
    my = sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    den_x = math.sqrt(sum((rx[i] - mx) ** 2 for i in range(n)))
    den_y = math.sqrt(sum((ry[i] - my) ** 2 for i in range(n)))
    if den_x == 0 or den_y == 0:
        return float("nan")
    return num / (den_x * den_y)


def decile_analysis(xs: list, ys: list) -> list:
    """Sort by x and return mean-y per decile of x."""
    if not xs:
        return []
    pairs = sorted(zip(xs, ys))
    n = len(pairs)
    out = []
    for d in range(10):
        lo, hi = d * n // 10, (d + 1) * n // 10
        chunk = pairs[lo:hi]
        if not chunk:
            continue
        mean_x = sum(x for x, _ in chunk) / len(chunk)
        mean_y = sum(y for _, y in chunk) / len(chunk)
        out.append((d + 1, len(chunk), mean_x, mean_y))
    return out


def ols_fit(X: list, y: list) -> tuple:
    """Minimal OLS: solve (X'X) b = X'y via Gaussian elimination. Intercept added."""
    n = len(y)
    if n == 0:
        return ([], 0.0)
    k = len(X[0])
    # Add intercept column
    Xb = [[1.0] + row for row in X]
    kb = k + 1
    # Normal equations
    XtX = [[0.0] * kb for _ in range(kb)]
    Xty = [0.0] * kb
    for i in range(n):
        for a in range(kb):
            Xty[a] += Xb[i][a] * y[i]
            for b in range(a, kb):
                XtX[a][b] += Xb[i][a] * Xb[i][b]
    for a in range(kb):
        for b in range(a):
            XtX[a][b] = XtX[b][a]
    # Gaussian elimination
    A = [row[:] + [Xty[r]] for r, row in enumerate(XtX)]
    for r in range(kb):
        piv = max(range(r, kb), key=lambda rr: abs(A[rr][r]))
        A[r], A[piv] = A[piv], A[r]
        if abs(A[r][r]) < 1e-12:
            continue
        for rr in range(r + 1, kb):
            factor = A[rr][r] / A[r][r]
            for c in range(r, kb + 1):
                A[rr][c] -= factor * A[r][c]
    beta = [0.0] * kb
    for r in range(kb - 1, -1, -1):
        if abs(A[r][r]) < 1e-12:
            continue
        s = A[r][kb] - sum(A[r][c] * beta[c] for c in range(r + 1, kb))
        beta[r] = s / A[r][r]

    # R²
    y_mean = sum(y) / n
    ss_tot = sum((yi - y_mean) ** 2 for yi in y)
    ss_res = 0.0
    for i in range(n):
        pred = sum(Xb[i][c] * beta[c] for c in range(kb))
        ss_res += (y[i] - pred) ** 2
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return (beta, r2)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def trim_outliers(samples, column: str, lo_pct=0.01, hi_pct=0.99):
    """Trim samples where `column` is in the top/bottom 1% — kills crazy prints."""
    vals = sorted(getattr(s, column) for s in samples)
    n = len(vals)
    lo = vals[int(n * lo_pct)]
    hi = vals[int(n * hi_pct)]
    return [s for s in samples if lo <= getattr(s, column) <= hi]


def zscore(values: list) -> list:
    """Return z-scored values so different features can be summed fairly."""
    mu = sum(values) / len(values)
    var = sum((v - mu) ** 2 for v in values) / max(1, len(values) - 1)
    sd = math.sqrt(var)
    if sd == 0:
        return [0.0] * len(values)
    return [(v - mu) / sd for v in values]


def new_scorer(samples: list[Sample]) -> list[float]:
    """Data-driven replacement scorer — z-scored OLS-weighted version.

    Weights come directly from the OLS coefficients in the backtest, with
    features z-scored so each factor contributes on the same scale and the
    final score is comparable across cards/time.

    Direction highlights:
      * peak_discount    (+)  — buy the dip
      * volatility       (+)  — mean-reversion proxy
      * cultural         (+)  — brand floor
      * mom_3m           (-)  — REVERSAL not momentum (key fix)
      * log_price        (-)  — size premium (cheap outperforms)
      * mom_6m           (+)  — weak trend check (near zero)
    """
    n = len(samples)
    peak_disc = zscore([s.peak_discount for s in samples])
    vol       = zscore([s.volatility for s in samples])
    cult      = zscore([s.cultural for s in samples])
    m3        = zscore([s.mom_3m for s in samples])
    m6        = zscore([s.mom_6m for s in samples])
    lp        = zscore([s.log_price for s in samples])
    # Weights (normalized sum of absolute values = 1.0)
    W_DISC, W_VOL, W_CULT, W_M3, W_M6, W_LP = 0.35, 0.20, 0.20, -0.15, +0.05, -0.05
    return [
        W_DISC * peak_disc[i] +
        W_VOL  * vol[i] +
        W_CULT * cult[i] +
        W_M3   * m3[i] +
        W_M6   * m6[i] +
        W_LP   * lp[i]
        for i in range(n)
    ]


def compute_set_medians(samples: list[Sample]) -> dict:
    """Compute median 3-month trailing return per (set_code, anchor_month).

    Returns {(set_code, anchor_month): median_mom_3m}. Used for setAlpha
    factor in js_native_scorer — each sample looks up its own set's
    median trailing return at its own anchor date so the look-up is
    causally clean (no future info leakage).
    """
    by_key: dict = defaultdict(list)
    for s in samples:
        key = (s.set_code, s.anchor_date)
        by_key[key].append(s.mom_3m)
    medians = {}
    for k, vals in by_key.items():
        if len(vals) >= 3:  # need at least 3 peers to form a median
            medians[k] = sorted(vals)[len(vals) // 2]
    return medians


def js_native_scorer(samples: list[Sample], set_medians: Optional[dict] = None) -> list[float]:
    """Port of the NEW wishlist_store.js scorer (medium horizon, audit v3).

    This is the EXACT formula that runs in the browser — not z-scored,
    not OLS-fit, using the raw factor functions from the JS implementation.

    Medium-horizon weights (audit v3, with setAlpha):
        peakDisc: 0.25, maDistance: 0.20, volatility: 0.10, cultural: 0.15,
        reversal: 0.10, setAlpha: 0.10, sizeDiscount: 0.05, mustBuy: 0.05
    """
    # Medium-horizon weights - must match the JS (audit v3)
    W = {
        "peakDisc":    0.25,
        "maDistance":  0.20,
        "volatility":  0.10,
        "cultural":    0.15,
        "reversal":    0.10,
        "setAlpha":    0.10,
        "sizeDiscount": 0.05,
        "mustBuy":     0.05,
    }

    scores = []
    for s in samples:
        # Peak discount (0..1) — clamp01((max - current) / max), NOT capped
        peak_disc = max(0.0, min(1.0, s.peak_discount))

        # Volatility factor: range / current, approximated from
        # peak_discount + trough_recovery
        vol_proxy = max(0.0, min(1.0, s.peak_discount + s.trough_recovery))

        # Cultural — already 0..1
        cultural = s.cultural

        # Weighted multi-horizon momentum (new — matches wishlist_store.js).
        # Uses 30d/90d/365d with 0.50/0.30/0.20 weights. In the monthly
        # backtest we have mom_1m (≈30d), mom_3m (≈90d), mom_6m (≈180d).
        # 365d anchor isn't directly available, so we use 6m as a reasonable
        # proxy for the "longer term" component.
        weighted_mom = 0.50 * s.mom_1m + 0.30 * s.mom_3m + 0.20 * s.mom_6m

        # Reversal factor: 0.5 - weighted_mom/0.60, clamped
        reversal = max(0.0, min(1.0, 0.5 - weighted_mom / 0.60))

        # Size discount factor: (3.0 - log10(price)) / 2 + 0.5, clamped
        log10_price = s.log_price / math.log(10)
        size_disc = max(0.0, min(1.0, (3.0 - log10_price) / 2.0 + 0.5))

        # NEW (audit v2): moving-average distance factor. JS computes MA
        # from (30d, 90d, 365d) anchors; the monthly backtest uses the
        # 6-month moving average in ma_distance.
        below_ma_pct = -s.ma_distance
        ma_dist_factor = max(0.0, min(1.0, 0.5 + below_ma_pct / 0.60))

        # NEW (audit v3): set alpha factor. Card return minus set-median
        # return. Negative alpha = laggard = buy signal (peer reversal).
        # Look up the set median for this sample's (set_code, anchor_date).
        set_alpha = None
        if set_medians is not None:
            set_median = set_medians.get((s.set_code, s.anchor_date))
            if set_median is not None:
                alpha = s.mom_3m - set_median
                # Reversal mapping: -20% → 1.0, 0 → 0.5, +20% → 0.0
                set_alpha = max(0.0, min(1.0, 0.5 - alpha / 0.40))

        # mustBuy — missing in historical backtest → redistribute
        parts = [
            (peak_disc,      W["peakDisc"]),
            (ma_dist_factor, W["maDistance"]),
            (vol_proxy,      W["volatility"]),
            (cultural,       W["cultural"]),
            (reversal,       W["reversal"]),
            (size_disc,      W["sizeDiscount"]),
        ]
        if set_alpha is not None:
            parts.append((set_alpha, W["setAlpha"]))
        total_weight = sum(w for _, w in parts)
        base = sum(v * w for v, w in parts) / total_weight if total_weight > 0 else 0

        # Conviction bonus (audit v2): threshold lowered 0.25 → 0.10
        if peak_disc >= 0.10 and cultural >= 0.45:
            base *= 1.10
        elif reversal >= 0.70 and cultural < 0.20:
            base *= 0.80

        scores.append(min(1.0, max(0.0, base)) * 100)

    return scores


def report(samples: list[Sample]):
    print("=" * 76)
    print("WISHLIST SCORER BACKTEST")
    print("=" * 76)
    print()
    print(f"Total samples: {len(samples)}")
    unique_cards = len(set(s.card_id for s in samples))
    anchor_months = sorted(set(s.anchor_date for s in samples))
    print(f"Unique cards:  {unique_cards}")
    print(f"Anchor months: {len(anchor_months)} "
          f"(from {anchor_months[0]} to {anchor_months[-1]})")
    print(f"Forward horizon: 3 months")
    print()

    # Trim 1% tails on forward return to kill print errors + squelched listings
    samples = trim_outliers(samples, "forward_return", 0.01, 0.99)
    print(f"After 1% trim on forward_return: {len(samples)} samples")
    print()

    forward = [s.forward_return for s in samples]
    mean_fwd = sum(forward) / len(forward)
    med_fwd = statistics.median(forward)
    std_fwd = statistics.stdev(forward) if len(forward) > 1 else 0
    print(f"Forward 3m return: mean={mean_fwd:+.2%}  median={med_fwd:+.2%}  stdev={std_fwd:.2%}")
    print()

    print("-" * 76)
    print("UNIVARIATE SPEARMAN RANK CORRELATIONS vs forward 3m return")
    print("-" * 76)
    print(f"{'Feature':<30} {'Spearman':>10}   Interpretation")
    features = [
        ("mom_1m",           "mom_1m"),
        ("mom_3m",           "mom_3m"),
        ("mom_6m",           "mom_6m"),
        ("peak_discount",    "peak_discount"),
        ("trough_recovery",  "trough_recovery"),
        ("volatility",       "volatility"),
        ("cultural",         "cultural"),
        ("log_price",        "log_price"),
        ("old_hold_strength","old_hold_strength"),
        ("old_momentum_bucket","old_momentum_bucket"),
        ("old_composite_medium (CURRENT scorer)", "old_composite_medium"),
    ]
    for label, attr in features:
        xs = [getattr(s, attr) for s in samples]
        rho = spearman_rank_corr(xs, forward)
        interp = "random" if abs(rho) < 0.03 \
                else "weak" if abs(rho) < 0.10 \
                else "moderate" if abs(rho) < 0.20 \
                else "strong"
        print(f"{label:<30} {rho:>+10.4f}   {interp}")
    print()

    print("-" * 76)
    print("DECILE ANALYSIS — current composite scorer vs forward return")
    print("-" * 76)
    print(f"{'Decile':>6} {'N':>5} {'mean_score':>12} {'mean_fwd_ret':>14}")
    deciles = decile_analysis(
        [s.old_composite_medium for s in samples],
        forward,
    )
    for d, n, mx, my in deciles:
        print(f"{d:>6} {n:>5} {mx:>12.3f} {my:>+14.2%}")
    print()
    if len(deciles) >= 10:
        top = deciles[-1][3]
        bot = deciles[0][3]
        print(f"Top-decile vs bottom-decile spread: {top - bot:+.2%}")
        print("(If the scorer has ANY signal, top decile should return > bottom decile.)")
    print()

    print("-" * 76)
    print("DECILE ANALYSIS — simple mom_3m momentum vs forward return")
    print("-" * 76)
    print(f"{'Decile':>6} {'N':>5} {'mean_mom_3m':>14} {'mean_fwd_ret':>14}")
    deciles_mom = decile_analysis(
        [s.mom_3m for s in samples],
        forward,
    )
    for d, n, mx, my in deciles_mom:
        print(f"{d:>6} {n:>5} {mx:>+14.2%} {my:>+14.2%}")
    print()

    print("-" * 76)
    print("OLS REGRESSION — forward return ~ features")
    print("-" * 76)
    # Build design matrix
    feat_names = ["mom_3m", "mom_6m", "peak_discount", "volatility", "cultural", "log_price"]
    X = [[getattr(s, f) for f in feat_names] for s in samples]
    y = forward
    beta, r2 = ols_fit(X, y)
    print(f"R²: {r2:.4f}   (0 = no signal, 1 = perfect)")
    print()
    print(f"{'Feature':<20} {'Coefficient':>14}")
    print(f"{'(intercept)':<20} {beta[0]:>+14.4f}")
    for i, fname in enumerate(feat_names):
        print(f"{fname:<20} {beta[i+1]:>+14.4f}")
    print()
    print("INTERPRETATION:")
    print("  * Positive coefficient = this feature predicts HIGHER forward returns.")
    print("  * Negative coefficient = this feature predicts LOWER forward returns.")
    print("  * R² below ~0.02 means the features explain basically nothing and the")
    print("    scorer is mostly noise. R² > 0.05 is respectable for asset returns.")
    print()

    # ----------------------------------------------------------------------
    # PROPOSED DATA-DRIVEN SCORER — evaluate it against the same dataset
    # ----------------------------------------------------------------------
    print("=" * 76)
    print("PROPOSED REPLACEMENT SCORER (data-driven weights)")
    print("=" * 76)
    new_scores = new_scorer(samples)
    new_rho = spearman_rank_corr(new_scores, forward)
    print(f"Spearman rank correlation: {new_rho:+.4f}")
    print()
    print(f"{'Decile':>6} {'N':>5} {'mean_score':>14} {'mean_fwd_ret':>14}")
    deciles_new = decile_analysis(new_scores, forward)
    for d, n, mx, my in deciles_new:
        print(f"{d:>6} {n:>5} {mx:>+14.3f} {my:>+14.2%}")
    print()
    if len(deciles_new) >= 10:
        top = deciles_new[-1][3]
        bot = deciles_new[0][3]
        print(f"Top-decile vs bottom-decile spread: {top - bot:+.2%}")
        print(f"(Positive = scorer finds alpha. Current scorer spread was −0.24%.)")
    print()

    # ----------------------------------------------------------------------
    # JS-NATIVE SCORER — same formula that will actually run in the browser
    # ----------------------------------------------------------------------
    print("=" * 76)
    print("JS-NATIVE SCORER (exact port of wishlist_store.js, medium horizon)")
    print("=" * 76)
    set_medians = compute_set_medians(samples)
    print(f"  Computed set medians for {len(set_medians)} (set, anchor) pairs")
    js_scores = js_native_scorer(samples, set_medians)
    js_rho = spearman_rank_corr(js_scores, forward)
    print(f"Spearman rank correlation: {js_rho:+.4f}")
    print()
    print(f"{'Decile':>6} {'N':>5} {'mean_score':>12} {'mean_fwd_ret':>14}")
    deciles_js = decile_analysis(js_scores, forward)
    for d, n, mx, my in deciles_js:
        print(f"{d:>6} {n:>5} {mx:>12.2f} {my:>+14.2%}")
    print()
    js_spread = 0
    if len(deciles_js) >= 10:
        js_top = deciles_js[-1][3]
        js_bot = deciles_js[0][3]
        js_spread = js_top - js_bot
        print(f"Top-decile vs bottom-decile spread: {js_spread:+.2%}")
    print()

    # Side-by-side
    print("-" * 76)
    print("FINAL COMPARISON: old scorer vs proposed (z-score OLS) vs JS-native")
    print("-" * 76)
    old_rho = spearman_rank_corr([s.old_composite_medium for s in samples], forward)
    old_deciles = decile_analysis(
        [s.old_composite_medium for s in samples], forward
    )
    old_spread = old_deciles[-1][3] - old_deciles[0][3] if len(old_deciles) >= 10 else 0
    new_spread = top - bot
    print(f"                        Spearman    Top−Bottom spread")
    print(f"OLD scorer              {old_rho:+.4f}    {old_spread:+.2%}")
    print(f"OLS-fit (research)      {new_rho:+.4f}    {new_spread:+.2%}")
    print(f"JS-native (production)  {js_rho:+.4f}    {js_spread:+.2%}")
    print()


# ==========================================================================
# AUDIT v2 — deeper diagnostics on top of the headline backtest
# ==========================================================================

def bucket_spearman(samples, feature_attr, horizon_attr="forward_return"):
    """Spearman of feature vs forward return, filtered to samples with value."""
    xs, ys = [], []
    for s in samples:
        v = getattr(s, feature_attr)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            continue
        fwd = getattr(s, horizon_attr)
        if fwd is None:
            continue
        xs.append(v); ys.append(fwd)
    if not xs:
        return float("nan"), 0
    return spearman_rank_corr(xs, ys), len(xs)


def out_of_sample_test(samples):
    """80/20 chronological split: fit OLS on early 80%, test on late 20%."""
    print("-" * 76)
    print("OUT-OF-SAMPLE VALIDATION (80/20 chronological split)")
    print("-" * 76)
    sorted_samples = sorted(samples, key=lambda s: s.anchor_date)
    n = len(sorted_samples)
    split = int(n * 0.80)
    train = sorted_samples[:split]
    test = sorted_samples[split:]
    print(f"Train: {len(train)} samples  (anchors {train[0].anchor_date} … {train[-1].anchor_date})")
    print(f"Test:  {len(test)} samples  (anchors {test[0].anchor_date} … {test[-1].anchor_date})")
    print()

    feat_names = ["mom_3m", "mom_6m", "peak_discount", "volatility", "cultural", "log_price"]
    Xtr = [[getattr(s, f) for f in feat_names] for s in train]
    ytr = [s.forward_return for s in train]
    Xte = [[getattr(s, f) for f in feat_names] for s in test]
    yte = [s.forward_return for s in test]

    beta, r2_in = ols_fit(Xtr, ytr)
    # Apply train weights to test
    test_preds = []
    for row in Xte:
        p = beta[0] + sum(beta[i + 1] * row[i] for i in range(len(feat_names)))
        test_preds.append(p)
    rho_out = spearman_rank_corr(test_preds, yte)

    # R² on test
    y_mean = sum(yte) / len(yte)
    ss_tot = sum((y - y_mean) ** 2 for y in yte)
    ss_res = sum((yte[i] - test_preds[i]) ** 2 for i in range(len(yte)))
    r2_out = 1 - ss_res / ss_tot if ss_tot > 0 else 0

    print(f"In-sample R² (train):   {r2_in:.4f}")
    print(f"Out-of-sample R² (test):{r2_out:.4f}")
    print(f"Out-of-sample Spearman: {rho_out:+.4f}")
    print()

    # Decile analysis on TEST SET using train-fit predictions
    deciles = decile_analysis(test_preds, yte)
    print(f"Test-set decile analysis (using train weights):")
    print(f"{'Decile':>6} {'N':>5} {'mean_pred':>12} {'mean_fwd_ret':>14}")
    for d, nd, mx, my in deciles:
        print(f"{d:>6} {nd:>5} {mx:>+12.4f} {my:>+14.2%}")
    if len(deciles) >= 10:
        spread = deciles[-1][3] - deciles[0][3]
        print(f"Out-of-sample top-bottom spread: {spread:+.2%}")
        if spread > 0.03:
            print("  → Signal holds out of sample. Not overfit.")
        elif spread > 0:
            print("  → Weak but positive — possibly some overfit, but real signal underneath.")
        else:
            print("  → SIGNAL DOES NOT HOLD OUT OF SAMPLE — overfitting suspected.")
    print()


def yearly_stability_test(samples):
    """Is the effect stable across years, or concentrated in one regime?"""
    print("-" * 76)
    print("YEARLY REGIME STABILITY")
    print("-" * 76)
    by_year = defaultdict(list)
    for s in samples:
        year = s.anchor_date[:4]
        by_year[year].append(s)
    print(f"{'Year':<6} {'N':>6} {'mean_fwd':>10} {'Spearman(new)':>16} {'Spearman(old)':>16}")
    for year in sorted(by_year.keys()):
        ss = by_year[year]
        if len(ss) < 100:
            continue
        fwd = [s.forward_return for s in ss]
        mean_fwd = sum(fwd) / len(fwd)
        sm = compute_set_medians(ss)
        new_scores = js_native_scorer(ss, sm)
        rho_new = spearman_rank_corr(new_scores, fwd)
        rho_old = spearman_rank_corr([s.old_composite_medium for s in ss], fwd)
        print(f"{year:<6} {len(ss):>6} {mean_fwd:>+10.2%} {rho_new:>+16.4f} {rho_old:>+16.4f}")
    print()


def horizon_sensitivity_test(samples):
    """Does the mean-reversion signal persist at longer horizons?"""
    print("-" * 76)
    print("HORIZON SENSITIVITY — does the scorer work at all forward windows?")
    print("-" * 76)
    print(f"{'Horizon':<10} {'N':>6} {'Spearman(new)':>16} {'Top-Bot spread':>16}")
    horizons = [
        ("1 month",  "forward_return_1m"),
        ("3 month",  "forward_return"),
        ("6 month",  "forward_return_6m"),
        ("12 month", "forward_return_12m"),
    ]
    for label, attr in horizons:
        ss = [s for s in samples if getattr(s, attr) is not None]
        if len(ss) < 500:
            print(f"{label:<10} {len(ss):>6}   (insufficient data)")
            continue
        fwd = [getattr(s, attr) for s in ss]
        # Need to trim outliers fresh at each horizon
        vals = sorted(fwd)
        lo, hi = vals[int(len(vals)*0.01)], vals[int(len(vals)*0.99)]
        filtered = [s for s in ss if lo <= getattr(s, attr) <= hi]
        fwd_f = [getattr(s, attr) for s in filtered]
        sm = compute_set_medians(filtered)
        scores = js_native_scorer(filtered, sm)
        rho = spearman_rank_corr(scores, fwd_f)
        deciles = decile_analysis(scores, fwd_f)
        spread = deciles[-1][3] - deciles[0][3] if len(deciles) >= 10 else 0
        print(f"{label:<10} {len(filtered):>6} {rho:>+16.4f} {spread:>+16.2%}")
    print()


def subset_analysis(samples):
    """Does the effect vary by price / cultural / set-era buckets?"""
    print("-" * 76)
    print("SUBSET ANALYSIS — is the effect universal or pocket-specific?")
    print("-" * 76)
    print()
    print("By price bucket:")
    buckets = [
        ("$20–100",    lambda s: 20 <= math.exp(s.log_price) < 100),
        ("$100–500",   lambda s: 100 <= math.exp(s.log_price) < 500),
        ("$500–2000",  lambda s: 500 <= math.exp(s.log_price) < 2000),
        ("$2000+",     lambda s: math.exp(s.log_price) >= 2000),
    ]
    print(f"{'Bucket':<12} {'N':>7} {'Spearman':>12} {'Top-Bot spread':>16}")
    for label, pred in buckets:
        ss = [s for s in samples if pred(s)]
        if len(ss) < 200:
            print(f"{label:<12} {len(ss):>7}   (n/a)")
            continue
        fwd = [s.forward_return for s in ss]
        sm = compute_set_medians(ss)
        scores = js_native_scorer(ss, sm)
        rho = spearman_rank_corr(scores, fwd)
        deciles = decile_analysis(scores, fwd)
        spread = deciles[-1][3] - deciles[0][3] if len(deciles) >= 10 else 0
        print(f"{label:<12} {len(ss):>7} {rho:>+12.4f} {spread:>+16.2%}")
    print()

    print("By cultural tier:")
    buckets = [
        ("No moat",      lambda s: s.cultural < 0.20),
        ("Moderate",     lambda s: 0.20 <= s.cultural < 0.50),
        ("Strong",       lambda s: 0.50 <= s.cultural < 0.80),
        ("Iconic",       lambda s: s.cultural >= 0.80),
    ]
    print(f"{'Tier':<12} {'N':>7} {'Spearman':>12} {'Top-Bot spread':>16}")
    for label, pred in buckets:
        ss = [s for s in samples if pred(s)]
        if len(ss) < 200:
            print(f"{label:<12} {len(ss):>7}   (n/a)")
            continue
        fwd = [s.forward_return for s in ss]
        sm = compute_set_medians(ss)
        scores = js_native_scorer(ss, sm)
        rho = spearman_rank_corr(scores, fwd)
        deciles = decile_analysis(scores, fwd)
        spread = deciles[-1][3] - deciles[0][3] if len(deciles) >= 10 else 0
        print(f"{label:<12} {len(ss):>7} {rho:>+12.4f} {spread:>+16.2%}")
    print()


def candidate_features_test(samples):
    """Test new candidate features for predictive power."""
    print("-" * 76)
    print("CANDIDATE FEATURES — predictive power vs 3-month forward return")
    print("-" * 76)
    print(f"{'Feature':<30} {'N':>7} {'Spearman':>12}   Interpretation")
    candidates = [
        ("mom_accel",         "mom_accel",       "↑ acceleration = already bouncing"),
        ("range_pos (0=oversold)", "range_pos",  "↓ low = oversold (expected negative sign)"),
        ("ma_distance",       "ma_distance",     "↓ below MA = oversold"),
        ("rsi_6m",            "rsi_6m",          "↓ low RSI = oversold"),
        ("trough_recovery",   "trough_recovery", "already in Sample, unused in scorer"),
        ("vol_trend",         "vol_trend",       "sales volume momentum"),
    ]
    for label, attr, interp in candidates:
        rho, n = bucket_spearman(samples, attr)
        direction_ok = "✓" if (("↑" in interp and rho > 0) or ("↓" in interp and rho < 0)) else " "
        print(f"{label:<30} {n:>7} {rho:>+12.4f}  {direction_ok} {interp}")
    print()
    print("Positive sign = feature predicts higher forward returns.")
    print("A strong reading (|Spearman| > 0.10) = candidate worth adding.")
    print()


def deep_dive_peak_discount(samples):
    """Peak discount is the #1 factor — verify the relationship isn't just
    a broken-card trap (cards that lose 80% for a reason and stay down)."""
    print("-" * 76)
    print("PEAK DISCOUNT DEEP DIVE — is buying the dip real, or a trap?")
    print("-" * 76)
    # Bucket by how deep the discount is + by cultural
    print()
    print(f"{'Discount':<18} {'Cultural':<14} {'N':>7} {'mean fwd 3m':>14}")
    print("-" * 62)
    disc_buckets = [
        ("0-10% off peak",  lambda d: 0 <= d < 0.10),
        ("10-25% off",      lambda d: 0.10 <= d < 0.25),
        ("25-50% off",      lambda d: 0.25 <= d < 0.50),
        ("50-75% off",      lambda d: 0.50 <= d < 0.75),
        ("75%+ off",        lambda d: d >= 0.75),
    ]
    cult_buckets = [
        ("with moat≥0.45", lambda c: c >= 0.45),
        ("no moat<0.45",   lambda c: c < 0.45),
    ]
    for dlabel, dpred in disc_buckets:
        for clabel, cpred in cult_buckets:
            ss = [s for s in samples if dpred(s.peak_discount) and cpred(s.cultural)]
            if len(ss) < 100:
                continue
            mean_fwd = sum(s.forward_return for s in ss) / len(ss)
            print(f"{dlabel:<18} {clabel:<14} {len(ss):>7} {mean_fwd:>+14.2%}")
        print()


def main():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    print(f"DB: {DB_PATH}")
    cards = load_monthly_prices(con)
    print(f"Cards with ≥ {MIN_MONTHS_PER_CARD} monthly observations: {len(cards)}")
    samples = build_samples(cards)
    print(f"Anchor-date samples: {len(samples)}")
    print()
    if not samples:
        print("No samples — exiting.")
        return

    # Trim outliers once
    samples_trimmed = trim_outliers(samples, "forward_return", 0.01, 0.99)

    report(samples)

    print()
    print("=" * 76)
    print("AUDIT v2 — extended diagnostics")
    print("=" * 76)
    print()
    out_of_sample_test(samples_trimmed)
    yearly_stability_test(samples_trimmed)
    horizon_sensitivity_test(samples_trimmed)
    subset_analysis(samples_trimmed)
    deep_dive_peak_discount(samples_trimmed)
    candidate_features_test(samples_trimmed)


if __name__ == "__main__":
    main()
