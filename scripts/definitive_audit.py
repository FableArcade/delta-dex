"""
DEFINITIVE AUDIT — can a real user make money following the wishlist?

This is NOT a feature-correlation report. It simulates an ACTUAL USER:

  1. At date T, the user looks at ALL cards and picks the top N by fit score.
  2. They hold for H months.
  3. We measure the portfolio return vs a random equal-weight portfolio.

We do this at EVERY historical anchor date where we have enough forward
data, and report:
  * Mean portfolio return (top-N picks) vs mean random return
  * Win rate (% of months where top-N beat random)
  * Worst draw: what's the worst month where top-N picks lost the most?
  * Sharpe ratio: return / volatility — risk-adjusted edge

We also do a PROPER train/test split:
  * Train period: 2022-02 through 2025-06 (40 months)
  * Test period:  2025-07 through 2026-01 (7 months, fully out-of-sample)

And we do a BOOTSTRAP confidence interval on the edge:
  * Resample the monthly portfolio returns 5000 times with replacement
  * Report 5th–95th percentile of the mean edge
  * If the 5th percentile is still positive, the edge is statistically
    robust — not just a lucky sample.

Finally, we test three ALTERNATIVE approaches to see if mean reversion
is really the best:
  * Random (baseline)
  * Simple cultural-only ("just buy the most iconic cards")
  * Trend-following ("buy what's rising the fastest")
  * Mean-reversion (our scorer)
  * Oracle ("buy the cards that actually went up the most" — ceiling)

If mean-reversion doesn't beat cultural-only by a meaningful margin,
the user is better off just buying Charizards and not looking at the
dashboard at all.
"""

from __future__ import annotations

import math
import random
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path
from typing import List, Optional

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "pokemon.db"
MIN_PSA10_PRICE = 20
MIN_MONTHS = 9
FORWARD_MONTHS = 3
TOP_N = 20  # portfolio size — user picks top 20

# ---------------------------------------------------------------------------
# Import shared code from backtest script
# ---------------------------------------------------------------------------
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.backtest_wishlist_scorer import (
    load_monthly_prices, build_samples, cultural_score,
    js_native_scorer, compute_set_medians, trim_outliers,
    spearman_rank_corr, ols_fit,
)


def compute_monthly_portfolios(samples, scorer_fn, top_n=TOP_N):
    """For each anchor month, rank all samples by scorer, pick the top N,
    and record their mean forward return. Also record the universe mean
    (random baseline).

    Returns [(anchor_month, top_n_return, random_return, n_universe)]
    """
    by_month = defaultdict(list)
    for s in samples:
        by_month[s.anchor_date].append(s)

    results = []
    for month in sorted(by_month.keys()):
        ss = by_month[month]
        if len(ss) < top_n * 2:
            continue  # need at least 2× top_n for the split to be meaningful

        scores = scorer_fn(ss)
        pairs = list(zip(scores, ss))
        pairs.sort(key=lambda p: -p[0])

        top = pairs[:top_n]
        top_return = sum(s.forward_return for _, s in top) / len(top)
        all_return = sum(s.forward_return for s in ss) / len(ss)

        results.append((month, top_return, all_return, len(ss)))

    return results


def cultural_only_scorer(samples):
    """Baseline: just rank by cultural score."""
    return [s.cultural for s in samples]


def trend_following_scorer(samples):
    """Baseline: rank by strongest recent momentum (the old logic)."""
    return [s.mom_3m for s in samples]


def oracle_scorer(samples):
    """Ceiling: rank by actual forward return (impossible to know in advance)."""
    return [s.forward_return for s in samples]


def bootstrap_ci(values, n_boot=5000, ci=0.90):
    """Return (mean, lo, hi) for a 90% bootstrap CI."""
    n = len(values)
    if n == 0:
        return (0, 0, 0)
    means = []
    for _ in range(n_boot):
        boot = [values[random.randint(0, n - 1)] for _ in range(n)]
        means.append(sum(boot) / n)
    means.sort()
    lo_idx = int(n_boot * (1 - ci) / 2)
    hi_idx = int(n_boot * (1 + ci) / 2)
    return (sum(values) / n, means[lo_idx], means[hi_idx])


def sharpe(returns):
    """Annualised Sharpe ratio (assuming monthly returns, 12 months/year)."""
    if len(returns) < 2:
        return 0
    mu = sum(returns) / len(returns)
    sd = statistics.stdev(returns)
    if sd == 0:
        return 0
    return (mu / sd) * math.sqrt(12 / FORWARD_MONTHS)


def report_strategy(name, results, random_results=None):
    """Print a strategy report."""
    top_returns = [r[1] for r in results]
    rand_returns = [r[2] for r in results]
    edges = [t - r for t, r in zip(top_returns, rand_returns)]

    mean_top = sum(top_returns) / len(top_returns) if top_returns else 0
    mean_rand = sum(rand_returns) / len(rand_returns) if rand_returns else 0
    mean_edge = sum(edges) / len(edges) if edges else 0
    win_rate = sum(1 for e in edges if e > 0) / len(edges) if edges else 0
    worst = min(top_returns) if top_returns else 0
    best = max(top_returns) if top_returns else 0

    # Bootstrap CI on the edge
    boot_mean, boot_lo, boot_hi = bootstrap_ci(edges)

    print(f"  {name}:")
    print(f"    Months tested:        {len(results)}")
    print(f"    Mean 3m return (top{TOP_N}):  {mean_top:+.2%}")
    print(f"    Mean 3m return (random): {mean_rand:+.2%}")
    print(f"    Mean EDGE over random:   {mean_edge:+.2%}")
    print(f"    Win rate (beat random):  {win_rate:.0%} of months")
    print(f"    Sharpe (top{TOP_N}):          {sharpe(top_returns):.2f}")
    print(f"    Worst month:             {worst:+.2%}")
    print(f"    Best month:              {best:+.2%}")
    print(f"    Bootstrap 90% CI edge:   [{boot_lo:+.2%}, {boot_hi:+.2%}]")
    if boot_lo > 0:
        print(f"    --> STATISTICALLY SIGNIFICANT (5th pct > 0)")
    elif boot_hi < 0:
        print(f"    --> NEGATIVE EDGE (even 95th pct < 0)")
    else:
        print(f"    --> NOT SIGNIFICANT (CI straddles zero)")
    print()
    return mean_edge, boot_lo, boot_hi


def main():
    random.seed(42)

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cards = load_monthly_prices(con)
    print(f"Cards: {len(cards)}")
    samples = build_samples(cards)
    print(f"Total samples: {len(samples)}")
    samples = trim_outliers(samples, "forward_return", 0.01, 0.99)
    print(f"After trimming: {len(samples)}")
    print()

    # ---- Split into train (early) and test (late) ----
    sorted_samples = sorted(samples, key=lambda s: s.anchor_date)
    train_cutoff = "2025-07"
    train = [s for s in sorted_samples if s.anchor_date < train_cutoff]
    test = [s for s in sorted_samples if s.anchor_date >= train_cutoff]
    print(f"Train: {len(train)} samples ({train[0].anchor_date} to {train[-1].anchor_date})")
    print(f"Test:  {len(test)} samples ({test[0].anchor_date} to {test[-1].anchor_date})")
    print()

    # Pre-compute set medians for both sets
    train_sm = compute_set_medians(train)
    test_sm = compute_set_medians(test)

    # ---- Define strategies ----
    def mean_reversion_scorer_train(ss):
        return js_native_scorer(ss, train_sm)

    def mean_reversion_scorer_test(ss):
        return js_native_scorer(ss, test_sm)

    # ==================================================================
    print("=" * 70)
    print(f"FULL SAMPLE — all {len(samples)} observations")
    print("=" * 70)
    print()

    sm_all = compute_set_medians(samples)
    strategies = [
        ("Mean-reversion (our scorer)", lambda ss: js_native_scorer(ss, sm_all)),
        ("Cultural-only (just buy iconic)", cultural_only_scorer),
        ("Trend-following (buy what's rising)", trend_following_scorer),
        ("Oracle (impossible — forward knowledge)", oracle_scorer),
    ]
    for name, scorer in strategies:
        results = compute_monthly_portfolios(samples, scorer)
        report_strategy(name, results)

    # ==================================================================
    print("=" * 70)
    print(f"OUT-OF-SAMPLE TEST — train before {train_cutoff}, test after")
    print("=" * 70)
    print()
    print("--- TRAIN PERIOD ---")
    for name, scorer in [
        ("Mean-reversion", mean_reversion_scorer_train),
        ("Cultural-only", cultural_only_scorer),
        ("Trend-following", trend_following_scorer),
    ]:
        results = compute_monthly_portfolios(train, scorer)
        report_strategy(f"{name} [train]", results)

    print("--- TEST PERIOD (fully out-of-sample) ---")
    for name, scorer in [
        ("Mean-reversion", mean_reversion_scorer_test),
        ("Cultural-only", cultural_only_scorer),
        ("Trend-following", trend_following_scorer),
    ]:
        results = compute_monthly_portfolios(test, scorer)
        report_strategy(f"{name} [TEST]", results)

    # ==================================================================
    print("=" * 70)
    print("HEAD-TO-HEAD: Mean-reversion vs Cultural-only")
    print("=" * 70)
    print()
    print("If cultural-only is nearly as good as mean-reversion, the user is")
    print(f"better off just buying the {TOP_N} most iconic cards and ignoring")
    print("the dashboard entirely. The scorer must BEAT cultural-only to justify")
    print("its complexity.")
    print()

    mr_results = compute_monthly_portfolios(samples, lambda ss: js_native_scorer(ss, sm_all))
    co_results = compute_monthly_portfolios(samples, cultural_only_scorer)

    mr_returns = [r[1] for r in mr_results]
    co_returns = [r[1] for r in co_results]

    # Must use same months for comparison
    months_both = set(r[0] for r in mr_results) & set(r[0] for r in co_results)
    mr_by_month = {r[0]: r[1] for r in mr_results}
    co_by_month = {r[0]: r[1] for r in co_results}

    edges_vs_cultural = []
    for m in sorted(months_both):
        edges_vs_cultural.append(mr_by_month[m] - co_by_month[m])

    mean_edge_vc = sum(edges_vs_cultural) / len(edges_vs_cultural) if edges_vs_cultural else 0
    vc_mean, vc_lo, vc_hi = bootstrap_ci(edges_vs_cultural)
    win_rate_vc = sum(1 for e in edges_vs_cultural if e > 0) / len(edges_vs_cultural)

    print(f"Mean-reversion mean return:   {sum(mr_returns)/len(mr_returns):+.2%}")
    print(f"Cultural-only mean return:    {sum(co_returns)/len(co_returns):+.2%}")
    print(f"Mean-reversion EDGE vs cultural: {mean_edge_vc:+.2%}")
    print(f"Win rate vs cultural:         {win_rate_vc:.0%} of months")
    print(f"Bootstrap 90% CI:             [{vc_lo:+.2%}, {vc_hi:+.2%}]")
    if vc_lo > 0:
        print("--> Mean-reversion is STATISTICALLY BETTER than just buying iconic cards.")
    else:
        print("--> Edge over cultural-only is NOT statistically significant.")
        print("    Users might get comparable results by simply buying the most iconic cards.")
    print()

    # ==================================================================
    print("=" * 70)
    print("VERDICT")
    print("=" * 70)
    print()

    # Pull the test-period results for the final verdict
    mr_test = compute_monthly_portfolios(test, mean_reversion_scorer_test)
    co_test = compute_monthly_portfolios(test, cultural_only_scorer)
    tf_test = compute_monthly_portfolios(test, trend_following_scorer)

    print(f"{'Strategy':<35} {'Mean 3m ret':>12} {'Edge vs random':>16} {'Sharpe':>8}")
    for name, results in [
        ("Mean-reversion [TEST]", mr_test),
        ("Cultural-only [TEST]", co_test),
        ("Trend-following [TEST]", tf_test),
    ]:
        rets = [r[1] for r in results]
        rands = [r[2] for r in results]
        edge = sum(r - b for r, b in zip(rets, rands)) / len(rets) if rets else 0
        mean_ret = sum(rets) / len(rets) if rets else 0
        s = sharpe(rets)
        print(f"{name:<35} {mean_ret:>+12.2%} {edge:>+16.2%} {s:>8.2f}")


if __name__ == "__main__":
    main()
