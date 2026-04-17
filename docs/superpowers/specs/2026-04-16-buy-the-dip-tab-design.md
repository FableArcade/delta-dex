# Buy the Dip Tab + Rename Must Buy

**Date:** 2026-04-16
**Status:** Approved

## Summary

Add a "Buy the Dip" tab as the **first tab** (default view) on the card leaderboard.
Cards that have fallen significantly from their all-time PSA 10 high but show early
reversal signals. Rename "Must Buy Now" to "Top Picks."

## Data Additions

### API: `card_index` endpoint

Add three new fields to `_card_summary` in `api/routers/cards.py`, computed via
subqueries on the full `price_history` table (not limited to 12 months):

| Field | SQL | Description |
|-------|-----|-------------|
| `psa10-ath` | `MAX(psa_10_price) FROM price_history WHERE card_id = c.id` | All-time high PSA 10 price |
| `psa10-ath-date` | Date of the ATH row | When the peak was set |
| `psa10-dip-pct` | `(ATH - current) / ATH` | Fraction off ATH (0..1). Computed in JS, not SQL. |

No raw-price ATH fields. Buy the Dip is PSA-10-only.

### API: `card_detail` endpoint

Add the same `psa10-ath` and `psa10-ath-date` fields to the detail response so the
card detail page can display them later if desired.

## Buy the Dip Score (0-100, additive)

### Hard Gates (all must pass)

1. Not sealed (`is-sealed` = false)
2. PSA 10 price exists and > $10
3. PSA 10 ATH exists
4. Dip depth >= 20% off ATH
5. `supply_saturation_index` < 1 (not supply-saturated)
6. At least one of `net_flow_7d` or `net_flow_30d` > 0 (some reversal signal)

### Score Components

| Component | Max pts | Signal | Mapping |
|-----------|---------|--------|---------|
| **Dip depth** | 50 | `(ATH - current) / ATH` | 20% off = 0 pts, 80%+ off = 50 pts. Linear: `clamp01((dipPct - 0.20) / 0.60) * 50` |
| **Supply declining** | 15 | Supply saturation index (lower = tighter) | `clamp01(1.0 - satIdx) * 15`. Sat < 1 guaranteed by gate. |
| **Net flow positive** | 15 | Blend: `nf7_pct * 0.40 + nf30_pct * 0.60` | Same normalization as Must Buy: `clamp01((nfPct + 0.01) / 0.05)`. Weighted blend * 15. |
| **Price recovering** | 10 | Current PSA 10 > `psa10-30d-ago` AND current > `psa10-min-1y` | Binary: 10 pts if both true, 0 otherwise. Confirms bounce, not falling knife. |
| **Cultural floor** | 10 | `culturalImpactScore(card)` | Same function used by Top Picks. * 10. |

### Default Sort

Dip score descending. Highest opportunity first.

## Tab Configuration

### Order (left to right)

1. **Buy the Dip** (new, default active tab)
2. Top Picks (renamed from "Must Buy Now")
3. Top Chase
4. Demand Surge
5. Best Grading Play
6. Long-Term Holds

### Tab Styling

- CSS class: `buythedip`
- Active color: blue tone (`color: #004080; background: #d8e8ff;`)
- Icon: down-arrow or similar (`&#8600;` or `&#128200;`)

### Columns

| Column | Key | Notes |
|--------|-----|-------|
| Card | image + name | Same as other tabs |
| PSA 10 | `psa-10-price` | Current price |
| ATH | `psa10-ath` | All-time high |
| ATH Date | `psa10-ath-date` | When peak was set |
| Dip % | computed | `(ATH - current) / ATH * 100` |
| Sat Index | `supply-saturation-index` | Lower = tighter supply |
| Net Flow 7d | `net-flow-pct-7d` | Positive = bullish |
| Net Flow 30d | `net-flow-pct-30d` | Positive = bullish |
| Dip Score | `_dipScore` | 0-100 composite |

### Controls

- **Min Dip %** slider (default 20%, range 10-80%)
- **Min PSA 10** input (default $10)
- **Min Score** slider (default 30, range 0-100)

## Rename: Must Buy Now -> Top Picks

All references in:
- `card_leaderboard.html`: button label, CSS class display name, control labels, description text
- `card_leaderboard.js`: view string stays `"mustbuy"` internally (no refactor needed),
  but user-facing strings change to "Top Picks"
- Tab icon: keep the lightning bolt

## Files to Modify

1. **`api/routers/cards.py`** — Add `psa10-ath` and `psa10-ath-date` subqueries to
   `card_index` SQL and `_card_summary`. Add to `card_detail` response.
2. **`frontend/js/card_leaderboard.js`** — Add `computeDipScore()`, `filterBuyTheDip()`,
   `renderRowsBuyTheDip()`, `getSortValueDip()`, `HEADERS.buythedip`. Wire into
   `fullRender()` dispatch. Add controls. Rename Must Buy display strings.
3. **`frontend/card_leaderboard.html`** — Add Buy the Dip tab button (first position),
   add controls div, add CSS for `.buythedip` tab style. Rename Must Buy label.

## Performance Note

Two new `MAX()` subqueries per card in `card_index`. These are simple aggregates on an
indexed column (`card_id`). Current query already has ~12 correlated subqueries per card
and completes in 2.4s for 10,720 cards. Two more MAX subqueries should add <200ms.
If it becomes a problem, we can materialize ATH into a denormalized column later.
