/* ============================================================
   OPPORTUNITY_FINDER.EXE — card_leaderboard.js

   Three view modes:

   1) UNDERVALUED / OVERVALUED (grading EV)
      Score = gem_rate × psa10_price − grading_fee − raw_price
      Uses real gem rate from PSA population when available, else
      a user-adjustable fallback. Filters to singles with PSA 10 price.

   2) LONG-TERM HOLDS (momentum × discount from peak)
      Score = momentum_90d × (1 + peak_discount)
         where momentum_90d = (current − 90d_ago) / 90d_ago
               peak_discount = (12mo_max − current) / 12mo_max
      Applied to SEALED products (raw_price history) AND/OR
      PSA 10 singles (psa_10_price history).
      Filters: must have sufficient history.

   The table header is rebuilt on view change so we can show
   different columns per mode.
   ============================================================ */

const API_BASE = "/api";

// --- state ---
let allCards = [];
let modelProjections = {};  // card_id -> projection data from /api/model/projections

let view = "mustbuy";  // "mustbuy" | "undervalued" | "overvalued" | "holds" | "all"

// Constants used by computeEvScore (which feeds into Must Buy Now's hard gates).
// No UI to tweak these now — Must Buy Now uses fixed defaults so the score is
// stable across sessions.
const GEM_RATE_DEFAULT = 0.10;
const GRADING_FEE = 25;

// Hold-view knobs
let holdType = "both";
let minHistoryDays = 60;
let minHoldPrice = 200;

// Long-Term Holds — conviction gates. A hold is a multi-month thesis, so the
// bar is HIGHER than Must Buy Now (which only needs the next-30d setup):
//   * cultural moat (iconic Pokemon OR chase rarity), and
//   * positive demand sentiment (net flow > 0 after API negation), and
//   * supply not saturated.
// Sealed lacks per-card market_pressure, so it gets a softer rule (the hold
// score itself + a positive momentum check is enough — sealed has implicit
// brand floor since every chase product is iconic in its own right).
const HOLDS_MIN_CULTURAL = 0.20;       // higher than Must Buy's 0.10
const HOLDS_MIN_HOLD_SCORE = 0.05;     // 5% — keeps flat cards out
const HOLDS_MIN_MOMENTUM   = 0.03;     // 3% real appreciation over anchor window
// Sealed gets a softer but non-trivial score gate. Sealed lacks per-card
// market_pressure + cultural name matching (products aren't named "Charizard"),
// so the hold score itself is the conviction signal. A booster box up 10%
// over 90d is a real thesis; 1% is noise.
const HOLDS_MIN_SEALED_SCORE = 0.08;   // 8% composite score minimum for sealed

// Must-buy view knobs (smart-investor composite score, 0-100)
let mustBuyMinScore = 50;       // default min composite score to qualify
// Cultural hard gate: 0.15 requires EITHER an iconic Pokemon name match
// OR a chase rarity ≥ Special Illustration Rare (0.20). Basic Energy cards
// with only a Hyper Rare rarity bonus (0.12) fall below this — correctly,
// since there's no named-Pokemon floor to hold their price.
const MUSTBUY_MIN_CULTURAL = 0.15;

// Top Chase view knobs
// chaseMinDemand is in normalized units (net_flow_pct). Real-data scale is
// 0 to ~4% (positive) and symmetrically bearish on the downside. −0.005 is
// a NOISE TOLERATOR, not a discriminator — it lets cards with minor daily
// wobble (−0.5%) through but kicks out strict bearish trends. The real
// ranking is done by the score formula; this gate only drops the worst
// outliers so they don't pollute the tail.
let chaseMinPsa10 = 200;    // floor for "chase tier" — still investable
let chaseMinDemand = -0.005;

// Demand Surge view knobs.
// dsMinNfPct is in normalized units: daily net absorption as a fraction of
// the active pool. Calibration from real data (Apr 2026):
//   * across ALL cards passing other gates, max nf_pct ≈ 4%
//   * p95 ≈ 3.6%, median ≈ 1.2%, min > 0 ≈ 0.4%
// A daily 1% rate already clears ~7% of the pool per week, which is a
// credible surge. The earlier 5% threshold was based on a theoretical
// 0-20% range and filtered out every single card.
let dsMinPsa10 = 100;
let dsMinNfPct = 0.010;  // 1.0% daily = ~7% weekly absorption

// Best Grading Play view knobs
// Raw floor is $30 by default: below that, the $25 grading fee dominates
// the cost basis and the math is dominated by fee drag rather than the
// actual price differential. Users can lower it if they want to see more
// lottery-ticket trades, but the default is the honest investor lens.
let bgMinPsa10 = 100;
let bgMinRaw = 30;

let displayedCount = 0;
const PAGE_SIZE = 100;

// Sort state — different defaults per view family
let currentSort = { key: "mbscore", dir: "desc" };

// Current filtered list (cached so Load More and column sort don't re-fetch)
let _currentList = [];

// --- formatters ---
function money(x) {
    if (x === null || x === undefined || !Number.isFinite(Number(x))) return "\u2014";
    return Number(x).toLocaleString("en-US", {
        style: "currency", currency: "USD",
        minimumFractionDigits: 2, maximumFractionDigits: 2
    });
}
function pctSigned(x, digits = 0) {
    if (x === null || x === undefined || !Number.isFinite(Number(x))) return "\u2014";
    const n = Number(x) * 100;
    const sign = n > 0 ? "+" : "";
    return `${sign}${n.toFixed(digits)}%`;
}
function rankClass(r) {
    if (r === 1) return "rank rank-1";
    if (r === 2) return "rank rank-2";
    if (r === 3) return "rank rank-3";
    return "rank";
}

// HTML-escape user-sourced strings (card names, set codes, image URLs) before
// they get spliced into innerHTML templates. The data ultimately comes from
// PriceCharting / our own DB, but defending against an injected card name
// (e.g. one that contains <script>) is cheap insurance.
function esc(s) {
    if (s == null) return "";
    return String(s)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}
function evChipClass(v) {
    if (!Number.isFinite(v) || Math.abs(v) < 0.005) return "ev-chip zero";
    return v > 0 ? "ev-chip pos" : "ev-chip neg";
}

// --- scoring ---

function computeEvScore(card) {
    const raw = Number(card["raw-price"]);
    const psa10 = Number(card["psa-10-price"]);
    if (!Number.isFinite(raw) || !Number.isFinite(psa10) || psa10 <= 0) {
        card._ev = null; card._roi = null; card._gemUsed = null; card._gemIsReal = false;
        return;
    }
    const realGem = Number(card["gem-pct"]);
    const gem = Number.isFinite(realGem) && realGem > 0 ? realGem : GEM_RATE_DEFAULT;
    card._gemUsed = gem;
    card._gemIsReal = Number.isFinite(realGem) && realGem > 0;
    const ev = gem * psa10 - GRADING_FEE - raw;
    const cost = GRADING_FEE + raw;
    card._ev = ev;
    card._roi = cost > 0 ? ev / cost : 0;
}

/**
 * Best Grading Play — real expected-value math.
 *
 * The honest question isn't "how big is the raw→PSA 10 multiple?" but
 * "if I spend $raw + $fee to grade this card, what's my expected payout?"
 *
 * Answer:
 *
 *     EV  =  p10 × psa10  +  p9 × psa9  +  p8 × psa8  +  p7 × psa7  −  fee − raw
 *     ROI =  EV / (fee + raw)
 *
 * Where p10/p9/p8/p7 are grading probabilities. We use the real `gem-pct`
 * (PSA 10 rate from population data) when available; otherwise we fall back
 * to GEM_RATE_DEFAULT = 0.10 and flag the row as "assumed" so the user
 * knows the estimate is noisy.
 *
 * Sub-grade recovery is substantial for many cards (a PSA 9 can still be
 * worth 30-60% of a PSA 10). Ignoring it punishes cards that grade well
 * but not always perfectly. We estimate the non-PSA-10 distribution
 * conservatively: assume half the misses hit PSA 9, a third hit PSA 8,
 * the rest hit PSA 7 or worse (treated as zero-recovery).
 *
 * Fields written:
 *    _bgEv       expected value in dollars (can be negative)
 *    _bgRoi      return on cost as a fraction (−1.0 = total loss)
 *    _bgGain     alias of _bgEv for display ($ expected profit)
 *    _bgPct      ROI as a percentage (what the UI shows as "EV %")
 *    _bgMult     payout / cost (1.0 = break-even, 2.0 = double your money)
 *    _bgGemUsed  gem rate used (real or default)
 *    _bgGemReal  true if real PSA pop data was used
 */
function computeBestGradingScore(card) {
    const raw = Number(card["raw-price"]);
    const psa10 = Number(card["psa-10-price"]);
    if (!Number.isFinite(raw) || raw <= 0 || !Number.isFinite(psa10) || psa10 <= 0) {
        card._bgEv = null; card._bgRoi = null; card._bgGain = null;
        card._bgPct = null; card._bgMult = null;
        card._bgGemUsed = null; card._bgGemReal = false;
        return;
    }

    // Real gem rate from PSA pop data if we have it; otherwise a conservative
    // 10% default (honest industry baseline for modern chase cards).
    const realGem = Number(card["gem-pct"]);
    const gem = Number.isFinite(realGem) && realGem > 0 ? realGem : GEM_RATE_DEFAULT;
    const gemIsReal = Number.isFinite(realGem) && realGem > 0;

    // Sub-grade recovery. Of the cards that DON'T hit PSA 10:
    //   half get PSA 9           (still liquid, 30-60% of PSA 10 market)
    //   third get PSA 8          (recovers some money, thin market)
    //   remainder get PSA 7-     (essentially zero-recovery; the grading fee
    //                             dominates so we assign 0 so we're honest)
    const miss = 1 - gem;
    const p9 = miss * 0.50;
    const p8 = miss * 0.33;
    // p7 = miss * 0.17 → treated as zero-value recovery

    const psa9  = Number(card["psa-9-price"]);
    const psa8  = Number(card["psa-8-price"]);

    // Use real sub-grade prices if present. When missing, fall back to
    // market heuristics: PSA 9 ≈ 35% of PSA 10, PSA 8 ≈ 15% of PSA 10.
    // These are conservative industry averages for modern English TCG.
    const p9Price = Number.isFinite(psa9) && psa9 > 0 ? psa9 : psa10 * 0.35;
    const p8Price = Number.isFinite(psa8) && psa8 > 0 ? psa8 : psa10 * 0.15;

    const payout = gem * psa10 + p9 * p9Price + p8 * p8Price;
    const cost   = GRADING_FEE + raw;
    const ev     = payout - cost;
    const roi    = ev / cost;
    const mult   = payout / cost;

    card._bgEv      = ev;
    card._bgRoi     = roi;
    card._bgGain    = ev;               // display alias
    card._bgPct     = roi * 100;        // ROI as percentage for the table chip
    card._bgMult    = mult;
    card._bgGemUsed = gem;
    card._bgGemReal = gemIsReal;
}

/**
 * Compute long-term-hold score on either raw (sealed) or psa10 (graded)
 * price history. Returns an object with { current, anchor, peak, trough,
 * momentum, peakDiscount, score, track, anchorWindow, confirmedBottom }
 * — or null if insufficient data.
 *
 * Value-trap protection: the score is <code>momentum × (1 + capped_discount)</code>
 * where capped_discount is clamped to 0.50 (50% off peak max). Uncapped,
 * a card 90% off peak with a 3% bounce would score 1.9× a card at peak,
 * even though a 90% drawdown usually isn't finished bouncing. Capping the
 * multiplier to 1.5 keeps the "buy the dip in an uptrend" bonus without
 * rewarding still-bleeding cards.
 *
 * confirmedBottom: true when current > trough × 1.3, meaning the card has
 * recovered at least 30% off its 12mo low. Used by filterHolds to exclude
 * cards that are 80% off peak but still sitting at their low (falling knife).
 *
 * @param {object} card
 * @param {"raw"|"psa10"} track - which price series to evaluate
 */
function computeHoldScoreFor(card, track) {
    const curKey    = track === "raw" ? "raw-price"     : "psa-10-price";
    const a90Key    = track === "raw" ? "raw-90d-ago"   : "psa10-90d-ago";
    const a30Key    = track === "raw" ? "raw-30d-ago"   : "psa10-30d-ago";
    const a365Key   = track === "raw" ? "raw-365d-ago"  : "psa10-365d-ago";
    const maxKey    = track === "raw" ? "raw-max-1y"    : "psa10-max-1y";
    const minKey    = track === "raw" ? "raw-min-1y"    : "psa10-min-1y";

    const current = Number(card[curKey]);
    if (!Number.isFinite(current) || current <= 0) return null;

    // Prefer 90d anchor. We track anchor window separately so the UI can
    // distinguish real 90d conviction from 30d-only fallback and display
    // "thin" badges accordingly.
    let anchor = Number(card[a90Key]);
    let anchorWindow = "90d";
    if (!Number.isFinite(anchor) || anchor <= 0) {
        anchor = Number(card[a30Key]);
        anchorWindow = "30d";
    }
    if (!Number.isFinite(anchor) || anchor <= 0) return null;

    const peak = Number(card[maxKey]);
    const trough = Number(card[minKey]);
    if (!Number.isFinite(peak) || peak <= 0) return null;

    const momentum = (current - anchor) / anchor;                // >0 = appreciating
    const peakDiscount = Math.max(0, (peak - current) / peak);   // 0..1 raw

    // CAP the peak-discount contribution at 50%. Uncapped, a 90%-off card
    // would get a 1.9× multiplier on its momentum, rewarding reflation plays
    // that may still be bleeding. Capping recognizes that beyond ~50% off
    // peak you're buying a fundamentally different asset class (distressed).
    const cappedDiscount = Math.min(0.50, peakDiscount);
    const score = momentum * (1 + cappedDiscount);

    // Confirmed-bottom flag: has the price moved meaningfully off the 1-year
    // low? Cards sitting right at trough are falling knives no matter how
    // pretty the math. 1.20× is the right threshold given real-data volatility
    // (p75 range/current ≈ 0.39); a stricter 1.30× was culling legitimate
    // early-recovery plays.
    const confirmedBottom = Number.isFinite(trough) && trough > 0
        ? (current >= trough * 1.2)
        : true;  // unknown trough → don't block, but note it

    return {
        track,
        current,
        anchor,
        anchorWindow,
        peak,
        trough: Number.isFinite(trough) ? trough : null,
        momentum,
        peakDiscount,           // raw (for display)
        cappedDiscount,         // used in score formula
        confirmedBottom,
        score,
    };
}

function computeHoldScore(card) {
    // For sealed: only the raw track makes sense.
    if (card["is-sealed"]) {
        card._hold = computeHoldScoreFor(card, "raw");
        return;
    }
    // For singles: score the PSA 10 track.
    card._hold = computeHoldScoreFor(card, "psa10");
}

// ==========================================================================
// CULTURAL IMPACT — hardcoded brand strength scoring for iconic Pokemon and
// fan-favorite trainers. A real investor knows that a Charizard at $X is
// safer than an obscure card at $X because demand has a floor.
// Tier values (0..1) are based on community fan polls + auction data.
// ==========================================================================

const ICONIC_NAMES = [
    // Tier S — franchise faces (1.0)
    [/charizard/i,        1.00],
    [/pikachu/i,          1.00],
    [/mewtwo/i,           0.96],
    [/\bmew\b/i,          0.96],   // matches "Mew" exactly, not "Mewtwo"
    [/umbreon/i,          0.96],   // moonbreon-driven cult status

    // Tier A — legendary / iconic gen 1 (0.85)
    [/lugia/i,            0.88],
    [/rayquaza/i,         0.88],
    [/gengar/i,           0.85],
    [/snorlax/i,          0.82],
    [/dragonite/i,        0.82],
    [/blastoise/i,        0.78],
    [/venusaur/i,         0.78],
    [/gyarados/i,         0.80],

    // Tier A- — chase pseudo-legends + fan-vote winners (0.75-0.82)
    [/greninja/i,         0.82],
    [/lucario/i,          0.80],
    [/garchomp/i,         0.78],
    [/zoroark/i,          0.75],
    [/sceptile/i,         0.72],
    [/blaziken/i,         0.72],
    [/swampert/i,         0.72],

    // Tier B — Eeveelutions (Umbreon already covered) — 0.72
    [/sylveon/i,          0.78],   // fan favorite + recent
    [/espeon/i,           0.75],
    [/leafeon/i,          0.72],
    [/glaceon/i,          0.72],
    [/vaporeon/i,         0.70],
    [/jolteon/i,          0.70],
    [/flareon/i,          0.70],
    [/eevee/i,            0.72],

    // Tier C — popular legendaries / tournament Pokemon (0.65-0.72)
    [/giratina/i,         0.70],
    [/dialga/i,           0.65],
    [/palkia/i,           0.65],
    [/arceus/i,           0.72],
    [/zekrom|reshiram/i,  0.65],
    [/yveltal|xerneas/i,  0.62],
    [/groudon|kyogre/i,   0.65],
    [/zacian|zamazenta/i, 0.62],
    [/calyrex/i,          0.60],

    // Tier C — chase trainer characters (cult followings)
    [/cynthia/i,          0.75],   // beloved champion
    [/lillie/i,           0.72],
    [/acerola/i,          0.70],
    [/iono/i,             0.68],
    [/marnie/i,           0.65],
    [/\bhop\b|\bleon\b/i, 0.55],   // word-anchored so "chameleon" doesn't match
    [/\bN['\u2019]s\b/i,  0.65],   // Black/White N — requires N's with word boundary
    [/team rocket/i,      0.60],   // recent set hype
    [/giovanni/i,         0.62],
    [/erika/i,            0.55],
    [/misty/i,            0.62],
    [/brock/i,            0.55],
];

// Rarity bonus on top of name match — chase rarities deserve a small boost
// even if the Pokemon isn't on the iconic list.
const RARITY_BONUS = {
    "Special Illustration Rare": 0.20,
    "Hyper Rare":                0.12,
    "Mega Hyper Rare":           0.18,
    "Mega Attack Rare":          0.12,
    "Secret Rare":               0.12,
    "Rainbow Rare":              0.12,
    "Gold Rare":                 0.12,
    "Illustration Rare":         0.08,
    "Ultra Rare":                0.05,
};

function culturalImpactScore(card) {
    let nameScore = 0;
    const name = card["product-name"] || "";
    for (const [re, score] of ICONIC_NAMES) {
        if (re.test(name) && score > nameScore) nameScore = score;
    }
    const rarityBonus = RARITY_BONUS[card["rarity-name"]] || 0;
    return Math.max(0, Math.min(1, nameScore + rarityBonus));
}

/**
 * Smart Investor Must Buy Now — composite 0-100 score.
 *
 * Five INDEPENDENT signal dimensions. The prior version had two scarcity
 * sub-signals (listings_trend_ratio and active_listings_delta_pct) that
 * were algebraically the same number, so the "3 signals must agree"
 * framing was an illusion and scarcity was effectively a duplicate of
 * listings-trend. This version uses genuinely independent signals:
 *
 *   15%  Cultural Impact   iconic Pokemon + chase rarity (brand floor)
 *   25%  Demand Momentum   NORMALIZED net flow % (scale-invariant) +
 *                           demand/supply ratio
 *   25%  Real Scarcity     3 INDEPENDENT signals:
 *                           - lifetime PSA 10 pop (absolute rarity)
 *                           - current supply tightness (listings ratio)
 *                           - 12mo price-range stability (behavioral scarcity)
 *                          All three measure different things.
 *   15%  PSA 10 Momentum   trajectory (rising / rebound / flat / falling)
 *   20%  Grading Value     REAL EV from computeEvScore, not PSA/raw ratio
 *                          (which would double-count gem rate).
 *
 * Hard gates: not sealed, PSA 10 ≥ $20, supply_saturation_index < 1,
 * cultural ≥ MUSTBUY_MIN_CULTURAL, normalized net flow data present.
 */
function computeMustBuyScore(card) {
    if (card["is-sealed"]) { card._mbScore = null; card._mbComps = null; return; }

    const psa10 = Number(card["psa-10-price"]);
    if (!Number.isFinite(psa10) || psa10 < 20) {
        card._mbScore = null; card._mbComps = null; return;
    }

    // numOrNull: null/undefined/empty stays null. Number(null) → 0 which would
    // falsely satisfy `isFinite` and pollute scores for cards with no market data.
    const numOrNull = (v) => (v === null || v === undefined || v === "") ? null : Number(v);

    const raw     = numOrNull(card["raw-price"]);
    const nf7Pct  = numOrNull(card["net-flow-pct-7d"]);
    const nf30Pct = numOrNull(card["net-flow-pct-30d"]);
    const dem     = numOrNull(card["demand-pressure"]);
    const sup     = numOrNull(card["supply-pressure"]);
    const satIdx  = numOrNull(card["supply-saturation-index"]);
    const pop     = numOrNull(card["psa-10-pop"]);
    const gemPct  = numOrNull(card["gem-pct"]);
    const a30     = numOrNull(card["psa10-30d-ago"]);
    const a90     = numOrNull(card["psa10-90d-ago"]);
    const max1y   = numOrNull(card["psa10-max-1y"]);
    const min1y   = numOrNull(card["psa10-min-1y"]);
    const historyDays = Number(card["history-days"]) || 0;

    // Hard gate: need real normalized demand data. Without nf_pct we can't
    // compute a scale-invariant demand score, and the composite would be
    // dominated by the missing-data penalty.
    if (nf30Pct === null) {
        card._mbScore = null; card._mbComps = null; return;
    }

    // Hard gate: supply saturation index MUST be < 1. A card with saturated
    // current listings (sat ≥ 1) can't be a "must buy now" no matter how
    // strong the other signals are.
    if (satIdx === null || satIdx >= 1) {
        card._mbScore = null; card._mbComps = null; return;
    }

    const clamp01 = v => Math.max(0, Math.min(1, v));

    // ---- 1. Cultural impact (0..1) ----
    const cultural = culturalImpactScore(card);

    // Hard gate: without cultural moat, even a strong technical setup is
    // fragile — there's no demand base to fall back on if sentiment cools.
    if (cultural < MUSTBUY_MIN_CULTURAL) {
        card._mbScore = null; card._mbComps = null; return;
    }

    // ---- 2. Demand momentum (0..1) — NORMALIZED, real-data calibrated ----
    // net_flow_pct is listings absorbed per day as a fraction of the active
    // pool. Real-data max across the catalog is ~4% daily — anything above
    // that is essentially "pool clearing fast." Calibration (Apr 2026):
    //   * nf_pct -1%  → 0.0  (strong bearish, reject)
    //   * nf_pct  0%  → 0.20 (neutral)
    //   * nf_pct +1%  → 0.40 (weak drain)
    //   * nf_pct +2%  → 0.60 (moderate drain)
    //   * nf_pct +3%  → 0.80 (strong drain)
    //   * nf_pct +4%+ → 1.00 (max)
    const nf7Norm  = nf7Pct  !== null ? clamp01((nf7Pct  + 0.01) / 0.05) : 0.2;
    const nf30Norm = nf30Pct !== null ? clamp01((nf30Pct + 0.01) / 0.05) : 0.2;

    // Demand/supply ratio — complements nfPct by measuring the absolute ratio
    // of buyers to sellers, not the rate of change. Real-data range: max=2.0,
    // p95=1.33, median=1.02. Calibration maps 1.0→0, 1.25→0.5, 1.5+→1.0 so
    // the signal actually discriminates across the real range instead of
    // being squished into the bottom third.
    let dsRatio = 0;
    if (dem !== null && sup !== null && sup > 0) {
        dsRatio = clamp01((dem / sup - 1.0) / 0.5);
    }

    // Weight nf7 less than nf30 (noisier) and blend in dsRatio.
    const demand = nf7Norm * 0.25 + nf30Norm * 0.50 + dsRatio * 0.25;

    // ---- 3. REAL SCARCITY (0..1) — three INDEPENDENT signals ----

    // 3a. Lifetime population scarcity (0..1) — absolute rarity.
    let popScarce = null;
    if (pop !== null && pop > 0) {
        if      (pop <= 100)  popScarce = 1.00;
        else if (pop <= 500)  popScarce = 1.00 - (pop - 100) / 800;       // 1.0 → 0.5
        else if (pop <= 2000) popScarce = 0.50 - (pop - 500) / 3000;      // 0.5 → 0.0
        else                   popScarce = 0.00;
    }

    // 3b. Current supply tightness (0..1) — listings_7d vs 30d baseline.
    //     satIdx is REQUIRED by the hard gate above (< 1), so post-gate real
    //     distribution lives in ≈[0.41, 1.0]. This remap spreads that range
    //     across the full 0..1 instead of compressing it into [0.5, 1.0]:
    //        sat=0.41 → 1.0 (min observed, maximally tight)
    //        sat=0.75 → 0.50
    //        sat=1.00 → 0.0  (right at the gate)
    //     The old `1.5 - satIdx` formula pinned median to ~0.5 and wasted
    //     half the range; this new slope actually discriminates across the
    //     post-gate population.
    const supplyTight = clamp01((1.0 - satIdx) / 0.6);

    // 3c. Price-range STABILITY (0..1) — behavioral scarcity.
    //     A card whose PSA 10 price has stayed in a narrow band over 12 months
    //     (small max−min spread relative to current) is a HELD asset — people
    //     aren't flipping, they're stacking. A card with a wide 12mo range is
    //     volatile / speculative. This is genuinely independent of listings
    //     momentum.
    let priceStable = null;
    if (max1y !== null && min1y !== null && max1y > 0 && psa10 > 0 && historyDays >= 60) {
        const range = (max1y - min1y) / psa10;
        // range = 0.20 (20% swing) → 1.0, range = 1.0 (100% swing) → 0
        priceStable = clamp01(1.25 - range * 1.25);
    }

    // Scarcity composite — three signals, weighted. Missing data scores 0
    // (not a default floor), so absence of signal never inflates the score.
    // Alignment bonus: if all three present AND all ≥ 0.5, full credit;
    // missing signals only contribute if they happen to be strong.
    const popS    = popScarce    !== null ? popScarce    : 0;
    const stableS = priceStable  !== null ? priceStable  : 0;
    const scarcityRaw = 0.35 * popS + 0.40 * supplyTight + 0.25 * stableS;
    // Misalignment penalty: if any KNOWN signal is < 0.3 the thesis is weak.
    const knowns = [supplyTight];
    if (popScarce   !== null) knowns.push(popScarce);
    if (priceStable !== null) knowns.push(priceStable);
    const weakest = Math.min(...knowns);
    const alignmentPenalty = weakest < 0.3 ? 0.6 : weakest < 0.5 ? 0.85 : 1.0;
    const scarcity = scarcityRaw * alignmentPenalty;

    // ---- 4. PSA 10 momentum (0..1) ----
    // Give partial credit when only 30d anchor exists (new cards with < 90d
    // history shouldn't get zero momentum; they just get a reduced ceiling).
    let momentum = 0;
    let trajectory = "unknown";
    if (a30 !== null && a30 > 0) {
        const change30 = (psa10 - a30) / a30;
        if (a90 !== null && a90 > 0) {
            if (psa10 > a30 && a30 > a90) {
                trajectory = "rising";
                momentum = 0.70 + clamp01(change30 / 0.30) * 0.30;   // 0.70 - 1.00
            } else if (psa10 > a30 && a30 <= a90) {
                trajectory = "rebound";
                momentum = 0.50 + clamp01(change30 / 0.30) * 0.30;   // 0.50 - 0.80
            } else if (Math.abs(change30) < 0.02) {
                trajectory = "flat";
                momentum = 0.30;
            } else {
                trajectory = "falling";
                momentum = clamp01(0.30 + change30);                 // 0 - 0.30
            }
        } else {
            // Only 30d anchor available — give partial credit, capped lower
            // than the 90d-confirmed version. Can't distinguish rising from
            // rebound without the 90d arc, so treat as "thin momentum".
            trajectory = change30 > 0.02 ? "thin-rising" :
                         change30 < -0.02 ? "thin-falling" : "thin-flat";
            if      (change30 >  0.02) momentum = 0.40 + clamp01(change30 / 0.30) * 0.20;  // 0.40 - 0.60
            else if (change30 < -0.02) momentum = clamp01(0.25 + change30);                 // 0 - 0.25
            else                       momentum = 0.30;
        }
    }

    // ---- 5. Grading value (0..1) — real EV, no double-counting ----
    // Use the same EV math as Best Grading Play. This incorporates grading
    // fee + sub-grade recovery + real or assumed gem rate without the prior
    // "average with gemPct * 1.5" hack that double-counted gem rate.
    let gradingValue = 0;
    let evForDisplay = null;
    let roiForDisplay = null;
    if (raw !== null && raw > 0) {
        const realGem = gemPct;
        const gem = realGem !== null && realGem > 0 ? realGem : GEM_RATE_DEFAULT;
        const miss = 1 - gem;
        const p9  = miss * 0.50;
        const p8  = miss * 0.33;
        const psa9  = numOrNull(card["psa-9-price"]);
        const psa8  = numOrNull(card["psa-8-price"]);
        const p9Price = psa9 !== null && psa9 > 0 ? psa9 : psa10 * 0.35;
        const p8Price = psa8 !== null && psa8 > 0 ? psa8 : psa10 * 0.15;
        const payout = gem * psa10 + p9 * p9Price + p8 * p8Price;
        const cost   = GRADING_FEE + raw;
        const ev     = payout - cost;
        const roi    = ev / cost;
        evForDisplay = ev;
        roiForDisplay = roi;
        // Map ROI to 0..1:  0 ROI → 0.3 (break-even),  100% ROI → 1.0
        //                 -50% ROI → 0,                 50% ROI → 0.65
        if      (roi <= -0.50) gradingValue = 0;
        else if (roi >=  1.00) gradingValue = 1;
        else if (roi <=  0)    gradingValue = 0.30 * (1 + roi / 0.50);  // -0.50→0, 0→0.30
        else                   gradingValue = 0.30 + 0.70 * (roi / 1.00); // 0→0.30, 1.00→1.0
    }

    // ---- Weighted composite (5 dimensions, sum to 100) ----
    const score = Math.round(
        cultural     * 15 +
        demand       * 25 +
        scarcity     * 25 +
        momentum     * 15 +
        gradingValue * 20
    );

    card._mbScore = score;
    card._mbComps = {
        cultural, demand, scarcity, momentum, gradingValue,
        // Sub-components for the breakdown tooltip
        popScarce, supplyTight, priceStable, alignmentPenalty,
        nf7Pct, nf30Pct, dem, sup, pop, satIdx, gemPct,
        ev: evForDisplay, roi: roiForDisplay,
        trajectory,
    };
}

// --- filtering ---

function filterBestGrading() {
    const out = [];
    for (const c of allCards) {
        if (c["is-sealed"]) continue;
        const psa10 = Number(c["psa-10-price"]);
        const raw = Number(c["raw-price"]);
        if (!Number.isFinite(psa10) || psa10 < bgMinPsa10) continue;
        if (!Number.isFinite(raw) || raw < bgMinRaw) continue;
        // Require POSITIVE expected value after fees + sub-grade recovery.
        // Anything EV ≤ 0 is a losing trade regardless of how pretty the
        // raw→PSA10 multiple looks — the grading-fee trap.
        if (!Number.isFinite(c._bgEv) || c._bgEv <= 0) continue;
        out.push(c);
    }
    return out;
}

function filterDemandSurge() {
    const hasProj = Object.keys(modelProjections).length > 0;
    const out = [];
    for (const c of allCards) {
        if (c["is-sealed"]) continue;
        const psa10 = Number(c["psa-10-price"]);
        if (!Number.isFinite(psa10) || psa10 < dsMinPsa10) continue;

        const nfPct = Number(c["net-flow-pct"]);
        if (!Number.isFinite(nfPct) || nfPct < dsMinNfPct) continue;

        const satIdx = Number(c["supply-saturation-index"]);
        if (Number.isFinite(satIdx) && satIdx >= 1) continue;

        const dem = Number(c["demand-pressure"]);
        const sup = Number(c["supply-pressure"]);
        if (!Number.isFinite(dem) || !Number.isFinite(sup) || sup <= 0) continue;
        if (dem / sup < 1.2) continue;

        // Attach projection data
        if (hasProj) {
            const proj = modelProjections[c.id];
            c._projReturn = proj ? proj["projected-return"] : null;
        }
        out.push(c);
    }
    return out;
}

function filterMustBuy() {
    const hasProjections = Object.keys(modelProjections).length > 0;
    const out = [];
    for (const c of allCards) {
        if (!Number.isFinite(c._mbScore)) continue;
        if (c._mbScore < mustBuyMinScore) continue;

        // When model projections are available, boost ranking:
        // attach projection data to card for sorting
        if (hasProjections) {
            const proj = modelProjections[c.id];
            if (proj) {
                c._projReturn = proj["projected-return"];
                c._confLow = proj["confidence-low"];
                c._confHigh = proj["confidence-high"];
                c._confWidth = proj["confidence-width"];
            } else {
                c._projReturn = null;
                c._confLow = null;
                c._confHigh = null;
                c._confWidth = null;
            }
        }
        out.push(c);
    }

    // When model available, re-sort by projected return (model-driven ranking)
    // Cards with positive confidence_low (even pessimistic case is profitable) go first
    if (hasProjections) {
        out.sort((a, b) => {
            const aProj = Number.isFinite(a._projReturn) ? a._projReturn : -Infinity;
            const bProj = Number.isFinite(b._projReturn) ? b._projReturn : -Infinity;
            // Primary: cards where confidence_low > 0 (high conviction) first
            const aConv = (a._confLow != null && a._confLow > 0) ? 1 : 0;
            const bConv = (b._confLow != null && b._confLow > 0) ? 1 : 0;
            if (aConv !== bConv) return bConv - aConv;
            // Secondary: sort by projected return descending
            return bProj - aProj;
        });
    }

    return out;
}

function filterTopChase() {
    const hasProj = Object.keys(modelProjections).length > 0;
    const out = [];
    for (const c of allCards) {
        if (c["is-sealed"]) continue;
        const psa10 = Number(c["psa-10-price"]);
        if (!Number.isFinite(psa10) || psa10 < chaseMinPsa10) continue;
        const nfPct = Number(c["net-flow-pct-30d"]);
        if (Number.isFinite(nfPct) && nfPct < chaseMinDemand) continue;
        // Attach projection data
        if (hasProj) {
            const proj = modelProjections[c.id];
            c._projReturn = proj ? proj["projected-return"] : null;
            c._confLow = proj ? proj["confidence-low"] : null;
            c._confHigh = proj ? proj["confidence-high"] : null;
            c._confWidth = proj ? proj["confidence-width"] : null;
        }
        out.push(c);
    }
    return out;
}

/**
 * Top Chase score — price tier × normalized demand × cultural moat.
 *
 *   price_tier    = sqrt(psa10 / 100)         // $100 → 1.0, $400 → 2.0, $1600 → 4.0
 *   demand_boost  = 1 + clamp(nf30pct, 0, 0.04) × 25    // 1.0 → 2.0 at 4% drain
 *   cultural_boost = 1 + cultural × 0.8                  // 1.0 → 1.8 for iconic
 *
 *   chase_score = price_tier × demand_boost × cultural_boost
 *
 * Real-data calibration (Apr 2026):
 *   Across the catalog, max observed nf30_pct is ~4%, not 50%. Raw
 *   net_flow = avg_new - avg_ended is a daily rate, and dividing by
 *   active_pool gives a fraction that rarely exceeds 0.04 in practice
 *   (a 4% daily pool clearance rate). Clamping at 0.04 and scaling by 25
 *   gives demand a full 1x–2x contribution range at real values, instead
 *   of being dominated by the clamp and contributing ~1.00–1.08.
 */
function computeTopChaseScoreEnriched(card) {
    if (card["is-sealed"]) { card._chaseScore = null; return; }
    const psa10 = Number(card["psa-10-price"]);
    if (!Number.isFinite(psa10) || psa10 <= 0) { card._chaseScore = null; return; }

    const priceTier = Math.sqrt(psa10 / 100);

    const nfPct = Number(card["net-flow-pct-30d"]);
    const nfClamped = Number.isFinite(nfPct) ? Math.max(0, Math.min(0.04, nfPct)) : 0;
    const demandBoost = 1 + nfClamped * 25;  // 0.04 → 2.0

    const culturalBoost = 1 + culturalImpactScore(card) * 0.8;

    card._chaseScore = priceTier * demandBoost * culturalBoost;
}

function filterHolds() {
    const numOrNull = (v) => (v === null || v === undefined || v === "") ? null : Number(v);
    const out = [];
    for (const c of allCards) {
        if (!c._hold) continue;
        const h = c._hold;

        // Type filter
        if (holdType === "sealed" && !c["is-sealed"]) continue;
        if (holdType === "psa10"  && c["is-sealed"])  continue;

        // History depth filter
        const historyDays = Number(c["history-days"]) || 0;
        if (historyDays < minHistoryDays) continue;

        // If user set min history ≥ 90, REQUIRE a genuine 90d anchor. Below
        // that threshold, accept the 30d fallback so we don't hide new cards
        // from users who explicitly opt into shorter windows.
        if (minHistoryDays >= 90 && h.anchorWindow !== "90d") continue;

        // Price floor filter (uses whichever track this card scores on)
        if (h.current < minHoldPrice) continue;

        // Momentum floor — a flat card is not a hold thesis, it's a hope.
        if (!Number.isFinite(h.momentum) || h.momentum < HOLDS_MIN_MOMENTUM) continue;

        // Confirmed-bottom gate — exclude cards still pinned to 1yr low.
        if (!h.confirmedBottom) continue;

        // -- Conviction gates -----------------------------------------------
        // A long-term hold is a months-long thesis, so we need MORE evidence
        // than just "appreciating + discounted from peak". We want cultural
        // floor + bullish sentiment so we're not gambling on a fad.
        const score = Number(h.score);
        if (!Number.isFinite(score) || score < HOLDS_MIN_HOLD_SCORE) continue;

        if (c["is-sealed"]) {
            // Sealed: hold score is the conviction signal. Sealed lacks
            // per-card market_pressure AND cultural name matching (products
            // aren't called "Charizard"), so we enforce a stricter score
            // floor instead. A booster box up 10% over 90d is a real thesis;
            // 1% is noise.
            if (score < HOLDS_MIN_SEALED_SCORE) continue;
        } else {
            // Singles: cultural moat + bullish demand + non-saturated supply.
            const cultural = culturalImpactScore(c);
            if (cultural < HOLDS_MIN_CULTURAL) continue;

            // Demand sentiment: at least one of the net-flow windows must be
            // positive (after API negation, positive nf = inventory absorbed
            // = bullish). We accept either window so a card with a strong 30d
            // accumulation but a noisy 7d still qualifies, and vice versa.
            const nf7  = numOrNull(c["net-flow-7d"]);
            const nf30 = numOrNull(c["net-flow-30d"]);
            const hasBullishFlow =
                (nf30 !== null && nf30 > 0) ||
                (nf7  !== null && nf7  > 0);
            if (!hasBullishFlow) continue;

            // Supply must not be saturated.
            const satIdx = numOrNull(c["supply-saturation-index"]);
            if (satIdx !== null && satIdx >= 1) continue;
        }

        out.push(c);
    }
    return out;
}

// --- sort ---

function getSortValueBestGrading(card, key) {
    switch (key) {
        case "name":   return (card["product-name"] || "").toLowerCase();
        case "set":    return (card["set-code"] || "").toLowerCase();
        case "raw":    return Number(card["raw-price"]) || 0;
        case "psa10":  return Number(card["psa-10-price"]) || 0;
        case "gem":    return Number.isFinite(card._bgGemUsed) ? card._bgGemUsed : -Infinity;
        case "ev":     return Number.isFinite(card._bgEv)  ? card._bgEv  : -Infinity;
        case "roi":    return Number.isFinite(card._bgRoi) ? card._bgRoi : -Infinity;
        case "mult":   return Number.isFinite(card._bgMult) ? card._bgMult : -Infinity;
        default:        return 0;
    }
}

function getSortValueDemandSurge(card, key) {
    switch (key) {
        case "name":     return (card["product-name"] || "").toLowerCase();
        case "set":      return (card["set-code"] || "").toLowerCase();
        case "psa10":    return Number(card["psa-10-price"]) || 0;
        case "proj":     return Number.isFinite(card._projReturn) ? card._projReturn : -Infinity;
        case "demand":   return Number(card["demand-pressure"]) || 0;
        case "supply":   return Number(card["supply-pressure"]) || 0;
        case "ratio":    {
            const d = Number(card["demand-pressure"]);
            const s = Number(card["supply-pressure"]);
            if (!Number.isFinite(d) || !Number.isFinite(s) || s <= 0) return 0;
            return d / s;
        }
        case "nfpct":    return Number.isFinite(Number(card["net-flow-pct"])) ? Number(card["net-flow-pct"]) : -Infinity;
        default:          return 0;
    }
}

function getSortValueMustBuy(card, key) {
    const m = card._mbComps || {};
    switch (key) {
        case "name":     return (card["product-name"] || "").toLowerCase();
        case "set":      return (card["set-code"] || "").toLowerCase();
        case "psa10":    return Number(card["psa-10-price"]) || 0;
        case "proj":     return Number.isFinite(card._projReturn) ? card._projReturn : -Infinity;
        case "conf":     return card._confWidth != null ? -card._confWidth : Infinity; // tighter = higher
        case "cultural": return Number(m.cultural)     || 0;
        case "demand":   return Number(m.demand)       || 0;
        case "scarcity": return Number(m.scarcity)     || 0;
        case "momentum": return Number(m.momentum)     || 0;
        case "grading":  return Number(m.gradingValue) || 0;
        case "mbscore":  return Number.isFinite(card._mbScore) ? card._mbScore : -Infinity;
        default:          return 0;
    }
}

function getSortValueTopChase(card, key) {
    switch (key) {
        case "name":      return (card["product-name"] || "").toLowerCase();
        case "set":       return (card["set-code"] || "").toLowerCase();
        case "psa10":     return Number(card["psa-10-price"]) || 0;
        case "proj":      return Number.isFinite(card._projReturn) ? card._projReturn : -Infinity;
        case "cultural":  return culturalImpactScore(card);
        case "nfpct":     return Number.isFinite(Number(card["net-flow-pct-30d"])) ? Number(card["net-flow-pct-30d"]) : -Infinity;
        case "chasescore":return Number(card._chaseScore) || 0;
        default:           return 0;
    }
}

function getSortValueHold(card, key) {
    const h = card._hold || {};
    switch (key) {
        case "name":     return (card["product-name"] || "").toLowerCase();
        case "set":      return (card["set-code"] || "").toLowerCase();
        case "type":     return card["is-sealed"] ? (card["sealed-type"] || "sealed") : "psa 10";
        case "current":  return Number(h.current)     || 0;
        case "momentum": return Number(h.momentum)    || 0;
        case "discount": return Number(h.peakDiscount)|| 0;
        case "peak":     return Number(h.peak)        || 0;
        case "anchor":   return h.anchorWindow === "90d" ? 1 : 0;  // 90d beats 30d
        case "cultural": return card["is-sealed"] ? 0 : culturalImpactScore(card);
        case "score":    return Number.isFinite(h.score) ? h.score : -Infinity;
        default:         return 0;
    }
}

function sortList(list) {
    const { key, dir } = currentSort;
    const getter =
        view === "holds"       ? getSortValueHold :
        view === "mustbuy"     ? getSortValueMustBuy :
        view === "topchase"    ? getSortValueTopChase :
        view === "demandsurge" ? getSortValueDemandSurge :
        view === "bestgrading" ? getSortValueBestGrading :
                                  getSortValueMustBuy;  // safe fallback
    list.sort((a, b) => {
        const av = getter(a, key);
        const bv = getter(b, key);
        if (typeof av === "string" && typeof bv === "string") {
            return dir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
        }
        return dir === "asc" ? av - bv : bv - av;
    });
}

// --- table header rendering (per-view) ---

const HEADERS = {
    holds: [
        { key: "rank",     label: "#",         width: 50 },
        { key: "none",     label: "IMAGE",     width: 60 },
        { key: "name",     label: "CARD / PRODUCT" },
        { key: "set",      label: "SET" },
        { key: "type",     label: "TYPE" },
        { key: "current",  label: "CURRENT" },
        { key: "peak",     label: "12MO PEAK" },
        { key: "anchor",   label: "DATA" },
        { key: "cultural", label: "CULTURE" },
        { key: "momentum", label: "APPREC \u0394" },
        { key: "discount", label: "OFF PEAK" },
        { key: "score",    label: "HOLD SCORE" },
    ],
    mustbuy: [
        { key: "rank",     label: "#",         width: 50 },
        { key: "none",     label: "IMAGE",     width: 60 },
        { key: "name",     label: "CARD NAME" },
        { key: "set",      label: "SET" },
        { key: "psa10",    label: "PSA 10" },
        { key: "proj",     label: "90D PROJ" },
        { key: "conf",     label: "CONFIDENCE" },
        { key: "cultural", label: "CULTURE" },
        { key: "demand",   label: "DEMAND" },
        { key: "scarcity", label: "SCARCITY" },
        { key: "mbscore",  label: "SCORE" },
    ],
    topchase: [
        { key: "rank",       label: "#",          width: 50 },
        { key: "none",       label: "IMAGE",      width: 60 },
        { key: "name",       label: "CARD NAME" },
        { key: "set",        label: "SET" },
        { key: "psa10",      label: "PSA 10" },
        { key: "proj",       label: "90D PROJ" },
        { key: "cultural",   label: "CULTURE" },
        { key: "nfpct",      label: "NET FLOW 30D %" },
        { key: "chasescore", label: "CHASE" },
    ],
    demandsurge: [
        { key: "rank",    label: "#",         width: 50 },
        { key: "none",    label: "IMAGE",     width: 60 },
        { key: "name",    label: "CARD NAME" },
        { key: "set",     label: "SET" },
        { key: "psa10",   label: "PSA 10" },
        { key: "proj",    label: "90D PROJ" },
        { key: "demand",  label: "DEMAND" },
        { key: "supply",  label: "SUPPLY" },
        { key: "ratio",   label: "D/S RATIO" },
        { key: "nfpct",   label: "NET FLOW %" },
    ],
    bestgrading: [
        { key: "rank",   label: "#",         width: 50 },
        { key: "none",   label: "IMAGE",     width: 60 },
        { key: "name",   label: "CARD NAME" },
        { key: "set",    label: "SET" },
        { key: "raw",    label: "RAW" },
        { key: "psa10",  label: "PSA 10" },
        { key: "gem",    label: "GEM %" },
        { key: "ev",     label: "EV $" },
        { key: "roi",    label: "ROI %" },
        { key: "mult",   label: "PAYOUT \u00d7" },
    ],
};

function renderThead() {
    const thead = document.getElementById("card-thead");
    const cols =
        view === "holds"       ? HEADERS.holds :
        view === "mustbuy"     ? HEADERS.mustbuy :
        view === "topchase"    ? HEADERS.topchase :
        view === "demandsurge" ? HEADERS.demandsurge :
        view === "bestgrading" ? HEADERS.bestgrading :
                                  HEADERS.mustbuy;  // safe fallback
    const tr = document.createElement("tr");
    for (const col of cols) {
        const th = document.createElement("th");
        th.dataset.sort = col.key;
        if (col.width) th.style.width = `${col.width}px`;
        th.textContent = col.label;
        // Active sort indicator
        if (col.key === currentSort.key) {
            th.textContent += currentSort.dir === "asc" ? " \u25B2" : " \u25BC";
        }
        tr.appendChild(th);
    }
    thead.innerHTML = "";
    thead.appendChild(tr);
}

// --- row rendering (per-view) ---

function renderRowsBestGrading(list, start, count) {
    const tbody = document.getElementById("card-tbody");
    if (start === 0) tbody.innerHTML = "";
    const end = Math.min(start + count, list.length);
    for (let i = start; i < end; i++) {
        const c = list[i];
        const imgUrl = esc(c["image-url"] || "");
        const cardId = c.id || "";
        const setCode = esc(c["set-code"] || "");
        const name    = esc(c["product-name"] || "\u2014");
        const rawPrice = Number(c["raw-price"]) || 0;
        const psa10Price = Number(c["psa-10-price"]) || 0;
        const ev = c._bgEv;
        const roi = c._bgRoi;
        const mult = c._bgMult;
        const gem = c._bgGemUsed;
        const gemReal = c._bgGemReal;

        const tr = document.createElement("tr");
        tr.className = "rowLink";
        tr.dataset.href = `/card.html?id=${encodeURIComponent(cardId)}`;
        tr.onclick = function(e) {
            if (!e.target.closest("a")) window.location = this.dataset.href;
        };

        const evStr  = Number.isFinite(ev)  ? (ev >= 0 ? "+" : "") + money(ev) : "\u2014";
        const roiStr = Number.isFinite(roi) ? (roi >= 0 ? "+" : "") + (roi * 100).toFixed(0) + "%" : "\u2014";
        const multStr = Number.isFinite(mult) ? mult.toFixed(2) + "\u00d7" : "\u2014";
        // Gem % shown with a "~" prefix + dim color when it's the default
        // fallback, so the user can see which rows have real PSA pop data.
        const gemStr = Number.isFinite(gem)
            ? (gemReal ? "" : "~") + (gem * 100).toFixed(0) + "%"
            : "\u2014";
        const gemCls = gemReal ? "text-mono" : "text-mono gem-assumed";
        // EV tier: tier chip scales with ROI since that's what matters per-$
        const tierCls =
            (Number.isFinite(roi) && roi >= 1.00) ? "mb-chip tier-strong" :  // ≥+100% ROI
            (Number.isFinite(roi) && roi >= 0.30) ? "mb-chip tier-solid"  :  // ≥+30% ROI
                                                    "mb-chip tier-weak";

        tr.innerHTML = `
            <td class="text-center"><span class="${rankClass(i + 1)}">${i + 1}</span></td>
            <td>${imgUrl ? `<img src="${imgUrl}" alt="" style="width:50px;height:70px;object-fit:contain;image-rendering:auto;" loading="lazy">` : "\u2014"}</td>
            <td>${name}</td>
            <td>${setCode}</td>
            <td class="text-right text-mono">${money(rawPrice)}</td>
            <td class="text-right text-mono">${money(psa10Price)}</td>
            <td class="text-right ${gemCls}">${gemStr}</td>
            <td class="text-right text-mono">${evStr}</td>
            <td class="text-right"><span class="${tierCls}">${roiStr}</span></td>
            <td class="text-right text-mono">${multStr}</td>
        `;
        tbody.appendChild(tr);
    }
    displayedCount = end;
    updateLoadMore(list);
}

function renderRowsDemandSurge(list, start, count) {
    const tbody = document.getElementById("card-tbody");
    if (start === 0) tbody.innerHTML = "";
    const end = Math.min(start + count, list.length);
    for (let i = start; i < end; i++) {
        const c = list[i];
        const imgUrl = esc(c["image-url"] || "");
        const cardId = c.id || "";
        const setCode = esc(c["set-code"] || "");
        const name    = esc(c["product-name"] || "\u2014");
        const psa10Price = Number(c["psa-10-price"]) || 0;
        const demand = Number(c["demand-pressure"]);
        const supply = Number(c["supply-pressure"]);
        const ratio = (Number.isFinite(demand) && Number.isFinite(supply) && supply > 0) ? demand / supply : null;
        const nfPct = Number(c["net-flow-pct"]);
        const { projHtml: dsProjHtml } = projChipHtml(cardId);

        const tr = document.createElement("tr");
        tr.className = "rowLink";
        tr.dataset.href = `/card.html?id=${encodeURIComponent(cardId)}`;
        tr.onclick = function(e) {
            if (!e.target.closest("a")) window.location = this.dataset.href;
        };

        const demandStr = Number.isFinite(demand) ? demand.toFixed(3) : "\u2014";
        const supplyStr = Number.isFinite(supply) ? supply.toFixed(3) : "\u2014";
        const ratioStr = Number.isFinite(ratio) ? ratio.toFixed(2) + "\u00d7" : "\u2014";
        const nfPctStr = Number.isFinite(nfPct)
            ? (nfPct >= 0 ? "+" : "") + (nfPct * 100).toFixed(1) + "%"
            : "\u2014";
        const nfCls = Number.isFinite(nfPct) && nfPct > 0.02 ? "chip chip-pos"
                    : Number.isFinite(nfPct) && nfPct < -0.02 ? "chip chip-neg"
                    : "chip chip-neu";
        const ratioCls =
            (Number.isFinite(ratio) && ratio >= 3) ? "mb-chip tier-strong" :
            (Number.isFinite(ratio) && ratio >= 2) ? "mb-chip tier-solid"  :
                                                     "mb-chip tier-weak";

        // Note: all values are escaped or numeric. innerHTML pattern matches existing codebase.
        tr.innerHTML = `
            <td class="text-center"><span class="${esc(rankClass(i + 1))}">${i + 1}</span></td>
            <td>${imgUrl ? `<img src="${imgUrl}" alt="" style="width:50px;height:70px;object-fit:contain;image-rendering:auto;" loading="lazy">` : "\u2014"}</td>
            <td>${name}</td>
            <td>${setCode}</td>
            <td class="text-right text-mono">${money(psa10Price)}</td>
            <td class="text-right">${dsProjHtml}</td>
            <td class="text-right text-mono">${demandStr}</td>
            <td class="text-right text-mono">${supplyStr}</td>
            <td class="text-right"><span class="${esc(ratioCls)}">${ratioStr}</span></td>
            <td class="text-right"><span class="${esc(nfCls)}">${nfPctStr}</span></td>
        `;
        tbody.appendChild(tr);
    }
    displayedCount = end;
    updateLoadMore(list);
}

/**
 * Build a per-card breakdown string for the Must Buy Now score, exposed via
 * the .mb-chip's `data-tip` attribute. Hovering shows exactly which signals
 * contributed how many points so the score is fully transparent.
 *
 * Reads c._mbComps which was populated by computeMustBuyScore().
 */
function renderRowsTopChase(list, start, count) {
    const tbody = document.getElementById("card-tbody");
    if (start === 0) tbody.innerHTML = "";
    const end = Math.min(start + count, list.length);
    for (let i = start; i < end; i++) {
        const c = list[i];
        const imgUrl = esc(c["image-url"] || "");
        const cardId = c.id || "";
        const setCode = esc(c["set-code"] || "");
        const name    = esc(c["product-name"] || "\u2014");
        const psa10 = Number(c["psa-10-price"]) || 0;
        const cultural = culturalImpactScore(c);
        const nfPct = Number(c["net-flow-pct-30d"]);
        const score = Number(c._chaseScore);
        const { projHtml: tcProjHtml } = projChipHtml(cardId);

        const tr = document.createElement("tr");
        tr.className = "rowLink";
        tr.dataset.href = `/card.html?id=${encodeURIComponent(cardId)}`;
        tr.onclick = function(e) {
            if (!e.target.closest("a")) window.location = this.dataset.href;
        };

        const culturalStr = (cultural * 100).toFixed(0) + "%";
        const culturalCls =
            cultural >= 0.75 ? "mb-chip tier-strong" :
            cultural >= 0.45 ? "mb-chip tier-solid"  :
                               "mb-chip tier-weak";
        const nfStr = Number.isFinite(nfPct)
            ? (nfPct >= 0 ? "+" : "") + (nfPct * 100).toFixed(1) + "%"
            : "\u2014";
        const nfCls = Number.isFinite(nfPct) && nfPct > 0.05 ? "chip chip-pos" :
                     Number.isFinite(nfPct) && nfPct > 0     ? "chip chip-neu" :
                                                               "chip chip-neg";
        const tierCls =
            Number.isFinite(score) && score >= 8 ? "mb-chip tier-strong" :
            Number.isFinite(score) && score >= 4 ? "mb-chip tier-solid"  :
                                                    "mb-chip tier-weak";
        const scoreStr = Number.isFinite(score) ? score.toFixed(2) : "\u2014";

        // Note: all values are escaped or numeric. innerHTML pattern matches existing codebase.
        tr.innerHTML = `
            <td class="text-center"><span class="${esc(rankClass(i + 1))}">${i + 1}</span></td>
            <td>${imgUrl ? `<img src="${imgUrl}" alt="" style="width:50px;height:70px;object-fit:contain;image-rendering:auto;" loading="lazy">` : "\u2014"}</td>
            <td>${name}</td>
            <td>${setCode}</td>
            <td class="text-right text-mono">${money(psa10)}</td>
            <td class="text-right">${tcProjHtml}</td>
            <td class="text-right"><span class="${esc(culturalCls)}">${culturalStr}</span></td>
            <td class="text-right"><span class="${esc(nfCls)}">${nfStr}</span></td>
            <td class="text-right"><span class="${esc(tierCls)}">${scoreStr}</span></td>
        `;
        tbody.appendChild(tr);
    }
    displayedCount = end;
    updateLoadMore(list);
}

/**
 * Hover-tooltip breakdown of the smart-investor Must Buy score.
 * Shows each of the five contributing dimensions with raw values + points.
 * Real Scarcity is exploded into its 3 sub-components so you can see why
 * a card with rare PSA pop might still get a low scarcity score.
 */
function buildMustBuyBreakdown(c) {
    const m = c._mbComps;
    if (!m) return "";
    const score = c._mbScore;
    const pts = (v, max) => `${(v * max).toFixed(0).padStart(2)} / ${max}`;
    const pct = (v) => Number.isFinite(v) ? `${(v * 100).toFixed(0)}%` : "—";
    const pctSigned = (v) => Number.isFinite(v) ? (v >= 0 ? "+" : "") + (v * 100).toFixed(1) + "%" : "—";
    const trajLabel =
        m.trajectory === "rising"       ? "↑ rising"       :
        m.trajectory === "rebound"      ? "↻ rebound"      :
        m.trajectory === "flat"         ? "→ flat"         :
        m.trajectory === "falling"      ? "↓ falling"      :
        m.trajectory === "thin-rising"  ? "↑ thin rising"  :
        m.trajectory === "thin-falling" ? "↓ thin falling" :
        m.trajectory === "thin-flat"    ? "→ thin flat"    : "—";
    const popStr   = m.pop != null && m.pop > 0 ? String(Math.round(m.pop)) : "—";
    const satStr   = Number.isFinite(m.satIdx) ? m.satIdx.toFixed(2) : "—";
    const alignNote = m.alignmentPenalty < 1 ? `  (×${m.alignmentPenalty.toFixed(2)} misalign penalty)` : "";
    const evStr    = Number.isFinite(m.ev) ? (m.ev >= 0 ? "+$" : "-$") + Math.abs(m.ev).toFixed(0) : "—";
    const roiStr   = Number.isFinite(m.roi) ? (m.roi >= 0 ? "+" : "") + (m.roi * 100).toFixed(0) + "%" : "—";
    return [
        `SMART INVESTOR SCORE  ${score} / 100`,
        ``,
        `Cultural impact    ${pct(m.cultural).padStart(4)}   →  ${pts(m.cultural, 15)} pts`,
        ``,
        `Demand momentum    7d=${pctSigned(m.nf7Pct)} / 30d=${pctSigned(m.nf30Pct)}`,
        `                            →  ${pts(m.demand, 25)} pts`,
        ``,
        `Real Scarcity (3 INDEPENDENT signals):`,
        `  PSA 10 pop       ${popStr.padStart(4)}      →  ${pct(m.popScarce)}`,
        `  Supply tightness sat=${satStr}   →  ${pct(m.supplyTight)}`,
        `  Price stability  12mo spread → ${pct(m.priceStable)}`,
        `                            →  ${pts(m.scarcity, 25)} pts${alignNote}`,
        ``,
        `PSA 10 trajectory  ${trajLabel.padStart(14)}`,
        `                            →  ${pts(m.momentum, 15)} pts`,
        ``,
        `Grading value      EV ${evStr}  ROI ${roiStr}  gem ${pct(m.gemPct)}`,
        `                            →  ${pts(m.gradingValue, 20)} pts`,
        ``,
        `100 = perfect alignment across all 5 dimensions.`,
    ].join("\n");
}

function projChipHtml(cardId) {
    // Returns {projHtml, confHtml} for model projection display.
    // All values are pre-escaped numbers, safe for innerHTML (existing pattern).
    const proj = modelProjections[cardId];
    if (!proj) return { projHtml: "\u2014", confHtml: "\u2014" };

    const projReturn = proj["projected-return"];
    const confLow = proj["confidence-low"];
    const confHigh = proj["confidence-high"];
    const confWidth = proj["confidence-width"];

    let projHtml = "\u2014";
    let confHtml = "\u2014";

    if (projReturn !== null && Number.isFinite(projReturn)) {
        const projPct = (projReturn * 100).toFixed(1);
        const projSign = projReturn > 0 ? "+" : "";
        const projCls = projReturn > 0.05 ? "ev-chip pos"
                      : projReturn < -0.05 ? "ev-chip neg"
                      : "ev-chip zero";
        projHtml = `<span class="${esc(projCls)}">${esc(projSign + projPct)}%</span>`;

        if (confLow !== null && confHigh !== null) {
            const lo = (confLow * 100).toFixed(0);
            const hi = (confHigh * 100).toFixed(0);
            const w = confWidth || (confHigh - confLow);
            const confCls = w < 0.15 ? "mb-chip tier-strong"
                          : w < 0.30 ? "mb-chip tier-solid"
                          : "mb-chip tier-weak";
            const confLabel = w < 0.15 ? "HIGH" : w < 0.30 ? "MED" : "LOW";
            confHtml = `<span class="${esc(confCls)}" data-tip="${esc(lo)}% to ${esc(hi)}%">${esc(confLabel)}</span>`;
        }
    }
    return { projHtml, confHtml };
}

function renderRowsMustBuy(list, start, count) {
    const tbody = document.getElementById("card-tbody");
    if (start === 0) tbody.innerHTML = "";
    const end = Math.min(start + count, list.length);
    for (let i = start; i < end; i++) {
        const c = list[i];
        const m = c._mbComps || {};
        const imgUrl = esc(c["image-url"] || "");
        const cardId = c.id || "";
        const setCode = esc(c["set-code"] || "");
        const name    = esc(c["product-name"] || "\u2014");
        const psa10 = Number(c["psa-10-price"]) || 0;
        const score = Number(c._mbScore);

        const { projHtml, confHtml } = projChipHtml(cardId);

        const tr = document.createElement("tr");
        tr.className = "rowLink";
        tr.dataset.href = `/card.html?id=${encodeURIComponent(cardId)}`;
        tr.onclick = function(e) {
            if (!e.target.closest("a")) window.location = this.dataset.href;
        };

        // Each component as a 0-100 bar chip — visually scannable
        const dimChip = (val, label) => {
            const pct = Math.round((val || 0) * 100);
            const cls = pct >= 75 ? "mb-chip tier-strong"
                      : pct >= 45 ? "mb-chip tier-solid"
                                  : "mb-chip tier-weak";
            return `<span class="${esc(cls)}" style="min-width:42px;">${pct}</span>`;
        };

        // Final composite score chip
        const tierCls =
            score >= 75 ? "mb-chip tier-strong" :
            score >= 60 ? "mb-chip tier-solid"  :
                          "mb-chip tier-weak";
        const scoreStr = Number.isFinite(score) ? String(score) : "\u2014";
        const breakdown = esc(buildMustBuyBreakdown(c));

        // Note: all dynamic values are escaped via esc() above.
        // This innerHTML pattern is used throughout the existing codebase.
        tr.innerHTML = `
            <td class="text-center"><span class="${esc(rankClass(i + 1))}">${i + 1}</span></td>
            <td>${imgUrl ? `<img src="${imgUrl}" alt="" style="width:50px;height:70px;object-fit:contain;image-rendering:auto;" loading="lazy">` : "\u2014"}</td>
            <td>${name}</td>
            <td>${setCode}</td>
            <td class="text-right text-mono">${money(psa10)}</td>
            <td class="text-right">${projHtml}</td>
            <td class="text-center">${confHtml}</td>
            <td class="text-right">${dimChip(m.cultural, "C")}</td>
            <td class="text-right">${dimChip(m.demand, "D")}</td>
            <td class="text-right">${dimChip(m.scarcity, "S")}</td>
            <td class="text-right"><span class="${esc(tierCls)}" data-tip="${breakdown}">${scoreStr}</span></td>
        `;
        tbody.appendChild(tr);
    }
    displayedCount = end;
    updateLoadMore(list);
}

function renderRowsHolds(list, start, count) {
    const tbody = document.getElementById("card-tbody");
    if (start === 0) tbody.innerHTML = "";
    const end = Math.min(start + count, list.length);
    for (let i = start; i < end; i++) {
        const c = list[i];
        const h = c._hold;
        const imgUrl = esc(c["image-url"] || "");
        const cardId = c.id || "";
        const setCode = esc(c["set-code"] || "");
        const name    = esc(c["product-name"] || "\u2014");
        const typeStr = c["is-sealed"]
            ? esc(c["sealed-type"] || "Sealed")
            : "PSA 10";

        const tr = document.createElement("tr");
        tr.className = "rowLink";
        tr.dataset.href = `/card.html?id=${encodeURIComponent(cardId)}`;
        tr.onclick = function(e) {
            if (!e.target.closest("a")) window.location = this.dataset.href;
        };

        const score = h.score;
        // Score is a unitless composite, NOT a return percentage. Display as
        // a bare number with 3 decimals so users don't misread it as "+35%"
        // expected return.
        const scoreStr = Number.isFinite(score)
            ? (score >= 0 ? "+" : "") + score.toFixed(3)
            : "\u2014";

        const momentumStr = pctSigned(h.momentum, 1);
        const discountStr = pctSigned(h.peakDiscount, 1);

        // Anchor window badge — shows whether the score was computed on real
        // 90d data or a 30d fallback (thin data, less conviction).
        const anchorBadge = h.anchorWindow === "90d"
            ? `<span class="anchor-badge anchor-90d">90d</span>`
            : `<span class="anchor-badge anchor-30d" title="Computed on 30d fallback — limited history">30d</span>`;

        // Cultural chip for singles (sealed has no Pokemon-name cultural).
        const cultural = c["is-sealed"] ? null : culturalImpactScore(c);
        const culturalStr = cultural === null
            ? "\u2014"
            : (cultural * 100).toFixed(0) + "%";
        const culturalCls = cultural === null ? "mb-chip tier-weak"
            : cultural >= 0.75 ? "mb-chip tier-strong"
            : cultural >= 0.45 ? "mb-chip tier-solid"
                               : "mb-chip tier-weak";

        tr.innerHTML = `
            <td class="text-center"><span class="${rankClass(i + 1)}">${i + 1}</span></td>
            <td>${imgUrl ? `<img src="${imgUrl}" alt="" style="width:50px;height:70px;object-fit:contain;image-rendering:auto;" loading="lazy">` : "\u2014"}</td>
            <td>${name}</td>
            <td>${setCode}</td>
            <td><span style="font-size:10px;color:#404040;">${typeStr}</span></td>
            <td class="text-right text-mono">${money(h.current)}</td>
            <td class="text-right text-mono" style="color:#606060;">${money(h.peak)}</td>
            <td class="text-center">${anchorBadge}</td>
            <td class="text-right"><span class="${culturalCls}">${culturalStr}</span></td>
            <td class="text-right"><span class="${evChipClass(h.momentum)}">${momentumStr}</span></td>
            <td class="text-right"><span class="${evChipClass(h.peakDiscount)}">${discountStr}</span></td>
            <td class="text-right"><span class="${evChipClass(score)}">${scoreStr}</span></td>
        `;
        tbody.appendChild(tr);
    }
    displayedCount = end;
    updateLoadMore(list);
}

function updateLoadMore(list) {
    const loadMoreBtn = document.getElementById("load-more-btn");
    loadMoreBtn.style.display = displayedCount < list.length ? "inline-block" : "none";

    // Coverage transparency: Must Buy Now, Demand Surge, and Top Chase all
    // require market-pressure data which only exists for a subset of cards
    // (~17% of catalog as of Apr 2026). Show the pool size the view is
    // actually searching over, not just the total catalog, so users don't
    // think "only 60 results" means the tool is broken.
    const marketPoolSize = allCards.filter(c =>
        !c["is-sealed"] &&
        c["net-flow-pct"] !== null && c["net-flow-pct"] !== undefined
    ).length;

    const needsMarketData = (view === "mustbuy" || view === "demandsurge" || view === "topchase");
    const poolNote = needsMarketData
        ? ` (of ${marketPoolSize} with market data)`
        : "";

    document.getElementById("status-count").textContent =
        `${displayedCount} of ${list.length} cards${poolNote}`;
}

function renderRows(list, start, count) {
    if (list.length === 0) {
        const tbody = document.getElementById("card-tbody");
        const cols =
            view === "holds"       ? HEADERS.holds :
            view === "mustbuy"     ? HEADERS.mustbuy :
            view === "demandsurge" ? HEADERS.demandsurge :
            view === "bestgrading" ? HEADERS.bestgrading :
                                      HEADERS.mustbuy;
        const needsMarketData = (view === "mustbuy" || view === "demandsurge" || view === "topchase");
        const hint = needsMarketData
            ? " This view requires eBay market-pressure data, which currently covers ~17% of the catalog — try loosening a threshold or check the Long-Term Holds / Best Grading tabs which don't need market data."
            : " Try loosening the price floor or relaxing a filter.";
        tbody.innerHTML = `<tr><td colspan="${cols.length}" style="text-align:center;color:#808080;padding:16px;">No cards match the current filters.${hint}</td></tr>`;
        displayedCount = 0;
        document.getElementById("load-more-btn").style.display = "none";
        document.getElementById("status-count").textContent = `0 cards`;
        return;
    }
    if (view === "holds")              renderRowsHolds(list, start, count);
    else if (view === "mustbuy")       renderRowsMustBuy(list, start, count);
    else if (view === "topchase")      renderRowsTopChase(list, start, count);
    else if (view === "demandsurge")   renderRowsDemandSurge(list, start, count);
    else if (view === "bestgrading")   renderRowsBestGrading(list, start, count);
    else                                renderRowsMustBuy(list, start, count);
}

// --- scatter map chart ---
let distChart = null;
// Register the datalabels plugin once when it loads.
let _datalabelsRegistered = false;
function _ensureDatalabelsRegistered() {
    if (_datalabelsRegistered) return;
    if (window.Chart && window.ChartDataLabels) {
        Chart.register(window.ChartDataLabels);
        // Don't enable datalabels on every dataset by default — we turn it
        // on explicitly for the "chase cards" dataset below.
        Chart.defaults.plugins.datalabels = { display: false };
        _datalabelsRegistered = true;
    }
}

/**
 * Render (or update) a scatter map of the current filtered list.
 *
 *   X = current price (log scale) — PSA 10 price for singles, raw for sealed
 *   Y = the relevant score for the current view:
 *        * EV views (undervalued/overvalued/all) : grading EV in dollars
 *        * Long-Term Holds view                  : hold score in %
 *
 * Every card is plotted as a small dot, colored green (positive score) or
 * red (negative). The top 8 "chase cards" — ranked by PSA 10 price in the
 * EV views, or by hold score in the Holds view — get larger markers and a
 * text label so you can see where the marquee names fall on the map.
 */
function renderDistributionChart(list) {
    const canvas = document.getElementById("dist-chart");
    if (!canvas || !window.Chart) return;
    _ensureDatalabelsRegistered();

    const isHolds       = (view === "holds");
    const isMustBuy     = (view === "mustbuy");
    const isTopChase    = (view === "topchase");
    const isDemandSurge = (view === "demandsurge");
    const isBestGrading = (view === "bestgrading");

    // -------- Build the primary scatter dataset --------
    const allPoints = [];  // background cloud (small dots, no label)
    for (const c of list) {
        let x, y;
        if (isHolds) {
            x = Number(c._hold?.current);
            y = Number.isFinite(c._hold?.score) ? c._hold.score * 100 : null;
        } else if (isMustBuy) {
            // Y = the full Must Buy Now composite score (0-100), since that's
            // the actual thing the user is sorting on in the table.
            x = Number(c["psa-10-price"]);
            y = Number.isFinite(c._mbScore) ? c._mbScore : null;
        } else if (isTopChase) {
            x = Number(c["psa-10-price"]);
            const nfPct = Number(c["net-flow-pct-30d"]);
            y = Number.isFinite(nfPct) ? nfPct * 100 : null;
        } else if (isDemandSurge) {
            x = Number(c["psa-10-price"]);
            // Y = normalized net flow %, scaled to 0-100 for readability
            const nfPct = Number(c["net-flow-pct"]);
            y = Number.isFinite(nfPct) ? nfPct * 100 : null;
        } else if (isBestGrading) {
            x = Number(c["psa-10-price"]);
            // Y = ROI %, the clearest "is this worth it?" signal
            y = Number.isFinite(c._bgRoi) ? c._bgRoi * 100 : null;
        } else {
            x = Number(c["psa-10-price"]);
            y = Number(c["net-flow-30d"]);
        }
        if (!Number.isFinite(x) || x <= 0) continue;
        if (y == null || !Number.isFinite(y)) continue;
        allPoints.push({ x, y, card: c });
    }

    // Reference threshold (zero-line / tier line) per view.
    // Must Buy Y = composite score (60 is the "strong conviction" line),
    // Top Chase / Demand Surge Y = net flow % (0 is the bullish/bearish split),
    // Best Grading Y = ROI % (0 is break-even after fees),
    // Holds Y = hold score as % (0 is no-go).
    const posThreshold = isMustBuy ? 60 : 0;
    const posPoints = allPoints.filter(p => p.y >  posThreshold);
    const negPoints = allPoints.filter(p => p.y <  posThreshold);

    // -------- Pick "chase cards" to label --------
    // Top 8 by score for all four views.
    const chaseSource = allPoints.slice();
    chaseSource.sort((a, b) => b.y - a.y);
    const CHASE_N = 8;
    const chasePts = chaseSource.slice(0, CHASE_N).map(p => ({
        ...p,
        // Shorten labels to first ~18 chars for readability on the chart
        _label: (p.card["product-name"] || "").slice(0, 22),
    }));

    // Chase points get drawn in their own dataset so datalabels / marker
    // sizing can be scoped to just them.
    const chaseKeys = new Set(chasePts.map(p => p.card.id));
    const backgroundPos = posPoints.filter(p => !chaseKeys.has(p.card.id));
    const backgroundNeg = negPoints.filter(p => !chaseKeys.has(p.card.id));

    // -------- Headline numbers for the subtitle --------
    const posCount = posPoints.length;
    const negCount = negPoints.length;
    const topPos = allPoints.reduce((max, p) => p.y > (max?.y ?? -Infinity) ? p : max, null);
    const topNeg = allPoints.reduce((min, p) => p.y < (min?.y ??  Infinity) ? p : min, null);

    const formatY = (v) => {
        if (isHolds)                    return (v >= 0 ? "+" : "") + Number(v).toFixed(1) + "%";
        if (isBestGrading)              return (v >= 0 ? "+" : "") + Number(v).toFixed(0) + "%";
        if (isMustBuy)                  return Number(v).toFixed(0);               // composite score
        if (isDemandSurge || isTopChase) return (v >= 0 ? "+" : "") + Number(v).toFixed(1) + "%"; // nf %
        return Number(v).toFixed(0);
    };
    const formatX = (v) => "$" + Number(v).toLocaleString("en-US", { maximumFractionDigits: 0 });

    const titleText = isHolds        ? "Long-term hold map"
                    : isMustBuy      ? "Must Buy Now \u2014 sustained-demand map"
                    : isTopChase     ? "Top Chase \u2014 high-PSA-10 demand map"
                    : isDemandSurge  ? "Demand Surge \u2014 net flow map"
                    : isBestGrading  ? "Best Grading Play \u2014 % uplift map"
                                     : "Market map";
    const subText = isHolds
        ? `${allPoints.length} cards • top: ${topPos ? formatY(topPos.y) : "\u2014"} • X = current price (log), Y = hold score • labeled dots = top ${CHASE_N} picks`
        : isMustBuy ? `${allPoints.length} cards passing all gates • top composite: ${topPos ? formatY(topPos.y) : "\u2014"} • X = PSA 10 price (log), Y = Must Buy score • labeled dots = top ${CHASE_N} picks`
        : isTopChase ? `${allPoints.length} chase cards • top normalized flow: ${topPos ? formatY(topPos.y) : "\u2014"} • X = PSA 10 price (log), Y = 30d net flow % • labeled dots = top ${CHASE_N} picks`
        : isDemandSurge ? `${allPoints.length} cards in surge • top: ${topPos ? formatY(topPos.y) : "\u2014"} normalized flow • X = PSA 10 price (log), Y = net flow % • labeled dots = top ${CHASE_N} picks`
        : isBestGrading ? `${allPoints.length} cards with EV > 0 • top ROI: ${topPos ? formatY(topPos.y) : "\u2014"} • X = PSA 10 price (log), Y = ROI % (after fees + sub-grade recovery) • labeled dots = top ${CHASE_N} picks`
        : `${allPoints.length} cards`;

    document.getElementById("dist-chart-title").textContent = titleText;
    document.getElementById("dist-chart-sub").textContent = subText;

    const data = {
        datasets: [
            {
                label: "+EV",
                data: backgroundPos,
                backgroundColor: "rgba(0, 128, 0, 0.55)",
                borderColor: "rgba(0, 96, 0, 0.9)",
                borderWidth: 0.5,
                pointRadius: 3,
                pointHoverRadius: 6,
            },
            {
                label: "-EV",
                data: backgroundNeg,
                backgroundColor: "rgba(200, 0, 0, 0.55)",
                borderColor: "rgba(120, 0, 0, 0.9)",
                borderWidth: 0.5,
                pointRadius: 3,
                pointHoverRadius: 6,
            },
            {
                label: "Chase cards",
                data: chasePts,
                backgroundColor: chasePts.map(p => p.y >= 0 ? "rgba(0, 128, 0, 0.9)" : "rgba(200, 0, 0, 0.9)"),
                borderColor: "#000000",
                borderWidth: 1,
                pointRadius: 7,
                pointHoverRadius: 10,
                // Enable datalabels only for this dataset
                datalabels: {
                    display: true,
                    color: "#000000",
                    backgroundColor: "rgba(255, 255, 225, 0.9)",
                    borderColor: "#000",
                    borderWidth: 1,
                    borderRadius: 2,
                    padding: { top: 2, bottom: 2, left: 4, right: 4 },
                    font: { size: 9, family: "Tahoma, sans-serif", weight: "bold" },
                    align: "top",
                    anchor: "end",
                    offset: 6,
                    formatter: (value) => value._label || "",
                    clamp: true,
                },
            },
            // Invisible zero-line dataset (draws a horizontal reference)
            // implemented via a thin line on scale.y grid instead — see grid config below.
        ],
    };

    const options = {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: {
            legend: { display: false },
            tooltip: {
                callbacks: {
                    title: (items) => {
                        const p = items[0]?.raw;
                        if (!p || !p.card) return "";
                        return p.card["product-name"] || "";
                    },
                    label: (item) => {
                        const p = item.raw;
                        if (!p) return "";
                        const yLabel = isHolds       ? "Hold score"
                                      : isMustBuy     ? "Must Buy score"
                                      : isTopChase    ? "30d net flow %"
                                      : isDemandSurge ? "Net flow %"
                                      : isBestGrading ? "ROI %"
                                      : "Score";
                        return [
                            `${isHolds ? "Current" : "PSA 10"}: ${formatX(p.x)}`,
                            `${yLabel}: ${formatY(p.y)}`,
                            p.card["set-code"] ? `Set: ${p.card["set-code"]}` : "",
                        ].filter(Boolean);
                    },
                },
            },
        },
        scales: {
            x: {
                type: "logarithmic",
                title: {
                    display: true,
                    text: isHolds ? "Current price (log scale)" : "PSA 10 price (log scale)",
                    color: "#606060",
                    font: { size: 10 },
                },
                ticks: {
                    color: "#606060",
                    font: { size: 9 },
                    callback: (v) => {
                        // Only label "nice" log ticks: 10, 100, 1k, 10k...
                        const n = Number(v);
                        if (!Number.isFinite(n)) return "";
                        const log = Math.log10(n);
                        if (Math.abs(log - Math.round(log)) > 0.001) return "";
                        return formatX(n);
                    },
                },
                grid:  { color: "rgba(0,0,0,0.06)" },
            },
            y: {
                type: "linear",
                title: {
                    display: true,
                    text: isHolds        ? "Hold score (%)" :
                          isMustBuy      ? "Must Buy composite (0\u2013100)" :
                          isTopChase     ? "30d net flow % (normalized)" :
                          isDemandSurge  ? "Net flow % (normalized)" :
                          isBestGrading  ? "ROI % (after fees + sub-grade recovery)" :
                                           "Score",
                    color: "#606060",
                    font: { size: 10 },
                },
                ticks: {
                    color: "#606060",
                    font: { size: 9 },
                    callback: formatY,
                },
                grid: {
                    // Reference line: 60 for must-buy, 0 elsewhere
                    color: (ctx) => {
                        const ref = isMustBuy ? 60 : 0;
                        return ctx.tick.value === ref ? "rgba(0,0,0,0.45)" : "rgba(0,0,0,0.06)";
                    },
                    lineWidth: (ctx) => {
                        const ref = isMustBuy ? 60 : 0;
                        return ctx.tick.value === ref ? 1.5 : 1;
                    },
                },
            },
        },
    };

    if (distChart) {
        distChart.destroy();
        distChart = null;
    }
    distChart = new Chart(canvas.getContext("2d"), {
        type: "scatter",
        data,
        options,
    });
}

// --- full render (recompute + filter + sort + draw) ---
function fullRender() {
    // Recompute scores (knobs may have changed)
    for (const c of allCards) {
        computeEvScore(c);                  // legacy, only used for sealed-card display
        computeHoldScore(c);                // feeds Long-Term Holds
        computeMustBuyScore(c);             // 6-dimension smart-investor composite
        computeBestGradingScore(c);         // simple % uplift for Best Grading Play
        computeTopChaseScoreEnriched(c);    // log(psa10) × demand × cultural for Top Chase
    }
    let list;
    if (view === "holds")              list = filterHolds();
    else if (view === "mustbuy")       list = filterMustBuy();
    else if (view === "topchase")      list = filterTopChase();
    else if (view === "demandsurge")   list = filterDemandSurge();
    else if (view === "bestgrading")   list = filterBestGrading();
    else                                list = filterMustBuy();  // safe default
    sortList(list);
    _currentList = list;
    renderThead();
    displayedCount = 0;
    renderRows(list, 0, PAGE_SIZE);
    renderDistributionChart(list);
}

// --- toolbar wiring ---
function wireToolbar() {
    // -- Tabs --
    document.querySelectorAll(".opp-tab").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll(".opp-tab").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            view = btn.dataset.view;

            // Swap control groups
            const holdsCtrl = document.querySelector(".opp-controls-holds");
            const mbCtrl    = document.querySelector(".opp-controls-mustbuy");
            const tcCtrl    = document.querySelector(".opp-controls-topchase");
            const dsCtrl    = document.querySelector(".opp-controls-demandsurge");
            const bgCtrl    = document.querySelector(".opp-controls-bestgrading");
            if (holdsCtrl) holdsCtrl.style.display = (view === "holds")       ? "flex" : "none";
            if (mbCtrl)    mbCtrl.style.display    = (view === "mustbuy")     ? "flex" : "none";
            if (tcCtrl)    tcCtrl.style.display    = (view === "topchase")    ? "flex" : "none";
            if (dsCtrl)    dsCtrl.style.display    = (view === "demandsurge") ? "flex" : "none";
            if (bgCtrl)    bgCtrl.style.display    = (view === "bestgrading") ? "flex" : "none";

            // Reset sort to view's sensible default
            if (view === "holds")             currentSort = { key: "score",      dir: "desc" };
            else if (view === "mustbuy")      currentSort = Object.keys(modelProjections).length > 0
                                                             ? { key: "proj", dir: "desc" }
                                                             : { key: "mbscore", dir: "desc" };
            else if (view === "topchase")     currentSort = { key: "chasescore", dir: "desc" };
            else if (view === "demandsurge")  currentSort = { key: "ratio",      dir: "desc" };
            else if (view === "bestgrading")  currentSort = { key: "ev",         dir: "desc" };
            else                               currentSort = { key: "mbscore",   dir: "desc" };

            fullRender();
        });
    });

    // -- Must Buy Now controls (smart-investor composite score) --
    const mbScoreSlider = document.getElementById("mustbuy-min-score");
    const mbScoreVal    = document.getElementById("mustbuy-min-score-val");
    if (mbScoreSlider) {
        mbScoreSlider.addEventListener("input", () => {
            mustBuyMinScore = Number(mbScoreSlider.value);
            if (mbScoreVal) mbScoreVal.textContent = mbScoreSlider.value;
            if (view === "mustbuy") fullRender();
        });
    }

    // -- Top Chase controls --
    const tcMinPsa10Input = document.getElementById("topchase-min-psa10");
    if (tcMinPsa10Input) {
        tcMinPsa10Input.addEventListener("input", (e) => {
            const v = Number(e.target.value);
            if (Number.isFinite(v) && v >= 0) { chaseMinPsa10 = v; if (view === "topchase") fullRender(); }
        });
    }

    // -- Demand Surge controls --
    const dsPsa10Input = document.getElementById("ds-min-psa10");
    if (dsPsa10Input) {
        dsPsa10Input.addEventListener("input", (e) => {
            const v = Number(e.target.value);
            if (Number.isFinite(v) && v >= 0) { dsMinPsa10 = v; if (view === "demandsurge") fullRender(); }
        });
    }
    const dsNfSlider = document.getElementById("ds-min-nfpct");
    const dsNfVal = document.getElementById("ds-min-nfpct-val");
    if (dsNfSlider) {
        dsNfSlider.addEventListener("input", () => {
            dsMinNfPct = Number(dsNfSlider.value);
            if (dsNfVal) dsNfVal.textContent = (dsMinNfPct >= 0 ? "+" : "") + (dsMinNfPct * 100).toFixed(1) + "%";
            if (view === "demandsurge") fullRender();
        });
    }

    // -- Best Grading Play controls --
    const bgPsa10Input = document.getElementById("bg-min-psa10");
    if (bgPsa10Input) {
        bgPsa10Input.addEventListener("input", (e) => {
            const v = Number(e.target.value);
            if (Number.isFinite(v) && v >= 0) { bgMinPsa10 = v; if (view === "bestgrading") fullRender(); }
        });
    }
    const bgRawInput = document.getElementById("bg-min-raw");
    if (bgRawInput) {
        bgRawInput.addEventListener("input", (e) => {
            const v = Number(e.target.value);
            if (Number.isFinite(v) && v >= 0) { bgMinRaw = v; if (view === "bestgrading") fullRender(); }
        });
    }

    // -- Long-Term Holds controls --
    const holdTypeSelect = document.getElementById("hold-type");
    if (holdTypeSelect) {
        holdTypeSelect.addEventListener("change", (e) => {
            holdType = e.target.value;
            if (view === "holds") fullRender();
        });
    }
    const minHistoryInput = document.getElementById("min-history-input");
    if (minHistoryInput) {
        minHistoryInput.addEventListener("input", (e) => {
            const v = Number(e.target.value);
            if (Number.isFinite(v) && v >= 0) { minHistoryDays = v; if (view === "holds") fullRender(); }
        });
    }
    const minHoldInput = document.getElementById("min-hold-input");
    if (minHoldInput) {
        minHoldInput.addEventListener("input", (e) => {
            const v = Number(e.target.value);
            if (Number.isFinite(v) && v >= 0) { minHoldPrice = v; if (view === "holds") fullRender(); }
        });
    }
}

// --- column sort (delegated since thead is rebuilt each render) ---
document.getElementById("card-table").querySelector("thead").addEventListener("click", function(e) {
    const th = e.target.closest("th");
    if (!th) return;
    const key = th.dataset.sort;
    if (!key || key === "none") return;

    if (currentSort.key === key) {
        currentSort.dir = currentSort.dir === "asc" ? "desc" : "asc";
    } else {
        currentSort.key = key;
        currentSort.dir = (key === "name" || key === "set" || key === "rarity" || key === "type") ? "asc" : "desc";
    }
    fullRender();
});

// --- load more ---
document.getElementById("load-more-btn").addEventListener("click", function() {
    renderRows(_currentList, displayedCount, PAGE_SIZE);
});

// --- fetch ---
async function loadCardIndex() {
    const statusMsg = document.getElementById("status-msg");
    statusMsg.textContent = "Loading card data + model projections...";
    try {
        // Fetch card data and model projections in parallel
        const [cardRes, projRes] = await Promise.all([
            fetch(`${API_BASE}/card_index`),
            fetch(`${API_BASE}/model/projections`).catch(() => null),
        ]);

        if (!cardRes.ok) throw new Error(`HTTP ${cardRes.status}`);
        const data = await cardRes.json();
        const cards = Array.isArray(data) ? data : (data.cards || data.rows || []);
        allCards = cards;

        // Load projections (graceful — works without model)
        if (projRes && projRes.ok) {
            const projData = await projRes.json();
            modelProjections = projData.projections || {};
            console.log(`Loaded ${Object.keys(modelProjections).length} model projections`);
        } else {
            modelProjections = {};
            console.log("No model projections available (model not trained yet)");
        }

        fullRender();

        const singles = allCards.filter(c => !c["is-sealed"]);
        const sealed = allCards.filter(c => c["is-sealed"]);
        const withPsa10 = singles.filter(c => Number(c["psa-10-price"]) > 0).length;
        const projCount = Object.keys(modelProjections).length;
        const modelTag = projCount > 0 ? ` • ${projCount} model projections` : " • model not trained";
        statusMsg.textContent =
            `Loaded ${singles.length} singles + ${sealed.length} sealed • ${withPsa10} have PSA 10${modelTag}`;
    } catch (err) {
        console.error(err);
        statusMsg.textContent = `Error: ${err.message}`;
        const colCount = 11;
        document.getElementById("card-tbody").innerHTML =
            `<tr><td colspan="${colCount}" style="text-align:center;color:#cc0000;">Failed to load. Check connection.</td></tr>`;
    }
}

wireToolbar();
loadCardIndex();
