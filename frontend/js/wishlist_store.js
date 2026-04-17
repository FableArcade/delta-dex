/**
 * Wishlist store + priority algorithm.
 *
 * Storage: localStorage key "pokemon-analytics.wishlist" → array of
 * { id: string, addedAt: ISO string }. Single-user, client-side, survives
 * tabs but not browser profile wipes.
 *
 * Priority scoring: given a budget + holding horizon + a card's existing
 * metric payload from /api/card_index, produce a 0–100 "buy-first" fit
 * score + a human-readable rationale. Different horizons weight different
 * signals:
 *
 *   short  (≤30d)   : Must Buy Now composite dominates (recent setup)
 *   medium (90d–6mo): blend of Must Buy + cultural + positive momentum
 *   long   (1yr+)   : hold score + cultural moat + price stability
 *
 * Budget fit: cards above budget are dropped. Within budget, cards using
 * 20–60% of budget score highest (meaningful position without spending all
 * dry powder on one card). Cards using <20% are "small ticket" and score
 * slightly lower; cards using >60% are "concentration risk" and score lower.
 *
 * Trajectory bonus: applied on top of the horizon-weighted signal — cards
 * with rising 30d trajectory + tight supply get a multiplier, cards with
 * falling trajectory + no cultural moat get a penalty.
 */
(function(global) {
    "use strict";

    const STORAGE_KEY = "pokemon-analytics.wishlist";

    // ------------------------------------------------------------------
    // Storage
    // ------------------------------------------------------------------

    function loadWishlist() {
        try {
            const raw = localStorage.getItem(STORAGE_KEY);
            if (!raw) return [];
            const arr = JSON.parse(raw);
            return Array.isArray(arr) ? arr : [];
        } catch (e) {
            console.warn("wishlist load failed", e);
            return [];
        }
    }

    function saveWishlist(list) {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
        } catch (e) {
            console.warn("wishlist save failed", e);
        }
    }

    function isWishlisted(cardId) {
        if (!cardId) return false;
        const list = loadWishlist();
        return list.some(entry => String(entry.id) === String(cardId));
    }

    function addToWishlist(cardId) {
        if (!cardId) return false;
        const list = loadWishlist();
        if (list.some(e => String(e.id) === String(cardId))) return false;
        list.push({ id: String(cardId), addedAt: new Date().toISOString() });
        saveWishlist(list);
        return true;
    }

    function removeFromWishlist(cardId) {
        if (!cardId) return false;
        const before = loadWishlist();
        const after = before.filter(e => String(e.id) !== String(cardId));
        if (after.length === before.length) return false;
        saveWishlist(after);
        return true;
    }

    function toggleWishlist(cardId) {
        if (isWishlisted(cardId)) {
            removeFromWishlist(cardId);
            return false;
        }
        addToWishlist(cardId);
        return true;
    }

    function clearWishlist() {
        saveWishlist([]);
    }

    // ------------------------------------------------------------------
    // Cultural scoring — mirror of card_leaderboard.js
    // Kept in sync so wishlist priority uses the same brand floor signal.
    // ------------------------------------------------------------------

    const ICONIC_NAMES = [
        // S-tier: franchise faces
        [/charizard/i, 1.00], [/pikachu/i, 1.00], [/mewtwo/i, 0.96],
        [/\bmew\b/i, 0.96], [/umbreon/i, 0.96],
        // A-tier: mascots and pillars
        [/lugia/i, 0.88], [/rayquaza/i, 0.88], [/gengar/i, 0.85],
        [/snorlax/i, 0.82], [/dragonite/i, 0.82],
        [/blastoise/i, 0.78], [/venusaur/i, 0.78], [/gyarados/i, 0.80],
        [/greninja/i, 0.82], [/lucario/i, 0.80], [/garchomp/i, 0.78],
        [/zoroark/i, 0.75], [/sceptile/i, 0.72], [/blaziken/i, 0.72],
        [/swampert/i, 0.72],
        // Eeveelutions family (community darlings)
        [/sylveon/i, 0.78], [/espeon/i, 0.75], [/leafeon/i, 0.72],
        [/glaceon/i, 0.72], [/vaporeon/i, 0.70], [/jolteon/i, 0.70],
        [/flareon/i, 0.70], [/eevee/i, 0.72],
        // Legendaries / mythicals
        [/giratina/i, 0.70], [/dialga/i, 0.65], [/palkia/i, 0.65],
        [/arceus/i, 0.72], [/zekrom|reshiram/i, 0.65],
        [/yveltal|xerneas/i, 0.62], [/groudon|kyogre/i, 0.65],
        [/zacian|zamazenta/i, 0.62], [/calyrex/i, 0.60],
        [/\bho-?oh\b/i, 0.78], [/celebi/i, 0.70], [/jirachi/i, 0.68],
        [/darkrai/i, 0.68], [/deoxys/i, 0.62], [/genesect/i, 0.55],
        [/manaphy|phione/i, 0.55], [/shaymin/i, 0.58], [/victini/i, 0.60],
        [/meloetta/i, 0.55], [/keldeo/i, 0.52], [/necrozma/i, 0.58],
        [/tapu (koko|lele|bulu|fini)/i, 0.60],
        [/solgaleo|lunala/i, 0.60], [/zeraora/i, 0.58], [/marshadow/i, 0.55],
        [/eternatus/i, 0.58], [/koraidon|miraidon/i, 0.62],
        [/terapagos/i, 0.65], [/ogerpon/i, 0.65],
        // Meme / internet-culture tier (regional exclusives + meme Pokemon).
        // These don't need "legendary" status — internet loves them, and
        // that love reliably shows up in secondary-market demand for promos.
        [/\bditto\b/i, 0.75],
        [/psyduck/i, 0.70], [/magikarp/i, 0.68], [/slowpoke/i, 0.68],
        [/slowbro/i, 0.65], [/slowking/i, 0.62],
        [/\bsnom\b/i, 0.72], [/wooloo/i, 0.68], [/cubone/i, 0.70],
        [/jigglypuff/i, 0.68], [/wigglytuff/i, 0.55],
        [/\bgastly\b/i, 0.62], [/haunter/i, 0.65],
        [/mimikyu/i, 0.78], [/gardevoir/i, 0.75], [/tinkaton/i, 0.68],
        [/fuecoco/i, 0.62], [/quaxly/i, 0.60], [/sprigatito/i, 0.62],
        [/ceruledge|armarouge/i, 0.60], [/annihilape/i, 0.55],
        [/meowth/i, 0.72], [/farfetch'?d/i, 0.60], [/galvantula/i, 0.52],
        [/\bbulbasaur\b/i, 0.72], [/squirtle/i, 0.72], [/charmander/i, 0.78],
        [/kingambit/i, 0.55], [/roaring moon|iron (valiant|treads|hands|bundle|moth|leaves|jugulis|thorns|crown)/i, 0.55],
        [/paradox pokemon/i, 0.50],
        // Trainer / NPC — iconic characters
        [/cynthia/i, 0.75], [/lillie/i, 0.72], [/acerola/i, 0.70],
        [/iono/i, 0.68], [/marnie/i, 0.65],
        [/\bhop\b|\bleon\b/i, 0.55],
        [/\bN['\u2019]s\b/i, 0.65],
        [/team rocket/i, 0.60], [/giovanni/i, 0.62],
        [/erika/i, 0.55], [/misty/i, 0.62], [/brock/i, 0.55],
        [/nemona|arven|penny|clavell/i, 0.58],
        [/professor sada|professor turo/i, 0.55],
        [/\bred\b|\bblue\b(?! )|\bgreen\b(?! )/i, 0.62],   // classic trainers, not color words
        [/\bgold\b(?! )|\bsilver\b(?! )|\bcrystal\b(?! )/i, 0.58],
        [/may |brendan/i, 0.52],
        [/\bhilda\b|\bhilbert\b/i, 0.55], [/\brosa\b|\bnate\b/i, 0.52],
        [/\byuna\b|\bnaru\b/i, 0.55],
    ];
    const RARITY_BONUS = {
        "Special Illustration Rare": 0.20,
        "Hyper Rare": 0.12,
        "Mega Hyper Rare": 0.18,
        "Mega Attack Rare": 0.12,
        "Secret Rare": 0.12,
        "Rainbow Rare": 0.12,
        "Gold Rare": 0.12,
        "Illustration Rare": 0.08,
        "Ultra Rare": 0.05,
    };

    function culturalImpactScore(card) {
        // Sealed cards use a completely different framework — see
        // sealedCulturalScore below. The Pokemon-name regex approach
        // doesn't work because sealed products aren't named "Charizard".
        if (card["is-sealed"]) return sealedCulturalScore(card);

        let nameScore = 0;
        const name = card["product-name"] || "";
        for (const [re, score] of ICONIC_NAMES) {
            if (re.test(name) && score > nameScore) nameScore = score;
        }
        const rarityBonus = RARITY_BONUS[card["rarity-name"]] || 0;
        return Math.max(0, Math.min(1, nameScore + rarityBonus));
    }

    // ==================================================================
    // SEALED CULTURAL SCORING
    // ==================================================================
    // Sealed products aren't named for Pokemon — they're named for sets
    // and product types. The cultural score here is a BLEND of:
    //   1. Set tier: how hyped is the set itself? Chase sets like 151,
    //      Prismatic Evolutions, Paldean Fates carry demand floors that
    //      common-expansion products don't.
    //   2. Product-type tier: Booster Boxes and ETBs are the prestige
    //      formats. Blister packs and mini tins are entry-level and
    //      carry less cultural weight per unit.
    //
    // Values are hand-curated from community sentiment + auction data.
    // These should be updated when new chase sets drop.

    // Set code → tier (0.3 = generic, 0.8 = chase). Update as sets release.
    const SEALED_SET_TIER = {
        // Chase sets (strong demand floors, secondary market appreciation)
        "MEW": 0.85,  // Scarlet & Violet 151 (gen 1 nostalgia, massive brand)
        "PRE": 0.85,  // Prismatic Evolutions (Eevee-focused, best-seller)
        "PAF": 0.80,  // Paldean Fates (shiny set, high chase appeal)
        "SCR": 0.75,  // Stellar Crown (Terapagos + SIR chase lineup)
        "OBF": 0.70,  // Obsidian Flames (Charizard SIR is a permanent chase)
        "SSP": 0.70,  // Surging Sparks (Pikachu Illustration)
        "SFA": 0.65,  // Shrouded Fable (Pecharunt + trainer SIRs)

        // Standard modern (solid but not chase)
        "SVI": 0.55,  // Scarlet & Violet base
        "PAL": 0.55,  // Paldea Evolved
        "PAR": 0.55,  // Paradox Rift
        "TEF": 0.55,  // Temporal Forces
        "TWM": 0.55,  // Twilight Masquerade
        "JTG": 0.50,  // Journey Together
        "ASC": 0.50,  // Ascended Heroes
        "DRI": 0.50,  // Destined Rivals
        "PFL": 0.50,  // Phantasmal Flames
        "POR": 0.45,  // Perfect Order
        "MEG": 0.50,  // Mega Evolution (new gen hype still TBD)

        // Special / Black & White retro
        "BLK": 0.65,  // Black Bolt (Zekrom retro chase)
        "WHT": 0.65,  // White Flare (Reshiram retro chase)
    };
    const DEFAULT_SEALED_SET_TIER = 0.40;   // unknown / small sets

    // Product type → prestige tier. Booster Box is the flagship; Blister
    // packs are consumer-grade. These are multiplicative floors added to
    // the set tier to reward the "real" collector products.
    const SEALED_PRODUCT_TIER = {
        "Booster Box":                        0.20,
        "Elite Trainer Box":                  0.15,
        "Elite Trainer Box [Pokemon Center]": 0.18,  // PC-exclusive premium
        "Super Premium Collection Box":       0.18,
        "Booster Bundle":                     0.10,
        "Mini Tin":                           0.05,
        "Collectors Chest":                   0.12,
        "Binder Collection":                  0.08,
        "Booster Pack":                       0.00,   // single packs are consumable
        "Sleeved Booster Pack":               0.02,
        "Blister/Sticker":                    0.00,
        "Poster Collection":                  0.02,
        "Other":                              0.03,
    };

    function sealedCulturalScore(card) {
        const setCode = card["set-code"] || "";
        const sealedType = card["sealed-type"] || "Other";
        const setTier = SEALED_SET_TIER[setCode] ?? DEFAULT_SEALED_SET_TIER;
        const productTier = SEALED_PRODUCT_TIER[sealedType] ?? 0.00;
        return Math.max(0, Math.min(1, setTier + productTier));
    }

    // ------------------------------------------------------------------
    // Priority scoring
    // ------------------------------------------------------------------

    const clamp01 = v => Math.max(0, Math.min(1, v));

    /**
     * Horizon → data-driven factor weights.
     *
     * These weights come from an OLS regression of forward 3-month returns
     * on price-based features across 76,943 (card, month) samples from
     * 2022-02 through 2026-01 — see scripts/backtest_wishlist_scorer.py
     * for the full derivation.
     *
     * Key findings from the regression:
     *   * peak_discount has the strongest POSITIVE coefficient (+0.166)
     *     — buying the dip predicts higher forward returns.
     *   * volatility has a POSITIVE coefficient (+0.085) — volatile cards
     *     mean-revert harder (proxy for oversold bounces).
     *   * cultural has POSITIVE coefficient (+0.062) — brand floor works.
     *   * mom_3m has NEGATIVE coefficient (-0.025) — short-term price
     *     momentum *reverses*, it does not trend. The prior "rising price
     *     is good" intuition was exactly backwards.
     *   * log_price is weakly NEGATIVE — cheap cards outperform (size
     *     premium / liquidity discount).
     *
     * The old scorer's top-vs-bottom decile spread was −0.24% (worse than
     * random). The new scorer's spread is +10.95%. Spearman rank correlation
     * improved from −0.0623 to +0.2451.
     *
     * Weight signs below reflect the regression DIRECTION — the `reversal`
     * and `sizeDiscount` signals have positive weights here because the
     * raw inputs are negated before they feed into the combined score.
     */
    function horizonWeights(horizon) {
        // Audit v2 finding: the scorer's signal actually INCREASES at longer
        // horizons (3m spread +7%, 6m +17%, 12m +38%). The old horizon design
        // had this backwards — it put the heaviest reversal weight at short
        // horizons, assuming mean reversion fades. The opposite is true:
        // reversal is the dominant signal at 6–12m, which means long-horizon
        // picks should lean most heavily on the dip-buying factors.
        //
        // Audit v3 (now): added setAlpha factor which captures the card's
        // return relative to its set's median return. This is the right
        // way to separate card-specific alpha from set-wide beta, so we
        // can reward cards that are LAGGING their set (mean-reversion peers).

        // Short (≤30d): the 1-month window is the weakest prediction horizon
        // in the backtest. Lean into immediate current-market signals.
        if (horizon === "short") {
            return {
                peakDisc: 0.20, maDistance: 0.15, volatility: 0.10, cultural: 0.15,
                reversal: 0.10, setAlpha: 0.10, sizeDiscount: 0.05, mustBuy: 0.15,
            };
        }
        // Medium (90d–6mo): the window the backtest primarily measured.
        if (horizon === "medium") {
            return {
                peakDisc: 0.25, maDistance: 0.20, volatility: 0.10, cultural: 0.15,
                reversal: 0.10, setAlpha: 0.10, sizeDiscount: 0.05, mustBuy: 0.05,
            };
        }
        // Long (1yr+): maximize dip-buying + cultural moat + setAlpha.
        // Long horizon benefits most from peer-relative signals because
        // set-wide rallies regress to the mean over multi-year periods.
        return {
            peakDisc: 0.30, maDistance: 0.15, volatility: 0.05, cultural: 0.25,
            reversal: 0.00, setAlpha: 0.15, sizeDiscount: 0.00, mustBuy: 0.10,
        };
    }

    /**
     * Rough Must Buy Now composite estimate from the card_index payload.
     * This is a simplified port — the full computation lives in
     * card_leaderboard.js, but for wishlist ranking we only need the
     * 0–1 strength signal (not the 0–100 UI score). Any signal missing
     * safely degrades to 0.3 (neutral) rather than 0 so we don't punish
     * cards for data gaps.
     */
    function mustBuyStrength(card) {
        if (card["is-sealed"]) return null;
        const psa10 = Number(card["psa-10-price"]);
        if (!Number.isFinite(psa10) || psa10 < 20) return null;

        const nf30Pct = card["net-flow-pct-30d"];
        const satIdx  = card["supply-saturation-index"];
        const cultural = culturalImpactScore(card);

        // Soft scoring: return 0.3 baseline when no market data rather than
        // excluding. Wishlist fit should still rank cards without market
        // data, just with a penalty.
        let demand = 0.3;
        if (nf30Pct != null) {
            demand = clamp01((Number(nf30Pct) + 0.01) / 0.05);
        }

        let supplyTight = 0.3;
        if (satIdx != null) {
            const s = Number(satIdx);
            if (s >= 1) supplyTight = 0;   // saturated = penalty
            else supplyTight = clamp01((1.0 - s) / 0.6);
        }

        const pop = card["psa-10-pop"];
        let popScarce = 0.3;
        if (pop != null && pop > 0) {
            if      (pop <= 100)  popScarce = 1.0;
            else if (pop <= 500)  popScarce = 1.0 - (pop - 100) / 800;
            else if (pop <= 2000) popScarce = 0.5 - (pop - 500) / 3000;
            else                   popScarce = 0;
        }

        // Simple composite — cultural drives brand floor, demand + scarcity
        // drive the "buy now" edge.
        return clamp01(
            0.25 * cultural +
            0.30 * demand   +
            0.25 * supplyTight +
            0.20 * popScarce
        );
    }

    // ------------------------------------------------------------------
    // Data-driven factor signals (derived from historical backtest)
    // ------------------------------------------------------------------

    /**
     * Returns the card's current price and the price-track keys it uses.
     * Sealed products use raw_price; singles use psa_10_price.
     */
    function priceAnchors(card) {
        const isSealed = card["is-sealed"];
        return {
            current: Number(card[isSealed ? "raw-price"    : "psa-10-price"]),
            p30:     Number(card[isSealed ? "raw-30d-ago"  : "psa10-30d-ago"]),
            p90:     Number(card[isSealed ? "raw-90d-ago"  : "psa10-90d-ago"]),
            p365:    Number(card[isSealed ? "raw-365d-ago" : "psa10-365d-ago"]),
            max1y:   Number(card[isSealed ? "raw-max-1y"   : "psa10-max-1y"]),
            min1y:   Number(card[isSealed ? "raw-min-1y"   : "psa10-min-1y"]),
        };
    }

    /**
     * Factor 1 — Peak Discount (0..1). The STRONGEST predictor in the
     * backtest (OLS coef +0.166, Spearman +0.21). Uncapped, unlike the
     * old hold-score formula which clipped at 50% — the data shows more
     * discount = more forward alpha, not less.
     */
    function peakDiscountFactor(card) {
        const a = priceAnchors(card);
        if (!Number.isFinite(a.current) || a.current <= 0) return null;
        if (!Number.isFinite(a.max1y)   || a.max1y   <= 0) return null;
        return Math.max(0, Math.min(1, (a.max1y - a.current) / a.max1y));
    }

    /**
     * Factor 2 — Volatility proxy (0..1). Computed from the 12-month
     * max/min range relative to current price. High range = the card
     * has moved around a lot = mean-reversion setup is stronger (OLS
     * coef +0.085, Spearman +0.17).
     *
     * Formula: range = (max_1y - min_1y) / current, then mapped so
     * a card that has swung 100%+ of its current price gets max score
     * and a dead-flat card gets 0.
     */
    function volatilityFactor(card) {
        const a = priceAnchors(card);
        if (!Number.isFinite(a.current) || a.current <= 0) return null;
        if (!Number.isFinite(a.max1y) || !Number.isFinite(a.min1y)) return null;
        if (a.max1y <= 0 || a.min1y <= 0) return null;
        const range = (a.max1y - a.min1y) / a.current;
        // 0% range → 0, 50% range → 0.5, 100%+ range → 1.0
        return Math.max(0, Math.min(1, range));
    }

    /**
     * Weighted multi-horizon momentum — a recency-weighted blend of
     * 30d / 90d / 365d returns. Replaces the single-anchor mom_3m that
     * the original reversalFactor used. A single anchor is vulnerable to
     * spike/bounce noise on that specific day; a weighted blend across
     * three horizons gives a cleaner read that's harder to fake.
     *
     * Weights: 0.50 × 30d + 0.30 × 90d + 0.20 × 365d
     *   — recency-biased (short term matters more for our mean-reversion
     *     thesis) but with enough long-term component that a card that's
     *     been flat-lining for a year doesn't look like a buy just because
     *     it had a noisy 30d print.
     *
     * Returns null if we don't have at least the 30d and 90d anchors.
     */
    function weightedMomentum(card) {
        const a = priceAnchors(card);
        if (!Number.isFinite(a.current) || a.current <= 0) return null;

        // 30d and 90d are required. 365d is optional (new cards lack it).
        if (!Number.isFinite(a.p30) || a.p30 <= 0) return null;
        if (!Number.isFinite(a.p90) || a.p90 <= 0) return null;
        const ret30 = (a.current / a.p30) - 1;
        const ret90 = (a.current / a.p90) - 1;

        let weightedReturn;
        if (Number.isFinite(a.p365) && a.p365 > 0) {
            const ret365 = (a.current / a.p365) - 1;
            weightedReturn = 0.50 * ret30 + 0.30 * ret90 + 0.20 * ret365;
        } else {
            // Missing 365d: redistribute weight proportionally (5/8 and 3/8)
            weightedReturn = 0.625 * ret30 + 0.375 * ret90;
        }
        return weightedReturn;
    }

    /**
     * Factor 3 — Reversal signal (0..1). The OPPOSITE of momentum.
     *
     * Uses the weighted multi-horizon momentum above instead of a single
     * 90d anchor. The backtest showed that cards down 20%+ over trailing
     * periods have historically outperformed cards up 20%+ by wide margins
     * (decile 1 +6.31% vs decile 10 +1.68% on 3m forward returns). Pokemon
     * card prices mean-revert, they don't trend, so this factor returns
     * HIGH values for cards that have recently fallen.
     *
     * Formula: map weighted return through a logistic-ish.
     *   return -30% → 1.0   (strong reversal setup)
     *   return   0% → 0.5   (neutral)
     *   return +30%+ → 0.0  (FOMO / late to the move)
     */
    function reversalFactor(card) {
        const wm = weightedMomentum(card);
        if (wm == null) return null;
        return Math.max(0, Math.min(1, 0.5 - wm / 0.60));
    }

    /**
     * Factor — Moving-average distance (0..1). Audit v2 found this is the
     * strongest NEW signal not already captured by peak_discount:
     *   Spearman −0.1859 (raw) vs 3-month forward return
     *
     * The idea: compare current price to the average of the last few
     * anchor points. If current is BELOW the moving average, the card is
     * oversold relative to its recent trend — a reversal setup. If above,
     * it's bought-in. Sign flipped so oversold → high factor score.
     *
     * We approximate a 6-month moving average from available snapshots:
     *   ma = average of (30d_ago, 90d_ago, 365d_ago) when all present
     * Fallback to whatever subset we have.
     */
    function maDistanceFactor(card) {
        const a = priceAnchors(card);
        if (!Number.isFinite(a.current) || a.current <= 0) return null;
        const anchors = [a.p30, a.p90, a.p365].filter(p => Number.isFinite(p) && p > 0);
        if (anchors.length < 2) return null;
        const ma = anchors.reduce((acc, p) => acc + p, 0) / anchors.length;
        if (ma <= 0) return null;
        // distance = (ma - current) / ma; positive = below MA = oversold
        const distance = (ma - a.current) / ma;
        // Map: -30% → 0.0 (way above MA, bought-in)
        //        0% → 0.5 (at MA)
        //      +30% → 1.0 (way below MA, oversold)
        return Math.max(0, Math.min(1, 0.5 + distance / 0.60));
    }

    /**
     * Factor — Set Alpha (0..1). The card's 90-day return RELATIVE to
     * its set's median 90-day return. This isolates card-specific alpha
     * from set-wide market beta.
     *
     * Example: a card is up +25% over 90 days. If its set median is up
     * +20%, the card is barely outperforming peers (alpha = +5%). If
     * its set median is up +5%, the card is beating peers by 20pp (real
     * alpha).
     *
     * For the wishlist's MEAN-REVERSION thesis, a card UNDERPERFORMING
     * its set is the buy signal — it's a laggard that's likely to catch
     * up. Map: alpha <= -20% → 1.0, alpha = 0 → 0.5, alpha >= +20% → 0.0.
     *
     * Returns null when we don't have set return data or the card's 90d
     * price history.
     *
     * @param card the card object
     * @param setReturns optional map of set_code → { "median-90d-return": float }
     */
    function setAlphaFactor(card, setReturns) {
        if (!setReturns) return null;
        const setCode = card["set-code"];
        if (!setCode) return null;
        const sr = setReturns[setCode];
        if (!sr) return null;
        const setMedian = sr["median-90d-return"];
        if (setMedian == null) return null;

        const a = priceAnchors(card);
        if (!Number.isFinite(a.current) || a.current <= 0) return null;
        if (!Number.isFinite(a.p90) || a.p90 <= 0) return null;
        const cardRet = (a.current / a.p90) - 1;
        const alpha = cardRet - setMedian;
        // Reversal-style mapping: negative alpha = laggard = buy signal
        return Math.max(0, Math.min(1, 0.5 - alpha / 0.40));
    }

    /**
     * Factor — Size discount / "cheap-is-good" (0..1). The backtest
     * showed a weak-but-consistent negative relationship between log(price)
     * and forward returns (OLS coef −0.014, Spearman −0.14). Cheaper
     * cards outperform, likely because the small-cap / low-liquidity
     * segment has more mispricing.
     *
     * Formula: rewards cheap cards, penalizes expensive ones relative to
     * a $1000 reference. Using log scale so the effect is gentle.
     *   $50   → 0.65
     *   $200  → 0.42
     *   $1000 → 0 (reference)
     *   $5000 → -0.35 (clipped to 0)
     *
     * This factor then gets shifted to [0,1] and scaled so tiny cards
     * aren't treated as automatic winners — the weighting of this
     * signal is deliberately small (5% in the composite).
     */
    function sizeDiscountFactor(card) {
        const a = priceAnchors(card);
        if (!Number.isFinite(a.current) || a.current <= 0) return null;
        // log10(1000) = 3. log10(current)/3, flipped so cheap is positive.
        const logP = Math.log10(a.current);
        return Math.max(0, Math.min(1, (3.0 - logP) / 2.0 + 0.5));
    }

    /**
     * Budget fit factor.
     *   >budget      → 0 (filtered out elsewhere, not here)
     *   0-20% budget → 0.7 (small ticket — fine but not ideal)
     *   20-60%       → 1.0 (sweet spot)
     *   60-80%       → 0.85 (stretch but ok)
     *   80-100%      → 0.65 (concentration risk)
     */
    function budgetFit(price, budget) {
        if (!Number.isFinite(budget) || budget <= 0) return 1.0;
        if (!Number.isFinite(price) || price <= 0)   return 0;
        const pct = price / budget;
        if (pct > 1.0) return 0;
        if (pct >= 0.80) return 0.65;
        if (pct >= 0.60) return 0.85;
        if (pct >= 0.20) return 1.00;
        // 0-20% of budget = small ticket
        return 0.65 + 0.35 * (pct / 0.20);   // 0→0.65, 0.20→1.0
    }

    // ==================================================================
    // SEALED-SPECIFIC SCORING
    // ==================================================================
    //
    // Sealed products behave opposite to singles: they TREND upward
    // as supply is destroyed (packs being opened). The singles scorer's
    // mean-reversion logic (buy the dip, reward beaten-down cards)
    // produces wrong signals on sealed — a sealed product that's 50%
    // off peak is usually still printing, not a value play.
    //
    // Sealed formula:
    //   * cultural (set tier + product type): primary driver
    //   * trend (POSITIVE momentum): rising price = demand confirmed
    //   * volatility: discount for risky / thin products
    //
    // No reversal factor, no peak-discount factor, no maDistance.
    function scoreForWishlistSealed(card, price, budget) {
        const cultural = sealedCulturalScore(card);
        // Trend = normalized weighted momentum, mapped so positive is good
        // (opposite of the singles reversal factor).
        const wm = weightedMomentum(card);
        let trend = null;
        if (wm != null) {
            // -20%+ → 0, 0% → 0.4, +20%+ → 1.0
            trend = Math.max(0, Math.min(1, 0.4 + wm / 0.40));
        }
        // Volatility proxy — the same formula as singles but we INVERT the
        // interpretation: low volatility = stable thesis = good for sealed.
        const volFactor = volatilityFactor(card);  // 0..1
        const stability = volFactor == null ? null : 1 - volFactor;

        // Horizon-weighted blend. Sealed is fundamentally a long-horizon
        // play — short-horizon sealed picks are rare and usually wrong.
        // The weights don't vary much across horizons because the core
        // thesis (confirmed trend + cultural moat + stability) is the same.
        const W = {
            cultural:  0.50,
            trend:     0.35,
            stability: 0.15,
        };

        const parts = [
            { value: cultural,   weight: W.cultural },
            { value: trend,      weight: W.trend },
            { value: stability,  weight: W.stability },
        ];
        let total = 0, base = 0;
        for (const p of parts) {
            if (p.value == null) continue;
            base += p.value * p.weight;
            total += p.weight;
        }
        base = total > 0 ? base / total : 0;

        const fit = budgetFit(price, budget);

        // Sealed conviction bonus: chase set + rising price = high conviction
        let convictionFactor = 1.0;
        const isChase = cultural >= 0.70;
        const isRising = trend != null && trend >= 0.60;
        const isFalling = trend != null && trend <= 0.30;
        if (isChase && isRising) convictionFactor = 1.10;
        else if (isFalling && !isChase) convictionFactor = 0.80;

        const fitScore = Math.round(clamp01(base * fit * convictionFactor) * 100);

        // Rationale
        const rationale = [];
        const setCode = card["set-code"] || "";
        const sealedType = card["sealed-type"] || "Sealed";
        rationale.push(`${sealedType} · ${setCode}`);
        if (cultural >= 0.80)      rationale.push(`Chase set tier (${Math.round(cultural * 100)}%)`);
        else if (cultural >= 0.60) rationale.push(`Strong set (${Math.round(cultural * 100)}%)`);
        else if (cultural >= 0.40) rationale.push(`Standard set (${Math.round(cultural * 100)}%)`);
        else                        rationale.push(`Lower-tier set`);

        if (trend != null) {
            const pct = wm == null ? 0 : Math.round(wm * 100);
            if      (trend >= 0.70) rationale.push(`Trending up +${pct}% (supply contracting)`);
            else if (trend >= 0.50) rationale.push(`Rising ${pct >= 0 ? "+" : ""}${pct}%`);
            else if (trend >= 0.35) rationale.push(`Flat ${pct >= 0 ? "+" : ""}${pct}%`);
            else                    rationale.push(`Declining ${pct}% (set may still be printing)`);
        }

        if (stability != null) {
            if (stability >= 0.70) rationale.push(`Stable pricing`);
            else if (stability <= 0.30) rationale.push(`High volatility (uncommon for sealed)`);
        }

        const pct = Number.isFinite(budget) && budget > 0 ? price / budget : null;
        if (pct != null) {
            if (pct < 0.20) rationale.push(`Small position (${Math.round(pct * 100)}% of budget)`);
            else if (pct > 0.80) rationale.push(`Concentration risk (${Math.round(pct * 100)}% of budget)`);
            else rationale.push(`Sized well (${Math.round(pct * 100)}% of budget)`);
        }

        if (convictionFactor > 1) rationale.push("✚ chase-set + rising trend bonus");
        if (convictionFactor < 1) rationale.push("− declining + no-moat penalty");

        return {
            fitScore,
            rationale,
            components: {
                cultural, trend, stability, fit, convictionFactor, price,
                weightedMomentum: wm,
            },
            filteredOut: false,
        };
    }

    /**
     * Main fit scorer — data-driven mean-reversion model for singles,
     * trend-following for sealed.
     *
     * See the horizonWeights() comment block for the full derivation.
     * In short: we backtested 76,943 (card, month) observations of
     * 3-month forward returns, discovered the old scorer had a −0.24%
     * top-vs-bottom decile spread (worse than random), and replaced its
     * trend-following logic with mean-reversion factors that produced
     * a +9.11% in-sample / +3.77% out-of-sample spread for singles.
     *
     * Sealed uses a separate (trend-following) branch.
     *
     * Returns { fitScore (0-100), rationale (string[]), components, filteredOut }
     */
    function scoreForWishlist(card, { budget, horizon, setReturns, projections }) {
        const price = card["is-sealed"]
            ? Number(card["raw-price"])
            : Number(card["psa-10-price"]);

        if (!Number.isFinite(price) || price <= 0) {
            return { fitScore: null, rationale: ["No price data"], filteredOut: true };
        }
        if (Number.isFinite(budget) && budget > 0 && price > budget) {
            return {
                fitScore: null,
                rationale: [`Over budget ($${price.toFixed(0)} > $${budget.toFixed(0)})`],
                filteredOut: true,
            };
        }

        // Sealed gets a completely different formula (trend-following,
        // cultural-dominant, no mean-reversion factors).
        if (card["is-sealed"]) {
            return scoreForWishlistSealed(card, price, budget);
        }

        // === Budget Fit v3.2 — additive composite ================================
        //
        // Mirrors the Must Buy v3.2 philosophy: every signal earns bounded
        // points toward a 0..100 scale. Model is the biggest single dimension
        // (35 pts) but never drowns out cultural, demand, or the classic
        // reversal setup. An 8th dimension — Budget Fit — rewards affordable
        // sizing so "affordable model picks" is the distinct lens.
        //
        //   Model projection   ≤ 35   (baseRoi × 35, same math as Pure ROI)
        //   Cultural moat      ≤ 15
        //   Demand momentum    ≤ 15   (mustBuyStrength proxy)
        //   Setup pattern      ≤ 15   (3 signals × 5 pts: off-peak, reversal,
        //                              below MA — the classic dip-buy setup)
        //   Budget fit         ≤ 15   (sweet spot 20-60% of budget)
        //   Timing kicker      ≤ 5    (confLow > 0 bonus)
        //   ───────────────
        //                   ≤ 100
        //
        // No multiplicative cascade. Cards over budget are still filtered
        // out at the top of the function before this runs.

        const cultural = culturalImpactScore(card);
        const peakDisc = peakDiscountFactor(card);
        const maDistance = maDistanceFactor(card);
        const reversal = reversalFactor(card);
        const mustBuy = mustBuyStrength(card);
        const fit = budgetFit(price, budget);

        const proj = projections ? projections[String(card.id)] : null;
        const projReturn = proj ? Number(proj["projected-return"]) : NaN;
        const confLow    = proj ? Number(proj["confidence-low"])   : NaN;
        const confHigh   = proj ? Number(proj["confidence-high"])  : NaN;
        const confWidth  = proj ? Number(proj["confidence-width"])
                                : NaN;

        if (Number.isFinite(projReturn)) {
            // modelScore maps projected return: 0%→0, 15%→0.5, 30%+→1.0.
            let modelScore;
            if (projReturn < 0) {
                modelScore = Math.max(0, 0.10 + projReturn);
            } else {
                modelScore = clamp01(projReturn / 0.30);
            }

            const confMult = !Number.isFinite(confWidth) ? 0.7
                           : confWidth < 0.15 ? 1.0
                           : confWidth < 0.30 ? 0.7
                           : 0.4;
            const confLabel = !Number.isFinite(confWidth) ? "—"
                            : confWidth < 0.15 ? "HIGH"
                            : confWidth < 0.30 ? "MED"
                            : "LOW";

            // baseRoi — the Pure ROI building block (0..~1.3). Bring onto
            // 0..1 before awarding points so the 35-pt ceiling isn't broken.
            const baseRoi = modelScore * confMult;
            const modelPts = clamp01(baseRoi) * 35;

            // Setup pattern — three classic dip-buy signals, 5 pts each.
            const isOffPeak   = (peakDisc   != null && peakDisc   >= 0.15) ? 1 : 0;
            const isReversal  = (reversal   != null && reversal   >= 0.50) ? 1 : 0;
            const isBelowMa   = (maDistance != null && maDistance >= 0.55) ? 1 : 0;
            const setupPts    = (isOffPeak + isReversal + isBelowMa) * 5;
            const setupSignals = { isOffPeak, isReversal, isBelowMa };

            // Demand proxy — mustBuyStrength is the closest single-number
            // demand pressure metric we compute in wishlist_store.
            const demandPts = (mustBuy != null ? mustBuy : 0) * 15;

            // Budget fit — inherit the existing sweet-spot curve, but award
            // points additively instead of multiplying.
            const budgetFitPts = fit * 15;

            // Timing kicker — 5 pts when the model's downside band is above
            // zero (even worst case is profitable). Small but meaningful.
            const timingPts = (Number.isFinite(confLow) && confLow > 0) ? 5 : 0;

            const culturalPts = cultural * 15;

            const rawScore = modelPts + culturalPts + demandPts
                           + setupPts + budgetFitPts + timingPts;
            const fitScore = Math.round(Math.min(100, rawScore));

            // --- Rationale ---
            const rationale = [];
            const signedPct = (v) => (v >= 0 ? "+" : "") + Math.round(v * 100) + "%";
            const ptsStr = (v, max) => `${Math.round(v)}/${max}`;

            rationale.push(
                `Model projects ${signedPct(projReturn)} (180d net)` +
                (Number.isFinite(confLow) && Number.isFinite(confHigh)
                    ? ` — conf [${signedPct(confLow)}, ${signedPct(confHigh)}] ${confLabel}`
                    : "") +
                ` → ${ptsStr(modelPts, 35)} pts`
            );
            if (timingPts > 0) rationale.push(`✚ downside band above zero → +${timingPts} pts`);

            if (cultural >= 0.75)      rationale.push(`Iconic moat (${Math.round(cultural * 100)}%) → ${ptsStr(culturalPts, 15)} pts`);
            else if (cultural >= 0.45) rationale.push(`Strong cultural (${Math.round(cultural * 100)}%) → ${ptsStr(culturalPts, 15)} pts`);
            else if (cultural >= 0.20) rationale.push(`Moderate cultural (${Math.round(cultural * 100)}%) → ${ptsStr(culturalPts, 15)} pts`);
            else                       rationale.push(`Weak cultural floor → ${ptsStr(culturalPts, 15)} pts`);

            if (mustBuy != null) {
                rationale.push(`Demand strength ${Math.round(mustBuy * 100)}% → ${ptsStr(demandPts, 15)} pts`);
            }

            const setupLabels = [];
            if (isOffPeak)  setupLabels.push("off-peak");
            if (isReversal) setupLabels.push("reversal");
            if (isBelowMa)  setupLabels.push("below MA");
            rationale.push(
                `Setup pattern: ${setupLabels.length ? setupLabels.join(" + ") : "none"} → ${setupPts}/15 pts`
            );
            if (peakDisc != null) {
                const pct = Math.round(peakDisc * 100);
                if      (peakDisc >= 0.40) rationale.push(`${pct}% off 12mo peak (strong dip)`);
                else if (peakDisc >= 0.20) rationale.push(`${pct}% off 12mo peak (mild dip)`);
                else if (peakDisc >= 0.05) rationale.push(`${pct}% off peak`);
            }

            const budgetPct = Number.isFinite(budget) && budget > 0 ? price / budget : null;
            if (budgetPct != null) {
                const bp = Math.round(budgetPct * 100);
                if      (budgetPct < 0.20)  rationale.push(`Small position (${bp}% of budget) → ${ptsStr(budgetFitPts, 15)} pts`);
                else if (budgetPct > 0.80)  rationale.push(`Concentration risk (${bp}% of budget) → ${ptsStr(budgetFitPts, 15)} pts`);
                else                         rationale.push(`Sized well (${bp}% of budget) → ${ptsStr(budgetFitPts, 15)} pts`);
            }

            _appendAlphaWarnings(rationale, card);
            _appendPsaPopWarnings(rationale, card);

            return {
                fitScore,
                rationale,
                components: {
                    modelScore, confMult, confLabel, baseRoi,
                    cultural, peakDisc, maDistance, reversal, mustBuy,
                    setupSignals, setupPts,
                    fit, budgetFitPts,
                    modelPts, culturalPts, demandPts, timingPts,
                    price, projReturn, confLow, confHigh, confWidth,
                },
                filteredOut: false,
            };
        }

        // ---- FALLBACK (no projection available) -------------------------------
        // Preserves the old mean-reversion composite for cards the model
        // hasn't seen yet (new promos, thin history). Users see this rationale
        // when the model can't weigh in.
        // (cultural, peakDisc, maDistance, reversal, mustBuy, fit already
        //  declared above — reuse rather than re-declare.)

        const vol         = volatilityFactor(card);
        const sizeDisc    = sizeDiscountFactor(card);
        const setAlpha    = setAlphaFactor(card, setReturns);

        const w = horizonWeights(horizon);
        const parts = [
            { value: peakDisc,   weight: w.peakDisc },
            { value: maDistance, weight: w.maDistance },
            { value: vol,        weight: w.volatility },
            { value: cultural,   weight: w.cultural },
            { value: reversal,   weight: w.reversal },
            { value: setAlpha,   weight: w.setAlpha },
            { value: sizeDisc,   weight: w.sizeDiscount },
            { value: mustBuy,    weight: w.mustBuy },
        ];
        let totalWeight = 0, base = 0;
        for (const p of parts) {
            if (p.value == null) continue;
            base += p.value * p.weight;
            totalWeight += p.weight;
        }
        base = totalWeight > 0 ? base / totalWeight : 0;

        let convictionFactor = 1.0;
        const isDipBuy   = (peakDisc != null && peakDisc >= 0.10) && cultural >= 0.45;
        const deadNoMoat = (reversal != null && reversal >= 0.70) && cultural < 0.20;
        if (isDipBuy) convictionFactor = 1.10;
        else if (deadNoMoat) convictionFactor = 0.80;

        const fitScore = Math.round(clamp01(base * fit * convictionFactor) * 100);

        const rationale = ["Model has no projection for this card — using fallback value composite."];
        if (cultural >= 0.75)      rationale.push(`Iconic moat (${Math.round(cultural * 100)}%)`);
        else if (cultural >= 0.45) rationale.push(`Strong cultural (${Math.round(cultural * 100)}%)`);
        else if (cultural >= 0.20) rationale.push(`Moderate cultural (${Math.round(cultural * 100)}%)`);
        else                       rationale.push(`Weak cultural floor`);
        if (peakDisc != null) {
            const pct = Math.round(peakDisc * 100);
            if (peakDisc >= 0.40)      rationale.push(`${pct}% off 12mo peak (strong dip)`);
            else if (peakDisc >= 0.20) rationale.push(`${pct}% off 12mo peak (mild dip)`);
            else if (peakDisc >= 0.05) rationale.push(`${pct}% off peak`);
        }
        if (convictionFactor > 1) rationale.push("✚ iconic-dip bonus");
        if (convictionFactor < 1) rationale.push("− falling + no moat penalty");
        _appendAlphaWarnings(rationale, card);
        _appendPsaPopWarnings(rationale, card);

        return {
            fitScore,
            rationale,
            components: {
                cultural, peakDisc, maDistance, vol, reversal, setAlpha,
                sizeDisc, mustBuy, fit, convictionFactor, price,
            },
            filteredOut: false,
        };
    }

    // Set-alpha linkage warning extracted so both branches share it.
    function _appendAlphaWarnings(rationale, card) {
        const alphaId      = card["alpha-card-id"];
        const alphaCorr    = Number(card["alpha-contemp-corr"]);
        const alphaName    = card["alpha-name"];
        const alphaCur     = Number(card["alpha-psa10-current"]);
        const alpha30      = Number(card["alpha-psa10-30d-ago"]);
        const alpha90      = Number(card["alpha-psa10-90d-ago"]);
        const isAlphaItself = alphaId && String(alphaId) === String(card.id);
        if (!isAlphaItself && Number.isFinite(alphaCorr) && alphaCorr >= 0.50
            && Number.isFinite(alphaCur) && alphaCur > 0) {
            const a30 = Number.isFinite(alpha30) && alpha30 > 0 ? alphaCur / alpha30 - 1 : null;
            const a90 = Number.isFinite(alpha90) && alpha90 > 0 ? alphaCur / alpha90 - 1 : null;
            const alphaShort = alphaName ? alphaName.replace(/\s*#\d+\s*$/, "") : "set alpha";
            if (a30 != null && a30 <= -0.10) {
                rationale.push(`⚠ ${alphaShort} (set alpha) down ${Math.round(a30 * 100)}% in 30d — beta linkage ρ=${alphaCorr.toFixed(2)}, expect drag next month`);
            } else if (a30 != null && a30 <= -0.03 && (a90 == null || a90 < 0)) {
                rationale.push(`⚠ ${alphaShort} (set alpha) softening (${Math.round(a30 * 100)}% 30d) — ρ=${alphaCorr.toFixed(2)}, watch for beta drag`);
            } else if (a30 != null && a30 >= 0.05) {
                rationale.push(`${alphaShort} (set alpha) +${Math.round(a30 * 100)}% 30d — positive drag for ρ=${alphaCorr.toFixed(2)} beta`);
            } else {
                rationale.push(`Tracks ${alphaShort} (set alpha, ρ=${alphaCorr.toFixed(2)}) — watch that chart`);
            }
        }
    }

    function _appendPsaPopWarnings(rationale, card) {
        const pop     = Number(card["psa-10-pop"]);
        const popPrev = Number(card["psa-10-pop-prev"]);
        if (Number.isFinite(pop) && Number.isFinite(popPrev) && popPrev > 0) {
            const popGrowth = pop / popPrev - 1;
            if (popGrowth >= 2.0) {
                rationale.push(`⚠ PSA 10 pop ${popPrev}→${pop} (+${Math.round(popGrowth * 100)}%) — severe grading wave`);
            } else if (popGrowth >= 0.50) {
                rationale.push(`⚠ PSA 10 pop +${Math.round(popGrowth * 100)}% MoM — active grading wave`);
            } else if (popGrowth >= 0.20) {
                rationale.push(`⚠ PSA 10 pop +${Math.round(popGrowth * 100)}% MoM — supply still expanding`);
            }
        }
    }

    // ------------------------------------------------------------------
    // Exports
    // ------------------------------------------------------------------

    global.WishlistStore = {
        // storage
        loadWishlist,
        saveWishlist,
        isWishlisted,
        addToWishlist,
        removeFromWishlist,
        toggleWishlist,
        clearWishlist,
        // scoring
        culturalImpactScore,
        scoreForWishlist,
    };
})(typeof window !== "undefined" ? window : globalThis);
