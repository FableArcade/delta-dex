/* ============================================================
   Card Detail Page — js/card.js
   Fetches card data + eBay market dynamics, renders charts & gauges
   ============================================================ */

const API_BASE = "/api";

// Game Boy color palette
const GB = {
    darkest:  "#0f380f",
    dark:     "#306230",
    light:    "#8bac0f",
    lightest: "#9bbc0f",
    bg:       "#0f380f",
};

// Extended palette for multi-line charts
const PALETTE = {
    rawGreen:       "#9bbc0f",
    psa10Yellow:    "#ffff00",
    psa9Orange:     "#ff8800",
    psa7Cyan:       "#00cccc",
    ebayDerived:    "#ff44ff",
    justTCG:        "#44aaff",
    collectrics:    "#33ff33",
    activeListings: "#9bbc0f",
    endedListings:  "#ff6644",
    newListings:    "#44aaff",
    demandPressure: "#ffff00",
    psa10Pop:       "#ffff00",
    psa9Pop:        "#ff8800",
    totalPop:       "#9bbc0f",
    gemPct:         "#33ff33",
};

/* ---- Metric tooltips — layman explanations for investors ----
   Each entry is a short, plain-English description of what the metric
   means and whether higher or lower is better for a holder.
   Wire into any label via `data-tooltip="${METRIC_TOOLTIPS.key}"` or the
   `tip(key)` helper which returns a full attribute string.
*/
const METRIC_TOOLTIPS = {
    // Supply & Demand pressure gauges (0–2 scale)
    demand_obs:        "How actively people are actually BUYING this card, measured from real eBay sold auctions over the window. Higher = stronger buyer interest. Scale 0–2. Above 1.0 = hot.",
    demand_est:        "An ESTIMATED demand score used when there aren't enough real sales to measure directly. Lower confidence than Demand (Obs). Same 0–2 scale.",
    supply:            "How flooded the market is with active listings (vs this card's normal baseline). LOWER IS BETTER for holders — less supply means sellers can hold firm on price. Scale 0–2.",
    net_flow:          "Listings sold − listings added, per day on average. POSITIVE = inventory is being absorbed faster than it's being listed (bullish — supply is shrinking, prices tend to rise). NEGATIVE = inventory is growing (bearish — supply is piling up, prices tend to fall). Higher is better for holders. Display uses scale-invariant % of active pool; +5% is a meaningful drain, +20% is a strong surge.",

    // Supply Saturation Index (despite the name, this is a listings MOMENTUM
    // ratio: avg active listings over the last 7 days divided by the last
    // 30-day average. It measures whether listings are trending up or down,
    // NOT absolute market saturation — a card with 3 listings and a card
    // with 3000 both score ssi ≈ 1.0 when stable.)
    saturation_index:  "Listings momentum ratio: 7-day average active listings divided by the 30-day average. Below 1 = listings contracting this week (tight / good for holders). Above 1 = listings expanding this week (building / bad for holders). This is a trend signal, not absolute scarcity — for absolute scarcity check the PSA 10 population.",
    saturation_tight:    "Tight: fewer active listings than normal for this card. Good for holders — sellers have pricing power.",
    saturation_balanced: "Balanced: a normal amount of supply relative to this card's baseline.",
    saturation_saturated:"Saturated: more active listings than normal. Expect downward price pressure until supply clears.",

    // State chips (from w7/w30 market_pressure.state_label).
    //
    // CAREFUL: avoid "buyer's market" / "seller's market" framing here — those
    // terms describe NEGOTIATING POWER, not flow direction, and they're the
    // opposite of what the underlying data is bullish for. When net flow is
    // negative (items selling faster than being listed), it's bullish for
    // PRICES going forward, but it's a seller's market in negotiating terms
    // (sellers don't need to lower prices). Stick to flow language so the
    // chip color (green = bullish) and label say the same thing.
    state_accumulating:"Net selling: more new listings appearing than items selling. Active eBay inventory is GROWING — bearish for prices going forward (expect softness as supply piles up). In negotiating terms this is a buyer's market (buyers can pick), but the price direction is what matters here.",
    state_draining:    "Net buying: more items selling than new listings appearing. Active eBay inventory is SHRINKING — bullish for prices going forward (supply is being absorbed faster than it's being replenished). In negotiating terms this is a seller's market (sellers don't need to discount), but for an investor what matters is that prices are likely to rise.",
    state_balanced:    "Balanced: new listings and ended listings are roughly in equilibrium. No strong directional signal either way.",

    // Key metrics
    avg_active:        "Average number of active eBay listings for this card over the window. Higher = more supply sitting on the market. Context matters — scarce chase cards often have <5 active at any time.",
    avg_ended:         "Average number of eBay auctions that ended (mostly sold) per window. Higher = more turnover and liquidity. Compare against Avg Active — you want ended ≥ active for a healthy market.",
    sold_rate_est:     "Estimated percentage of listings that actually sell through (vs sit unsold). Higher = faster turnover = healthier market. Above 60% is strong.",
    sales_volume:      "eBay sold listings in the last 7 days. Higher = more liquid — easier to buy or exit a position quickly.",

    // Price stat boxes
    raw_price:         "Current UNGRADED market price. This is your base cost before any grading fees.",
    psa10_price:       "Current market price for a PSA 10 (gem mint) copy. The ceiling for your grading upside.",
    psa10_vs_raw_dollar:"Absolute dollar premium of PSA 10 over Raw. Your gross margin before grading fees (~$25 per card at bulk rates).",
    psa10_vs_raw_pct:  "The PSA 10 premium as a percentage of raw. Above ~500% is usually a strong grading candidate once you factor in gem rate and fees.",

    // PSA population
    psa_10:            "Total copies graded PSA 10 by PSA (all-time population). LOWER population = rarer in top grade = higher PSA 10 premium. Supply-side scarcity signal.",
    psa_9:             "Total copies graded PSA 9 by PSA. Useful context for how hard it is to hit PSA 10 — compare against PSA 10 count.",
    total_graded:      "Total copies ever submitted to PSA (all grades). Higher = widely known as grade-worthy. A low total with high Gem Rate = potential hidden gem.",
    gem_rate:          "The percentage of PSA submissions that come back as PSA 10 for this card. Higher = easier to hit gem grade (better print quality / centering). Factor this into your grading expected-value math.",

    // Group section headers
    sd_pressure_group: "Supply & Demand Pressure: a pair of 0–2 gauges measuring how many buyers vs sellers are in the market for this card, plus the Net Flow between them.",
    saturation_group:  "Supply Saturation Index: single bar showing how flooded the listings are vs this card's historical baseline. Left (tight) = scarce. Right (saturated) = flooded.",
    listing_volume_group:"eBay Listing Volume: chart of active vs ended listings over time, with demand pressure overlaid. Rising active + flat ended = market building inventory (bearish). Rising ended + flat active = market clearing (bullish).",
    key_metrics_group: "Key Metrics: the raw numbers underneath the gauges — averages of active and ended listings, net flow, and estimated sold-through rate, for both the 7-day and 30-day windows.",
};

/** Return ` data-tooltip="..."` attribute (with escaping) for a tooltip key.
 *  Usage in template literals: `<div ${tip('demand_obs')}>Demand</div>` */
function tip(key) {
    const text = METRIC_TOOLTIPS[key];
    if (!text) return "";
    // Escape double quotes and HTML-sensitive chars for safe attribute use
    const safe = String(text)
        .replace(/&/g, "&amp;")
        .replace(/"/g, "&quot;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
    return ` data-tooltip="${safe}"`;
}

/* ---- Formatters ---- */

function money(x) {
    if (x == null || !Number.isFinite(Number(x))) return "\u2014";
    return Number(x).toLocaleString("en-US", {
        style: "currency", currency: "USD",
        minimumFractionDigits: 2, maximumFractionDigits: 2
    });
}

function pct(x) {
    if (x == null || !Number.isFinite(Number(x))) return "\u2014";
    return (Number(x) * 100).toFixed(1) + "%";
}

function num(x) {
    const n = Number(x);
    if (!Number.isFinite(n)) return "\u2014";
    return n.toLocaleString("en-US");
}

function numFixed(x, d = 2) {
    const n = Number(x);
    if (!Number.isFinite(n)) return "\u2014";
    return n.toFixed(d);
}

function gainClass(v) {
    const n = Number(v);
    if (!Number.isFinite(n) || Math.abs(n) < 0.01) return "";
    return n > 0 ? "chip-pos" : "chip-neg";
}

/* ---- Chart.js global defaults for CRT look ---- */

const CRT_CHART_DEFAULTS = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 400 },
    interaction: { mode: "index", intersect: false },
    plugins: {
        legend: { display: false },
        tooltip: {
            displayColors: true,
            backgroundColor: "#0f380f",
            borderColor: "#9bbc0f",
            borderWidth: 1,
            titleColor: "#9bbc0f",
            bodyColor: "#9bbc0f",
            titleFont: { family: "W95FA, Courier New, monospace", size: 11 },
            bodyFont: { family: "W95FA, Courier New, monospace", size: 11 },
        }
    },
    scales: {
        x: {
            ticks: { color: "#306230", font: { family: "W95FA, Courier New, monospace", size: 10 }, maxRotation: 0, autoSkipPadding: 20 },
            grid: { color: "rgba(48,98,48,0.3)" },
            border: { color: "#306230" },
        },
        y: {
            ticks: { color: "#306230", font: { family: "W95FA, Courier New, monospace", size: 10 } },
            grid: { color: "rgba(48,98,48,0.3)" },
            border: { color: "#306230" },
        }
    }
};

/* ---- State ---- */

let cardData = null;
let priceChart = null;
let listingChart = null;
let psaChart = null;
let currentPriceRange = "all";
let currentListingRange = "all";

/* ============================================================
   INIT
   ============================================================ */

(async function init() {
    const cardId = new URLSearchParams(window.location.search).get("id");
    if (!cardId) {
        setStatus("No card ID provided");
        return;
    }
    document.getElementById("status-id").textContent = `ID: ${cardId}`;
    setStatus("Fetching card data...");

    try {
        // Fetch card data and model projection in parallel
        const [cardRes, projRes] = await Promise.all([
            fetch(`${API_BASE}/card/${encodeURIComponent(cardId)}?include=ebay`),
            fetch(`${API_BASE}/model/projections`).catch(() => null),
        ]);
        if (!cardRes.ok) throw new Error(`HTTP ${cardRes.status}`);
        cardData = await cardRes.json();

        // Attach projection data if available
        if (projRes && projRes.ok) {
            const projData = await projRes.json();
            const proj = (projData.projections || {})[cardId];
            if (proj) cardData._projection = proj;
            // Track data freshness (used to render a staleness banner on projections).
            cardData._dataAsOf = projData.data_as_of || null;
            cardData._lastPipelineRunAt = projData.last_pipeline_run_at || null;
        }

        renderAll();
        setStatus("Ready");
    } catch (err) {
        console.error(err);
        setStatus(`Error: ${err.message}`);
    }
})();

function setStatus(msg) {
    const el = document.getElementById("status-msg");
    if (el) el.textContent = msg;
}

/* ============================================================
   RENDER ALL
   ============================================================ */

function renderAll() {
    if (!cardData) return;
    renderHeader();
    renderPriceChart();
    renderMarketDynamics();
    renderPSAPopulation();
    renderPriceSources();
    renderAlphaLinkage();
    setupIntervalButtons();
    renderModelProjection();
    renderTournamentPlay();
    document.getElementById("window-title").textContent =
        `DELTADEX.EXE - ${cardData["product-name"] || "Card Detail"}`;
    document.title = `${cardData["product-name"] || "Card"} | Delta Dex`;
}

/* ============================================================
   A) HEADER
   ============================================================ */

function renderHeader() {
    const d = cardData;
    const isSealed = (d["is-sealed"] === true) || (d["sealed-product"] === "Y");

    // Image — built with DOM methods (no innerHTML splatting of user-controlled
    // strings) so the security hook stays happy.
    const frame = document.getElementById("card-image-frame");
    while (frame.firstChild) frame.removeChild(frame.firstChild);
    if (d["image-url"]) {
        const img = document.createElement("img");
        img.src = d["image-url"];
        img.alt = d["product-name"] || "Card";
        img.loading = "eager";
        frame.appendChild(img);
    } else {
        const placeholder = document.createElement("div");
        placeholder.style.padding = "40px";
        placeholder.style.color = "#808080";
        placeholder.textContent = "No image available";
        frame.appendChild(placeholder);
    }
    renderRarityGrid();
    // Cultural + promo disclaimer render unconditionally — they apply even to
    // cards with no rarity-grade payload (e.g., promos missing pull data).
    renderCulturalRelevance();
    renderPromoRarityDisclaimer();

    // Name
    document.getElementById("card-name").textContent = d["product-name"] || "Unknown Product";

    // Wishlist button — toggles entry in localStorage via WishlistStore.
    // Also triggers a toast + pop animation so clicks feel unmistakably
    // acknowledged (the before/after visual state difference alone was
    // too subtle for users to notice it was working).
    const wishBtn   = document.getElementById("wishlist-btn");
    const wishIcon  = document.getElementById("wishlist-btn-icon");
    const wishLabel = document.getElementById("wishlist-btn-label");
    const cardId    = d.id;
    if (wishBtn && window.WishlistStore && cardId) {
        const paintState = (btn) => {
            const on = window.WishlistStore.isWishlisted(cardId);
            if (on) {
                btn.classList.add("added");
                btn.querySelector("#wishlist-btn-icon").textContent = "★";
                btn.querySelector("#wishlist-btn-label").textContent = "Wishlisted";
                btn.title = "Remove from your wishlist.";
            } else {
                btn.classList.remove("added");
                btn.querySelector("#wishlist-btn-icon").textContent = "☆";
                btn.querySelector("#wishlist-btn-label").textContent = "Add to Wishlist";
                btn.title = "Add this card to your wishlist — the Wishlist page will rank your picks by budget + holding horizon.";
            }
        };

        // Toast helper: slide a message in from the top, auto-dismiss after
        // a moment. Reused for both add and remove so the user always gets
        // an unmistakable "something happened" signal.
        const showToast = (msg, kind) => {
            const toast = document.getElementById("wishlist-toast");
            if (!toast) return;
            toast.textContent = msg;
            toast.classList.remove("removed");
            if (kind === "removed") toast.classList.add("removed");
            // Force reflow so the animation replays on repeat toggles
            void toast.offsetWidth;
            toast.classList.add("show");
            clearTimeout(showToast._timer);
            showToast._timer = setTimeout(() => {
                toast.classList.remove("show");
            }, 2600);
        };

        paintState(wishBtn);
        // Avoid double-binding on re-renders by cloning the button.
        const fresh = wishBtn.cloneNode(true);
        wishBtn.parentNode.replaceChild(fresh, wishBtn);
        fresh.addEventListener("click", () => {
            window.WishlistStore.toggleWishlist(cardId);
            const on = window.WishlistStore.isWishlisted(cardId);
            paintState(fresh);

            // Click-pop animation: remove + re-add so it replays on each click
            fresh.classList.remove("pop");
            void fresh.offsetWidth;
            fresh.classList.add("pop");

            // Toast — big, obvious, unmissable
            if (on) {
                const name = d["product-name"] || "card";
                showToast(`★ Added to wishlist: ${name}`, "added");
            } else {
                showToast("☆ Removed from wishlist", "removed");
            }
        });
    }

    // Set link — DOM construction so the set code can't break out of the href
    const setCode = d["set-code"] || "";
    const setInfo = document.getElementById("card-set-info");
    while (setInfo.firstChild) setInfo.removeChild(setInfo.firstChild);
    if (setCode) {
        const a = document.createElement("a");
        a.href = "/sets/" + encodeURIComponent(setCode);
        a.textContent = setCode;
        setInfo.appendChild(a);
    }

    // Rarity / sealed-type chip + card number
    if (isSealed) {
        document.getElementById("card-rarity").textContent = d["sealed-type"] || "Sealed Product";
        document.getElementById("card-number").textContent = "";
    } else {
        document.getElementById("card-rarity").textContent = d["rarity-name"] || "---";
        const cn = d["card-number"];
        const sc = d["set-count"];
        document.getElementById("card-number").textContent =
            cn != null ? `#${cn}${sc ? ` / ${sc}` : ""}` : "";
    }

    // Always show current price
    document.getElementById("stat-raw").textContent = money(d["raw-price"]);
    // Re-label "Raw Price" → "Sealed Price" for sealed products
    const rawLabel = document.querySelector('#stat-grid .stat-box:nth-child(1) .stat-box-label');
    if (rawLabel) rawLabel.textContent = isSealed ? "Sealed Price" : "Raw Price";

    if (isSealed) {
        // SEALED MODE — replace the four grading-related stats with momentum
        // / drawdown / sales volume. The history-based numbers come from
        // `history` (price_history) which we already pull for sealed products
        // via the daily Collectrics sync.
        const hist = Array.isArray(d.history) ? d.history : [];
        const validRaw = hist.filter(h => Number.isFinite(Number(h["raw-price"])) && Number(h["raw-price"]) > 0);
        const today = Number(d["raw-price"]);
        const findAtOffset = (days) => {
            if (validRaw.length === 0) return null;
            const cutoff = new Date();
            cutoff.setDate(cutoff.getDate() - days);
            const cutStr = cutoff.toISOString().slice(0, 10);
            const earlier = validRaw.filter(h => h.date <= cutStr);
            return earlier.length ? Number(earlier[earlier.length - 1]["raw-price"]) : null;
        };
        const r30 = findAtOffset(30);
        const r90 = findAtOffset(90);
        const peak = validRaw.reduce((m, h) => Math.max(m, Number(h["raw-price"]) || 0), 0);

        const setBox = (idx, label, value, cls) => {
            const box = document.querySelector(`#stat-grid .stat-box:nth-child(${idx})`);
            if (!box) return;
            const labelEl = box.querySelector(".stat-box-label");
            if (labelEl) labelEl.textContent = label;
            const valueEl = box.querySelector(".stat-box-value");
            if (valueEl) {
                valueEl.textContent = value;
                if (cls != null) valueEl.className = "stat-box-value small " + cls;
            }
        };

        // Box 2: 30-day change %
        const ch30 = (Number.isFinite(r30) && r30 > 0 && Number.isFinite(today))
            ? ((today - r30) / r30) * 100
            : null;
        setBox(2, "30D \u0394",
            ch30 == null ? "\u2014" : (ch30 >= 0 ? "+" : "") + ch30.toFixed(1) + "%",
            ch30 == null ? "" : ch30 >= 0 ? "chip-pos" : "chip-neg");

        // Box 3: 90-day change %
        const ch90 = (Number.isFinite(r90) && r90 > 0 && Number.isFinite(today))
            ? ((today - r90) / r90) * 100
            : null;
        setBox(3, "90D \u0394",
            ch90 == null ? "\u2014" : (ch90 >= 0 ? "+" : "") + ch90.toFixed(1) + "%",
            ch90 == null ? "" : ch90 >= 0 ? "chip-pos" : "chip-neg");

        // Box 4: Off peak %
        const offPeak = (peak > 0 && Number.isFinite(today))
            ? ((peak - today) / peak) * 100
            : null;
        setBox(4, "Off 12mo Peak",
            offPeak == null ? "\u2014" : "-" + offPeak.toFixed(1) + "%",
            offPeak != null && offPeak > 0 ? "chip-neg" : "");

        // Box 5: Sales volume
        document.getElementById("stat-volume").textContent = num(d["sales-volume"]);
    } else {
        // CARD MODE — original grading stats
        const rawP = Number(d["raw-price"]);
        const psa10P = Number(d["psa-10-price"]);
        const gainDollar = Number.isFinite(rawP) && Number.isFinite(psa10P) ? psa10P - rawP : null;
        const gainPctVal = Number.isFinite(rawP) && rawP > 0 && Number.isFinite(psa10P) ? (psa10P - rawP) / rawP : null;

        document.getElementById("stat-psa10").textContent = money(d["psa-10-price"]);

        const gainEl = document.getElementById("stat-gain");
        gainEl.textContent = gainDollar != null ? money(gainDollar) : "\u2014";
        gainEl.className = "stat-box-value small " + (gainDollar != null && gainDollar > 0 ? "chip-pos" : gainDollar != null && gainDollar < 0 ? "chip-neg" : "");

        const gainPctEl = document.getElementById("stat-gain-pct");
        gainPctEl.textContent = gainPctVal != null ? (gainPctVal * 100).toFixed(1) + "%" : "\u2014";
        gainPctEl.className = "stat-box-value small " + (gainPctVal != null && gainPctVal > 0 ? "chip-pos" : gainPctVal != null && gainPctVal < 0 ? "chip-neg" : "");

        document.getElementById("stat-volume").textContent = num(d["sales-volume"]);

        // --- Scoring Factors (wishlist model alignment) ---
        const sfDiv = document.getElementById("scoring-factors");
        if (sfDiv) {
            const hist = Array.isArray(d.history) ? d.history : [];
            const psa10Now = Number(d["psa-10-price"]);
            const validP10 = hist.filter(h => Number.isFinite(Number(h["psa-10-price"])) && Number(h["psa-10-price"]) > 0);

            // Helper: find PSA 10 price at N days ago
            const p10AtOffset = (days) => {
                if (!validP10.length) return null;
                const cutoff = new Date();
                cutoff.setDate(cutoff.getDate() - days);
                const cutStr = cutoff.toISOString().slice(0, 10);
                const earlier = validP10.filter(h => h.date <= cutStr);
                return earlier.length ? Number(earlier[earlier.length - 1]["psa-10-price"]) : null;
            };

            const p30 = p10AtOffset(30);
            const p90 = p10AtOffset(90);
            const p365 = p10AtOffset(365);

            // 12mo max/min from history
            const oneYearAgo = new Date();
            oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
            const yearStr = oneYearAgo.toISOString().slice(0, 10);
            const lastYear = validP10.filter(h => h.date >= yearStr);
            const max1y = lastYear.reduce((m, h) => Math.max(m, Number(h["psa-10-price"]) || 0), 0);
            const min1y = lastYear.reduce((m, h) => Math.min(m, Number(h["psa-10-price"]) || Infinity), Infinity);

            // 1) Off 12mo Peak
            const peakEl = document.getElementById("stat-peak-disc");
            if (max1y > 0 && Number.isFinite(psa10Now)) {
                const disc = ((max1y - psa10Now) / max1y) * 100;
                peakEl.textContent = disc > 0.5 ? `-${disc.toFixed(1)}%` : "At peak";
                peakEl.className = "stat-box-value small " + (disc > 0.5 ? "chip-neg" : "chip-pos");
            }

            // 2) Momentum (weighted multi-horizon)
            const momEl = document.getElementById("stat-momentum");
            const momParts = [];
            if (Number.isFinite(p30) && p30 > 0) momParts.push({ w: 0.50, ret: (psa10Now - p30) / p30 });
            if (Number.isFinite(p90) && p90 > 0) momParts.push({ w: 0.30, ret: (psa10Now - p90) / p90 });
            if (Number.isFinite(p365) && p365 > 0) momParts.push({ w: 0.20, ret: (psa10Now - p365) / p365 });
            if (momParts.length > 0 && Number.isFinite(psa10Now)) {
                const totalW = momParts.reduce((s, p) => s + p.w, 0);
                const wm = momParts.reduce((s, p) => s + p.w * p.ret, 0) / totalW;
                const wmPct = wm * 100;
                momEl.textContent = (wmPct >= 0 ? "+" : "") + wmPct.toFixed(1) + "%";
                momEl.className = "stat-box-value small " + (wmPct >= 0 ? "chip-pos" : "chip-neg");
            }

            // 3) vs Moving Avg
            const maEl = document.getElementById("stat-ma-dist");
            const anchors = [p30, p90, p365].filter(v => Number.isFinite(v) && v > 0);
            if (anchors.length > 0 && Number.isFinite(psa10Now) && psa10Now > 0) {
                const ma = anchors.reduce((s, v) => s + v, 0) / anchors.length;
                const dist = ((psa10Now - ma) / ma) * 100;
                maEl.textContent = (dist >= 0 ? "+" : "") + dist.toFixed(1) + "%";
                maEl.className = "stat-box-value small " + (dist >= 0 ? "chip-pos" : "chip-neg");
            }

            // 4) Volatility
            const volEl = document.getElementById("stat-volatility");
            if (max1y > 0 && min1y < Infinity && Number.isFinite(psa10Now) && psa10Now > 0) {
                const vol = (max1y - min1y) / psa10Now;
                volEl.textContent = vol.toFixed(2) + "x";
            }

            // 5) Set Alpha label (compact) — hover-popover wires lazily
            renderAlphaStatBox();
        }
    }
    const statusSrc = document.getElementById("status-source");
    if (statusSrc) statusSrc.textContent = setCode;

    // For sealed: hide the Market Dynamics + PSA Population sections — those
    // are graded-card concepts and would just look broken / empty.
    if (isSealed) {
        const sfHide = document.getElementById("scoring-factors");
        if (sfHide) sfHide.style.display = "none";
        const winBodies = document.querySelectorAll(".window-body .window");
        winBodies.forEach(w => {
            const title = w.querySelector(".window-title-text")?.textContent || "";
            if (title.includes("MARKET_DYNAMICS") ||
                title.includes("PSA_POPULATION")) {
                w.style.display = "none";
            }
        });
    }
}

/* ============================================================
   B) PRICE HISTORY CHART
   ============================================================ */

function filterByRange(arr, range) {
    if (!arr || !arr.length || range === "all") return arr;
    const days = parseInt(range);
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutStr = cutoff.toISOString().slice(0, 10);
    return arr.filter(h => h.date >= cutStr);
}

function buildPriceLegend() {
    const items = [
        { label: "Raw (PC)", color: PALETTE.rawGreen, dotted: false },
        { label: "PSA 10", color: PALETTE.psa10Yellow, dotted: false },
        { label: "PSA 9", color: PALETTE.psa9Orange, dotted: false },
        { label: "eBay Derived Raw", color: PALETTE.ebayDerived, dotted: true },
        { label: "JustTCG", color: PALETTE.justTCG, dotted: true },
        { label: "Collectrics Raw", color: PALETTE.collectrics, dotted: false },
    ];
    const el = document.getElementById("price-legend");
    el.innerHTML = items.map(i =>
        `<span class="crt-legend-item">
            <span class="crt-legend-swatch ${i.dotted ? "dotted" : ""}" style="${i.dotted ? "border-color:" + i.color : "background:" + i.color}"></span>
            ${i.label}
        </span>`
    ).join("");
}

function renderPriceChart() {
    buildPriceLegend();
    updatePriceChart();
}

function updatePriceChart() {
    const d = cardData;
    const hist = filterByRange(d.history || [], currentPriceRange);
    const histEbay = filterByRange(d["history-ebay-derived"] || [], currentPriceRange);
    const histJtcg = filterByRange(d["history-justtcg"] || [], currentPriceRange);
    const histColl = filterByRange(d["history-collectrics"] || [], currentPriceRange);

    // Build unified date labels from all sources
    const dateSet = new Set();
    [hist, histEbay, histJtcg, histColl].forEach(arr =>
        (arr || []).forEach(h => dateSet.add(h.date))
    );
    const labels = [...dateSet].sort();
    if (!labels.length) return;

    // Map data by date for each source
    function mapByDate(arr, key) {
        const m = {};
        (arr || []).forEach(h => { m[h.date] = h[key]; });
        return labels.map(d => m[d] ?? null);
    }

    const datasets = [
        {
            label: "Raw (PC)",
            data: mapByDate(hist, "raw-price"),
            borderColor: PALETTE.rawGreen,
            backgroundColor: PALETTE.rawGreen + "20",
            borderWidth: 2, pointRadius: 0, tension: 0.3, fill: false, spanGaps: true,
        },
        {
            label: "PSA 10",
            data: mapByDate(hist, "psa-10-price"),
            borderColor: PALETTE.psa10Yellow,
            borderWidth: 2, pointRadius: 0, tension: 0.3, fill: false, spanGaps: true,
        },
        {
            label: "PSA 9",
            data: mapByDate(hist, "psa-9-price"),
            borderColor: PALETTE.psa9Orange,
            borderWidth: 2, pointRadius: 0, tension: 0.3, fill: false, spanGaps: true,
        },
        {
            label: "eBay Derived Raw",
            data: mapByDate(histEbay, "d-raw-price"),
            borderColor: PALETTE.ebayDerived,
            borderWidth: 1.5, borderDash: [5, 3], pointRadius: 0, tension: 0.3, fill: false, spanGaps: true,
        },
        {
            label: "JustTCG",
            data: mapByDate(histJtcg, "j-raw-price"),
            borderColor: PALETTE.justTCG,
            borderWidth: 1.5, borderDash: [5, 3], pointRadius: 0, tension: 0.3, fill: false, spanGaps: true,
        },
        {
            label: "Collectrics Raw",
            data: mapByDate(histColl, "c-raw-price"),
            borderColor: PALETTE.collectrics,
            borderWidth: 2.5, pointRadius: 0, tension: 0.3, fill: false, spanGaps: true,
        },
    ];

    // Remove datasets that are all null
    const filtered = datasets.filter(ds => ds.data.some(v => v != null));

    if (priceChart) priceChart.destroy();

    const ctx = document.getElementById("chart-price").getContext("2d");
    priceChart = new Chart(ctx, {
        type: "line",
        data: { labels, datasets: filtered },
        options: {
            ...CRT_CHART_DEFAULTS,
            scales: {
                ...CRT_CHART_DEFAULTS.scales,
                y: {
                    ...CRT_CHART_DEFAULTS.scales.y,
                    ticks: {
                        ...CRT_CHART_DEFAULTS.scales.y.ticks,
                        callback: v => "$" + v.toFixed(2),
                    }
                }
            },
            plugins: {
                ...CRT_CHART_DEFAULTS.plugins,
                tooltip: {
                    ...CRT_CHART_DEFAULTS.plugins.tooltip,
                    callbacks: {
                        label: ctx => {
                            const v = ctx.parsed.y;
                            return v != null ? `${ctx.dataset.label}: $${v.toFixed(2)}` : null;
                        }
                    }
                }
            }
        }
    });
}

/* ============================================================
   C) MARKET DYNAMICS
   ============================================================ */

function renderMarketDynamics() {
    renderPressureGauges();
    renderSaturationGauge();
    renderListingChart();
    renderKeyMetrics();
}

/* C1: Pressure Gauges */
function renderPressureGauges() {
    const mp = cardData?.collectrics?.["market-pressure"];
    const container = document.getElementById("pressure-gauges");
    if (!mp) {
        container.innerHTML = `<div style="color:#808080;text-align:center;padding:8px;">No market pressure data available</div>`;
        return;
    }

    const obs = mp.observed || {};
    const est = mp.estimated || {};
    let html = "";

    // Map label text -> tooltip key so the same gaugeRow helper can cover
    // every pressure gauge variant.
    function gaugeTipKey(label) {
        const l = String(label).toLowerCase();
        if (l.includes("demand (obs)")) return "demand_obs";
        if (l.includes("demand (est)")) return "demand_est";
        if (l.startsWith("supply"))     return "supply";
        if (l.includes("net flow"))     return "net_flow";
        return null;
    }

    function gaugeRow(label, value, maxVal, barClass) {
        const tipKey = gaugeTipKey(label);
        const tipAttr = tipKey ? tip(tipKey) : "";
        const v = Number(value);
        if (!Number.isFinite(v)) {
            return `<div class="gauge-row">
                <span class="gauge-label"${tipAttr}>${label}</span>
                <div class="gauge-bar-wrap"><div class="progress-wrap"><div class="${barClass}" style="width:0%"></div></div></div>
                <span class="gauge-value">\u2014</span>
            </div>`;
        }
        const pctW = Math.min(Math.max((v / maxVal) * 100, 0), 100);
        return `<div class="gauge-row">
            <span class="gauge-label"${tipAttr}>${label}</span>
            <div class="gauge-bar-wrap"><div class="progress-wrap"><div class="${barClass}" style="width:${pctW}%"></div></div></div>
            <span class="gauge-value">${numFixed(v)}</span>
        </div>`;
    }

    function stateChip(state) {
        if (!state) return "";
        // FLOW-DIRECTION labels (not negotiating power):
        //   "draining"     = items selling faster than listing → inventory shrinking → BULLISH
        //   "accumulating" = items listing faster than selling → inventory growing  → BEARISH
        // We deliberately avoid "buyer's/seller's market" language because in
        // negotiating-power terms those would be EXACTLY INVERTED from the
        // price-direction signal, which is what an investor actually cares about.
        let cls, display;
        if (state === "draining") {
            cls = "state-bullish";
            display = "\u2191 Net buying";
        } else if (state === "accumulating") {
            cls = "state-bearish";
            display = "\u2193 Net selling";
        } else {
            cls = "state-balanced";
            display = "Balanced";
        }
        const tipAttr = tip(`state_${state}`);
        return `<span class="state-chip ${cls}"${tipAttr}>${display}</span>`;
    }

    // 7d window
    const w7 = obs["7d"];
    if (w7) {
        const m = w7.metrics || {};
        const l = w7.labels || {};
        html += `<div class="section-header"><span class="section-icon">&#9201;</span> 7-Day Window ${stateChip(l.state)}</div>`;
        html += gaugeRow("Demand (Obs)", m["demand-pressure"], 2, "progress-bar-green");
        html += gaugeRow("Supply", m["supply-pressure"], 2, "progress-bar-red");
        html += gaugeRow("Net Flow", m["net-flow"], 2, m["net-flow"] >= 0 ? "progress-bar-green" : "progress-bar-red");
    }

    // 7d estimated
    const e7 = est["7d"];
    if (e7) {
        const m = e7.metrics || {};
        html += gaugeRow("Demand (Est)", m["demand-pressure"], 2, "progress-bar-yellow");
    }

    // 30d window
    const w30 = obs["30d"];
    if (w30) {
        const m = w30.metrics || {};
        const l = w30.labels || {};
        html += `<div class="section-header mt-8"><span class="section-icon">&#128197;</span> 30-Day Window ${stateChip(l.state)}</div>`;
        html += gaugeRow("Demand (Obs)", m["demand-pressure"], 2, "progress-bar-green");
        html += gaugeRow("Supply", m["supply-pressure"], 2, "progress-bar-red");
        html += gaugeRow("Net Flow", m["net-flow"], 2, m["net-flow"] >= 0 ? "progress-bar-green" : "progress-bar-red");
    }

    // 30d estimated
    const e30 = est["30d"];
    if (e30) {
        const m = e30.metrics || {};
        html += gaugeRow("Demand (Est)", m["demand-pressure"], 2, "progress-bar-yellow");
    }

    container.innerHTML = html || `<div style="color:#808080;text-align:center;padding:8px;">No pressure data</div>`;
}

/* C2: Supply Saturation Index */
function renderSaturationGauge() {
    const mp = cardData?.collectrics?.["market-pressure"];
    const container = document.getElementById("saturation-gauge");
    const bc = mp?.observed?.["baseline-comparison"];
    if (!bc) {
        container.innerHTML = `<div style="color:#808080;text-align:center;padding:8px;">No saturation data</div>`;
        return;
    }

    const idx = Number(bc["supply-saturation-index"]);
    const label = bc["supply-saturation-label"] || "unknown";
    const trend = bc["trend"] || "\u2014";

    const satColor = label === "tight" ? "#008000"
        : label === "saturated" ? "#cc0000"
        : "#c0a000";

    // Map 0-2 range to percentage
    const pctW = Number.isFinite(idx) ? Math.min(Math.max((idx / 2) * 100, 0), 100) : 0;
    const markerLeft = Number.isFinite(idx) ? Math.min(Math.max((idx / 2) * 100, 1), 99) : 50;

    // Saturation chip — semantics: tight = good for holders (bullish), saturated = bad (bearish).
    // Use the new bullish/bearish classes directly so they're not coupled to the state-label classes.
    const labelCls = label === "tight" ? "state-bullish"
        : label === "saturated" ? "state-bearish"
        : "state-balanced";

    const labelTipKey = `saturation_${label}`;
    const labelTipAttr = METRIC_TOOLTIPS[labelTipKey] ? tip(labelTipKey) : tip("saturation_index");

    container.innerHTML = `
        <div style="margin-bottom:6px;">
            <span class="state-chip ${labelCls}"${labelTipAttr}>${label}</span>
            <span style="margin-left:8px;font-size:12px;">Index: <b${tip("saturation_index")}>${Number.isFinite(idx) ? idx.toFixed(2) : "\u2014"}</b></span>
            <span style="margin-left:8px;font-size:12px;">Trend: <b>${trend}</b></span>
        </div>
        <div class="sat-bar-track">
            <div class="sat-bar-fill" style="width:${pctW}%;background:${satColor};"></div>
            <div class="sat-bar-marker" style="left:${markerLeft}%;"></div>
        </div>
        <div class="sat-bar-labels">
            <span${tip("saturation_tight")}>0 (Tight)</span>
            <span>1 (Normal)</span>
            <span${tip("saturation_saturated")}>2 (Saturated)</span>
        </div>
    `;
}

/* C3: Listing Volume Chart */
function buildListingLegend() {
    const items = [
        { label: "Active Listings", color: PALETTE.activeListings, dotted: false },
        { label: "Ended/Sold", color: PALETTE.endedListings, dotted: false },
        { label: "New Listings", color: PALETTE.newListings, dotted: false },
        { label: "Demand Pressure", color: PALETTE.demandPressure, dotted: true },
    ];
    const el = document.getElementById("listing-legend");
    el.innerHTML = items.map(i =>
        `<span class="crt-legend-item">
            <span class="crt-legend-swatch ${i.dotted ? "dotted" : ""}" style="${i.dotted ? "border-color:" + i.color : "background:" + i.color}"></span>
            ${i.label}
        </span>`
    ).join("");
}

function renderListingChart() {
    buildListingLegend();
    updateListingChart();
}

function updateListingChart() {
    const histEbay = filterByRange(cardData["history-ebay"] || [], currentListingRange);
    const histMarket = filterByRange(cardData["history-ebay-market"] || [], currentListingRange);

    const dateSet = new Set();
    [histEbay, histMarket].forEach(arr =>
        (arr || []).forEach(h => dateSet.add(h.date))
    );
    const labels = [...dateSet].sort();
    if (!labels.length) return;

    function mapByDate(arr, key) {
        const m = {};
        (arr || []).forEach(h => { m[h.date] = h[key]; });
        return labels.map(d => m[d] ?? null);
    }

    const datasets = [
        {
            label: "Active Listings",
            data: mapByDate(histEbay, "active-to"),
            borderColor: PALETTE.activeListings,
            backgroundColor: PALETTE.activeListings + "30",
            borderWidth: 2, pointRadius: 0, tension: 0.3, fill: true, spanGaps: true,
            yAxisID: "y",
        },
        {
            label: "Ended/Sold",
            data: mapByDate(histEbay, "ended"),
            borderColor: PALETTE.endedListings,
            backgroundColor: PALETTE.endedListings + "40",
            borderWidth: 1.5, pointRadius: 0, tension: 0.3,
            type: "bar", yAxisID: "y",
        },
        {
            label: "New Listings",
            data: mapByDate(histEbay, "new"),
            borderColor: PALETTE.newListings,
            backgroundColor: PALETTE.newListings + "40",
            borderWidth: 1.5, pointRadius: 0, tension: 0.3,
            type: "bar", yAxisID: "y",
        },
        {
            label: "Demand Pressure",
            data: mapByDate(histMarket, "demand-pressure-observed"),
            borderColor: PALETTE.demandPressure,
            borderWidth: 2, borderDash: [4, 3], pointRadius: 0, tension: 0.3, fill: false, spanGaps: true,
            yAxisID: "y1",
        },
    ];

    const filtered = datasets.filter(ds => ds.data.some(v => v != null));

    if (listingChart) listingChart.destroy();

    const ctx = document.getElementById("chart-listings").getContext("2d");
    listingChart = new Chart(ctx, {
        type: "line",
        data: { labels, datasets: filtered },
        options: {
            ...CRT_CHART_DEFAULTS,
            scales: {
                x: CRT_CHART_DEFAULTS.scales.x,
                y: {
                    ...CRT_CHART_DEFAULTS.scales.y,
                    position: "left",
                    title: { display: true, text: "Listings", color: "#306230", font: { size: 10 } },
                },
                y1: {
                    ...CRT_CHART_DEFAULTS.scales.y,
                    position: "right",
                    title: { display: true, text: "Pressure", color: "#306230", font: { size: 10 } },
                    grid: { drawOnChartArea: false },
                }
            }
        }
    });
}

/* C4: Key Metrics */
function renderKeyMetrics() {
    const mp = cardData?.collectrics?.["market-pressure"];
    const container = document.getElementById("key-metrics");
    if (!mp) {
        container.innerHTML = `<div style="color:#808080;text-align:center;padding:8px;grid-column:1/-1;">No metrics available</div>`;
        return;
    }

    const obs = mp.observed || {};
    const est = mp.estimated || {};

    function val(window, metricPath) {
        const w = obs[window];
        if (!w) return "\u2014";
        const parts = metricPath.split(".");
        let v = w;
        for (const p of parts) { v = v?.[p]; }
        return v;
    }

    function metricBox(label, val7, val30, colorize, tipKey) {
        let v7str = formatMetricVal(val7);
        let v30str = formatMetricVal(val30);
        let v7cls = "", v30cls = "";
        if (colorize) {
            v7cls = Number(val7) > 0 ? "chip-pos" : Number(val7) < 0 ? "chip-neg" : "";
            v30cls = Number(val30) > 0 ? "chip-pos" : Number(val30) < 0 ? "chip-neg" : "";
        }
        const tipAttr = tipKey ? tip(tipKey) : "";
        return `<div class="stat-box" style="margin-top:0;">
            <div class="stat-box-label"${tipAttr}>${label}</div>
            <div style="font-size:12px;">
                <div style="display:flex;justify-content:space-between;margin-bottom:2px;">
                    <span style="color:#808080;">7d:</span>
                    <span class="${v7cls}" style="font-weight:bold;">${v7str}</span>
                </div>
                <div style="display:flex;justify-content:space-between;">
                    <span style="color:#808080;">30d:</span>
                    <span class="${v30cls}" style="font-weight:bold;">${v30str}</span>
                </div>
            </div>
        </div>`;
    }

    function formatMetricVal(v) {
        if (v == null || v === "\u2014") return "\u2014";
        const n = Number(v);
        if (!Number.isFinite(n)) return "\u2014";
        return n.toFixed(1);
    }

    // Sold Rate from estimated
    const soldRate7 = est["7d"]?.metrics?.["sold-rate-est"];
    const soldRate30 = est["30d"]?.metrics?.["sold-rate-est"];

    let html = "";
    html += metricBox("Avg Active",     val("7d", "raw.avg-active"), val("30d", "raw.avg-active"), false, "avg_active");
    html += metricBox("Avg Ended/Sold", val("7d", "raw.avg-ended"),  val("30d", "raw.avg-ended"),  false, "avg_ended");
    html += metricBox("Net Flow",       val("7d", "metrics.net-flow"), val("30d", "metrics.net-flow"), true,  "net_flow");

    // Sold rate
    let srHtml = `<div class="stat-box" style="margin-top:0;">
        <div class="stat-box-label"${tip("sold_rate_est")}>Sold Rate Est</div>
        <div style="font-size:12px;">
            <div style="display:flex;justify-content:space-between;margin-bottom:2px;">
                <span style="color:#808080;">7d:</span>
                <span style="font-weight:bold;">${formatMetricVal(soldRate7)}</span>
            </div>
            <div style="display:flex;justify-content:space-between;">
                <span style="color:#808080;">30d:</span>
                <span style="font-weight:bold;">${formatMetricVal(soldRate30)}</span>
            </div>
        </div>
    </div>`;
    html += srHtml;

    container.innerHTML = html;
}

/* ============================================================
   D) PSA POPULATION
   ============================================================ */

function renderPSAPopulation() {
    const histPsa = cardData["history-psa"] || [];
    const latest = histPsa.length ? histPsa[histPsa.length - 1] : null;

    const p10 = latest?.["10-base"];
    const p9 = latest?.["9-base"];
    const total = latest?.["total-base"];
    const gemPctVal = latest?.["gem-pct"];

    document.getElementById("psa-10-count").textContent = num(p10);
    document.getElementById("psa-9-count").textContent = num(p9);
    document.getElementById("psa-total").textContent = num(total);

    const gemN = Number(gemPctVal);
    if (Number.isFinite(gemN)) {
        const barPct = Math.min(gemN * 100, 100);
        document.getElementById("gem-rate-bar").style.width = barPct + "%";
        document.getElementById("gem-rate-pct").textContent = (gemN * 100).toFixed(1) + "%";
    }

    // PSA Pop chart over time
    if (histPsa.length >= 2) {
        document.getElementById("psa-chart-wrap").style.display = "block";
        const labels = histPsa.map(h => h.date);
        const datasets = [
            {
                label: "PSA 10",
                data: histPsa.map(h => h["10-base"] ?? null),
                borderColor: PALETTE.psa10Pop,
                borderWidth: 2, pointRadius: 0, tension: 0.3, fill: false, spanGaps: true,
            },
            {
                label: "PSA 9",
                data: histPsa.map(h => h["9-base"] ?? null),
                borderColor: PALETTE.psa9Pop,
                borderWidth: 2, pointRadius: 0, tension: 0.3, fill: false, spanGaps: true,
            },
            {
                label: "Total",
                data: histPsa.map(h => h["total-base"] ?? null),
                borderColor: PALETTE.totalPop,
                borderWidth: 1.5, borderDash: [4, 3], pointRadius: 0, tension: 0.3, fill: false, spanGaps: true,
            },
        ];

        if (psaChart) psaChart.destroy();
        const ctx = document.getElementById("chart-psa").getContext("2d");
        psaChart = new Chart(ctx, {
            type: "line",
            data: { labels, datasets },
            options: {
                ...CRT_CHART_DEFAULTS,
                plugins: {
                    ...CRT_CHART_DEFAULTS.plugins,
                    tooltip: {
                        ...CRT_CHART_DEFAULTS.plugins.tooltip,
                        callbacks: {
                            label: ctx => {
                                const v = ctx.parsed.y;
                                return v != null ? `${ctx.dataset.label}: ${num(v)}` : null;
                            }
                        }
                    }
                }
            }
        });
    }
}

/* ============================================================
   E) PRICE SOURCES COMPARISON
   ============================================================ */

function renderPriceSources() {
    const d = cardData;
    const tbody = document.getElementById("source-tbody");

    function latest(arr, key) {
        if (!arr || !arr.length) return null;
        const last = arr[arr.length - 1];
        return last?.[key] ?? null;
    }

    const sources = [
        {
            name: "PriceCharting",
            raw: d["raw-price"],
            psa9: d["psa-9-price"],
            psa10: d["psa-10-price"],
        },
        {
            name: "eBay Derived",
            raw: latest(d["history-ebay-derived"], "d-raw-price"),
            psa9: latest(d["history-ebay-derived"], "d-psa-9-price"),
            psa10: latest(d["history-ebay-derived"], "d-psa-10-price"),
        },
        {
            name: "JustTCG",
            raw: latest(d["history-justtcg"], "j-raw-price"),
            psa9: null,
            psa10: null,
        },
        {
            name: "Collectrics",
            raw: latest(d["history-collectrics"], "c-raw-price"),
            psa9: latest(d["history-collectrics"], "c-psa-9-price"),
            psa10: latest(d["history-collectrics"], "c-psa-10-price"),
        },
    ];

    tbody.innerHTML = sources.map(s => `
        <tr>
            <td style="font-weight:bold;">${s.name}</td>
            <td class="text-right text-mono">${money(s.raw)}</td>
            <td class="text-right text-mono">${money(s.psa9)}</td>
            <td class="text-right text-mono">${money(s.psa10)}</td>
        </tr>
    `).join("");
}

/* ============================================================
   RARITY CORNER — pull rate + gem rate + composite grade
   ============================================================ */

function renderRarityGrid() {
    const grid = document.getElementById("rarity-grid");
    if (!grid) return;
    const rg = cardData?.["rarity-grade"];
    if (!rg || cardData?.["is-sealed"]) { grid.style.display = "none"; return; }

    const {
        "specific-pull-rate": specificPull,
        "gem-rate": gemRate,
        "combined-odds": combined,
        "grade-basis": basis,
        grade,
    } = rg;

    // Always render the grid — fields that are missing show as "—"
    grid.style.display = "";

    const fmtOdds = (p) => {
        if (p == null) return "—";
        if (p >= 0.01) return `${(p * 100).toFixed(1)}%`;
        return `1 in ${Math.round(1 / p).toLocaleString()}`;
    };

    const variantLabel = rg["variant-label"];
    const gEl = document.getElementById("stat-rarity-grade");

    if (basis === "variant") {
        // Non-chase card — replace the grade glyph with the variant name
        gEl.textContent = variantLabel || "—";
        gEl.className = "stat-box-value grade-C";
        gEl.style.fontSize = variantLabel && variantLabel.length > 10 ? "13px" : "15px";
        gEl.style.lineHeight = "1.15";
    } else {
        gEl.textContent = grade || "—";
        gEl.className = `stat-box-value grade-${(grade || "C").replace("+", "-plus")}`;
        gEl.style.fontSize = "22px";
        gEl.style.lineHeight = "1";
    }

    const basisEl = gEl.parentElement?.querySelector(".stat-box-label");
    if (basisEl) {
        const suffixMap = {
            "combined":   "",
            "pull-only":  " (pull-only)",
            "variant":    " (non-chase)",
        };
        basisEl.childNodes[0].textContent = "Rarity Grade" + (suffixMap[basis] || "");
    }

    document.getElementById("stat-pull-rate").textContent  = fmtOdds(specificPull);
    document.getElementById("stat-gem-rate").textContent   = gemRate != null ? `${Math.round(gemRate * 100)}%` : "—";
    document.getElementById("stat-psa10-odds").textContent = fmtOdds(combined);

    renderCulturalRelevance();
    renderPromoRarityDisclaimer();
}

/* ============================================================
   CULTURAL RELEVANCE — 0..1 score rendered under the rarity grid
   ============================================================ */
const PROMO_SET_CODES = new Set(["PROMO", "KRP", "JPP"]);

function renderCulturalRelevance() {
    const grid = document.getElementById("cultural-grid");
    if (!grid || !cardData) return;
    const compute = window.WishlistStore && window.WishlistStore.culturalImpactScore;
    if (!compute) { grid.style.display = "none"; return; }

    const raw = compute(cardData);
    if (!Number.isFinite(raw)) { grid.style.display = "none"; return; }

    grid.style.display = "";
    const pct = Math.round(raw * 100);
    const valEl = document.getElementById("stat-cultural-relevance");
    const tierEl = document.getElementById("stat-cultural-tier");

    valEl.textContent = `${pct}%`;
    // Tier bucketing — matches rubric used elsewhere (wishlist rationale,
    // must-buy): 75+ iconic, 45+ strong, 20+ moderate, else weak.
    let tierLabel, tierClass;
    if      (raw >= 0.75) { tierLabel = "Iconic";   tierClass = "grade-S";  }
    else if (raw >= 0.45) { tierLabel = "Strong";   tierClass = "grade-A";  }
    else if (raw >= 0.20) { tierLabel = "Moderate"; tierClass = "grade-B";  }
    else                  { tierLabel = "Weak";     tierClass = "grade-C";  }
    valEl.className = `stat-box-value ${tierClass}`;
    tierEl.textContent = tierLabel;
}

function renderPromoRarityDisclaimer() {
    const el = document.getElementById("promo-rarity-disclaimer");
    if (!el || !cardData) return;
    const setCode = cardData["set-code"];
    const name = (cardData["product-name"] || "").toLowerCase();
    const isPromo = PROMO_SET_CODES.has(setCode)
                 || /\b(promo|sv-?p|sm-?p|xy-?p|s-?p)\b/.test(name);
    el.style.display = isPromo ? "" : "none";
}

/* ============================================================
   F) SET-ALPHA LINKAGE (panel + compact stat box + popover)
   ============================================================ */

function renderAlphaLinkage() {
    const wrap = document.getElementById("alpha-linkage-wrap");
    if (!wrap) return;
    const link = cardData["alpha-linkage"];
    if (!link || !link["alpha-card-id"] || cardData["is-sealed"]) {
        wrap.style.display = "none";
        return;
    }
    wrap.style.display = "";

    const isSelf = link["is-self"];
    const explainer = document.getElementById("alpha-linkage-explainer");
    if (isSelf) {
        explainer.textContent = "this IS the set alpha — other chase cards in this set tend to follow its moves";
    } else {
        explainer.textContent = "how this card tracks the top chase card in its set — based on the alpha-beta thesis backtest (mean lead ρ +0.266 across 17 sets)";
    }

    const alphaLink = document.getElementById("alpha-link");
    alphaLink.textContent = link["alpha-name"] || "---";
    alphaLink.href = isSelf ? "#" : `card.html?id=${encodeURIComponent(link["alpha-card-id"])}`;
    alphaLink.style.textDecoration = isSelf ? "none" : "underline";
    alphaLink.style.cursor = isSelf ? "default" : "pointer";

    const c = Number(link["contemp-corr"]);
    const l = Number(link["lead-corr"]);
    document.getElementById("alpha-contemp-cell").textContent = Number.isFinite(c) ? c.toFixed(2) : "---";
    document.getElementById("alpha-lead-cell").textContent = Number.isFinite(l) ? (l >= 0 ? "+" : "") + l.toFixed(2) : "---";

    const cur = Number(link["alpha-psa10-current"]);
    const a30 = Number(link["alpha-psa10-30d-ago"]);
    const a90 = Number(link["alpha-psa10-90d-ago"]);
    const r30 = Number.isFinite(cur) && Number.isFinite(a30) && a30 > 0 ? cur / a30 - 1 : null;
    const r90 = Number.isFinite(cur) && Number.isFinite(a90) && a90 > 0 ? cur / a90 - 1 : null;
    document.getElementById("alpha-30d-cell").textContent =
        r30 == null ? "---" : `${r30 >= 0 ? "+" : ""}${Math.round(r30 * 100)}%`;
    document.getElementById("alpha-90d-cell").textContent =
        r90 == null ? "---" : `${r90 >= 0 ? "+" : ""}${Math.round(r90 * 100)}%`;

    document.getElementById("alpha-linkage-verdict-body").textContent = verdictFor(isSelf, c, l, r30, r90);
}

function verdictFor(isSelf, contemp, lead, r30, r90) {
    if (isSelf) {
        if (r30 != null && r30 <= -0.10) return `You own the set alpha — and it's down ${Math.round(r30 * 100)}% in 30 days. Expect beta cards in this set to follow lower over the next month.`;
        if (r30 != null && r30 >= 0.10) return `You own the set alpha and it's up +${Math.round(r30 * 100)}% in 30 days. Betas in this set typically ride the wave with a 1-month lag.`;
        return "You own the set alpha — its trajectory leads the other chase cards in this set.";
    }
    if (!Number.isFinite(contemp)) return "Not enough monthly overlap yet to judge linkage.";
    let strength;
    if (contemp >= 0.70) strength = "tight co-movement";
    else if (contemp >= 0.50) strength = "meaningful beta";
    else if (contemp >= 0.30) strength = "modest linkage";
    else                      strength = "weak / mostly independent";
    const parts = [`Contemp ρ=${contemp.toFixed(2)} — ${strength}.`];
    if (r30 != null && r30 <= -0.10 && contemp >= 0.50) parts.push(`⚠ Set alpha down ${Math.round(r30 * 100)}% in 30d. Given the linkage strength, a drag on this card over the next month is likely.`);
    else if (r30 != null && r30 <= -0.03 && contemp >= 0.50 && (r90 == null || r90 < 0)) parts.push(`⚠ Set alpha softening (${Math.round(r30 * 100)}% 30d). Watch for beta drag.`);
    else if (r30 != null && r30 >= 0.05 && contemp >= 0.50) parts.push(`Alpha trending up (+${Math.round(r30 * 100)}% 30d) — positive tailwind for this card.`);
    else if (contemp >= 0.50) parts.push(`Alpha flat — this card's move will likely be idiosyncratic over the next month.`);
    else parts.push(`Linkage too weak to use as a forecasting signal — trade this card on its own chart.`);
    if (Number.isFinite(lead) && lead >= 0.30) parts.push(`Lead ρ=${(lead >= 0 ? "+" : "") + lead.toFixed(2)} — the alpha's move THIS month has historically predicted a same-direction move in this card NEXT month.`);
    return parts.join(" ");
}

function renderAlphaStatBox() {
    const wrap = document.getElementById("stat-alpha-wrap");
    const label = document.getElementById("stat-alpha-label");
    const selfTag = document.getElementById("stat-alpha-self-tag");
    if (!wrap || !label) return;

    const link = cardData?.["alpha-linkage"];
    if (!link || !link["alpha-card-id"] || cardData["is-sealed"]) {
        label.textContent = "—";
        label.title = "No alpha linkage (insufficient history for this set)";
        wrap.style.pointerEvents = "none";
        wrap.style.opacity = "0.55";
        return;
    }
    wrap.style.pointerEvents = "";
    wrap.style.opacity = "";

    const isSelf = !!link["is-self"];
    const corr = Number(link["contemp-corr"]);
    const alphaShort = (link["alpha-name"] || "alpha").replace(/\s*#\d+\s*$/, "").trim();

    if (isSelf) {
        selfTag.style.display = "";
        label.textContent = "this card";
        label.title = "This IS the set alpha — hover to see the betas tracking it";
    } else {
        selfTag.style.display = "none";
        label.textContent = `${alphaShort}${Number.isFinite(corr) ? ` ρ${corr.toFixed(2)}` : ""}`;
        label.title = `Tracks ${link["alpha-name"]} with correlation ${corr.toFixed(2)}. Hover to see peers.`;
    }

    if (!wrap.dataset.bound) {
        wrap.dataset.bound = "1";
        let hoverTimer = null;
        wrap.addEventListener("mouseenter", () => { hoverTimer = setTimeout(openAlphaPopover, 120); });
        wrap.addEventListener("mouseleave", () => {
            if (hoverTimer) { clearTimeout(hoverTimer); hoverTimer = null; }
            setTimeout(() => {
                const pop = document.getElementById("alpha-popover");
                if (pop && !pop.matches(":hover")) pop.style.display = "none";
            }, 150);
        });
        wrap.addEventListener("click", (e) => {
            if (e.target.id === "alpha-popover" || e.target.closest("#alpha-popover")) return;
            if (isSelf) { openAlphaPopover(); return; }
            const alphaId = cardData?.["alpha-linkage"]?.["alpha-card-id"];
            if (alphaId) window.location.href = `card.html?id=${encodeURIComponent(alphaId)}`;
        });
        const pop = document.getElementById("alpha-popover");
        if (pop) pop.addEventListener("mouseleave", () => { pop.style.display = "none"; });
    }
}

let alphaPeersCache = null;
async function openAlphaPopover() {
    const pop = document.getElementById("alpha-popover");
    const title = document.getElementById("alpha-popover-title");
    const body = document.getElementById("alpha-popover-body");
    if (!pop || !title || !body) return;
    pop.style.display = "block";
    if (alphaPeersCache) { fillAlphaPopover(alphaPeersCache); return; }
    title.textContent = "Loading peers…";
    body.textContent = "";
    try {
        const res = await fetch(`${API_BASE}/card/${encodeURIComponent(cardData.id)}/peers?min_corr=0.60`);
        const json = await res.json();
        alphaPeersCache = json;
        fillAlphaPopover(json);
    } catch (err) {
        title.textContent = "Couldn't load peers";
        body.textContent = "";
    }
}

function fillAlphaPopover(data) {
    const title = document.getElementById("alpha-popover-title");
    const body = document.getElementById("alpha-popover-body");
    body.textContent = "";
    const allPeers = data?.peers || [];
    // First row is the card being viewed; the rest are its peers ranked by ρ
    const self = allPeers.find(p => p["is-self"]);
    const others = allPeers.filter(p => !p["is-self"]);

    if (!others.length) {
        const selfName = self?.["product-name"] || "This card";
        title.textContent = `${selfName} — no strong peers`;
        const note = document.createElement("div");
        note.style.padding = "6px";
        note.style.color = "#666";
        note.textContent = "No other cards in this set correlate above ρ=0.60 with this one.";
        body.appendChild(note);
        return;
    }

    // Cap at 8 so the popover stays scannable even for the tightest clusters
    const peers = [];
    if (self) peers.push(self);
    for (const p of others.slice(0, 8)) peers.push(p);

    const selfShort = (self?.["product-name"] || "this card").replace(/\s*#\d+\s*$/, "").trim();
    title.textContent = `${selfShort} — correlated cluster`;

    for (const p of peers) {
        const row = document.createElement("div");
        row.className = "alpha-popover-row";
        if (p["is-alpha"]) row.classList.add("is-alpha");
        if (p["is-self"])  row.classList.add("is-self");
        const name = document.createElement("span");
        name.textContent = (p["is-alpha"] ? "★ " : "") + (p["product-name"] || `#${p["card-number"]}`) + (p["is-self"] ? "  (this card)" : "");
        row.appendChild(name);
        const corrEl = document.createElement("span");
        corrEl.className = "alpha-corr";
        if (p["is-self"]) {
            corrEl.textContent = "this card";
        } else if (Number.isFinite(p["corr"])) {
            const weak = p["weak-alpha"] ? " (weak)" : "";
            corrEl.textContent = `ρ ${Number(p["corr"]).toFixed(2)}${p["is-alpha"] ? " ★" : ""}${weak}`;
        } else if (p["is-alpha"]) {
            corrEl.textContent = "★ alpha";
        } else {
            corrEl.textContent = "—";
        }
        row.appendChild(corrEl);
        const retEl = document.createElement("span");
        retEl.className = "alpha-ret";
        const r = p["psa10-30d-return"];
        if (Number.isFinite(r)) { retEl.textContent = (r >= 0 ? "+" : "") + Math.round(r * 100) + "%"; retEl.classList.add(r >= 0 ? "up" : "down"); }
        else retEl.textContent = "—";
        row.appendChild(retEl);
        row.addEventListener("click", () => {
            if (p["is-self"]) return;
            window.location.href = `card.html?id=${encodeURIComponent(p.id)}`;
        });
        body.appendChild(row);
    }
    const footer = document.createElement("div");
    footer.className = "alpha-popover-footer";
    footer.textContent = "Cards in this set that correlate ρ ≥ 0.60 with this card. ★ = set alpha. Click to open.";
    body.appendChild(footer);
}

/* ============================================================
   INTERVAL BUTTON SETUP
   ============================================================ */

/* ============================================================
   MODEL PROJECTION — 90-day return forecast with confidence band
   ============================================================ */

function renderModelProjection() {
    const container = document.getElementById("model-projection");
    if (!container) return;

    const proj = cardData._projection;
    if (!proj) {
        container.style.display = "none";
        return;
    }

    container.style.display = "";
    const ret = proj["projected-return"];
    const lo = proj["confidence-low"];
    const hi = proj["confidence-high"];
    const width = proj["confidence-width"] || (hi - lo);
    const contribs = proj["feature-contributions"] || {};

    // Projected return
    const retEl = document.getElementById("proj-return");
    if (retEl && ret !== null && Number.isFinite(ret)) {
        const sign = ret > 0 ? "+" : "";
        retEl.textContent = sign + (ret * 100).toFixed(1) + "%";
        retEl.style.color = ret > 0.05 ? "#006400" : ret < -0.05 ? "#880000" : "#404040";
    }

    // Confidence band
    const bandEl = document.getElementById("proj-band");
    if (bandEl && lo !== null && hi !== null) {
        bandEl.textContent = (lo * 100).toFixed(0) + "% to " + (hi * 100).toFixed(0) + "%";
    }

    // Confidence label + abstention UX for LOW cases (PAIR: low-confidence is
    // a first-class state; hand control back to the user, don't force a number).
    // Thresholds calibrated meaning-first: LOW means conspicuously uncertain
    // (top ~8% of cards by disagreement width), not mild disagreement.
    // v2_0 bootstrap-ensemble p25/p75 widths — max 0.11, median 0.04.
    const confEl = document.getElementById("proj-confidence");
    const isLow = width >= 0.08;
    const isMedium = width >= 0.05 && width < 0.08;
    if (confEl) {
        const label = isLow ? "LOW" : isMedium ? "MEDIUM" : "HIGH";
        confEl.textContent = label;
        confEl.style.color = isLow ? "#880000" : isMedium ? "#806000" : "#006400";
    }

    // Abstention treatment: when confidence is LOW, de-emphasize the point
    // estimate and surface what the user should check themselves.
    const abstainEl = document.getElementById("proj-abstain");
    if (abstainEl) {
        while (abstainEl.firstChild) abstainEl.removeChild(abstainEl.firstChild);
        if (isLow) {
            if (retEl) retEl.style.opacity = "0.4";
            abstainEl.style.display = "";
            const hdr = document.createElement("strong");
            hdr.style.color = "#880000";
            hdr.textContent = "Interval too wide to act on. ";
            abstainEl.appendChild(hdr);
            const widthPts = Math.round(width * 1000) / 10;
            abstainEl.appendChild(document.createTextNode(
                "The bootstrap-ensemble p25–p75 band spans " + widthPts + " points — this card's " +
                "models disagree more than typical. Don't size a position on the point estimate. " +
                "Check: recent sales volume, grading population trends, any catalysts (anime, set rotation) " +
                "the model can't see."
            ));
        } else {
            if (retEl) retEl.style.opacity = "1";
            abstainEl.style.display = "none";
        }
    }

    // Feature waterfall — top 5 contributors
    const waterfallEl = document.getElementById("proj-waterfall");
    if (waterfallEl) {
        while (waterfallEl.firstChild) waterfallEl.removeChild(waterfallEl.firstChild);
        const entries = Object.entries(contribs).slice(0, 5);
        for (const [name, val] of entries) {
            const row = document.createElement("div");
            row.style.cssText = "display:flex;align-items:center;gap:6px;padding:2px 0;font-size:11px;";

            const nameSpan = document.createElement("span");
            nameSpan.style.cssText = "width:130px;text-align:right;font-weight:bold;color:#404040;";
            nameSpan.textContent = name.replace(/_/g, " ");

            const valSpan = document.createElement("span");
            valSpan.style.cssText = "font-variant-numeric:tabular-nums;min-width:60px;";
            const sign = val > 0 ? "+" : "";
            valSpan.textContent = sign + (val * 100).toFixed(1) + "%";
            valSpan.style.color = val > 0 ? "#006400" : "#880000";

            row.appendChild(nameSpan);
            row.appendChild(valSpan);
            waterfallEl.appendChild(row);
        }
    }

    // Model version
    const verEl = document.getElementById("proj-version");
    if (verEl) {
        verEl.textContent = "Model: " + (proj["model-version"] || "unknown") +
            " | As of: " + (proj["as-of"] || "unknown");
    }

    // Data freshness / staleness indicator.
    // >36h since last run => visible warning + mute the projection block.
    applyStalenessIndicator(
        container,
        cardData._dataAsOf || proj["as-of"] || null,
        cardData._lastPipelineRunAt || null
    );

    // Show the "Where this model fails" block whenever a projection renders.
    // (PAIR: calibrated trust > maximum trust — teach users the model's weak spots.)
    renderModelCaveats(proj, contribs);
}

/* ============================================================
   WHERE THIS MODEL FAILS — calibrated-trust block per PAIR.
   Base caveats are static (unmodeled signal classes). Per-card
   caveats are derived from the projection itself: thin listing
   history, sparse SHAP contributions, out-of-distribution rarity.
   ============================================================ */
function renderModelCaveats(proj, contribs) {
    const container = document.getElementById("model-caveats");
    if (!container) return;
    container.style.display = "";

    const perCard = document.getElementById("caveats-per-card");
    if (!perCard) return;
    while (perCard.firstChild) perCard.removeChild(perCard.firstChild);

    const warnings = [];

    // Sparse SHAP = few features carried meaningful weight => thin signal
    const nContribs = contribs ? Object.keys(contribs).length : 0;
    if (nContribs > 0 && nContribs < 5) {
        warnings.push(
            "Only " + nContribs + " feature(s) drove this projection — the model had thin signal on this card. " +
            "Treat the point estimate as illustrative, not decisive."
        );
    }

    // Wide interval is already flagged in the abstention block above; this
    // captures the "MED but leaning wide" case.
    const w = proj["confidence-width"];
    if (Number.isFinite(w) && w >= 0.22 && w < 0.30) {
        warnings.push(
            "Interval width " + Math.round(w * 100) + " points is on the wider side of MEDIUM — adjacent to LOW-confidence territory."
        );
    }

    // Staleness => fresh data missing
    if (proj["as-of"]) {
        const f = freshnessLabel(proj["as-of"]);
        if (f.hoursAgo > 72) {
            warnings.push(
                "Projection is " + Math.round(f.hoursAgo / 24) + " days old. Real market state may have diverged."
            );
        }
    }

    if (warnings.length === 0) return;

    const hdr = document.createElement("div");
    hdr.style.cssText = "font-size:10px;font-weight:bold;text-transform:uppercase;color:#606060;margin-bottom:6px;";
    hdr.textContent = "Specific gaps for this card";
    perCard.appendChild(hdr);

    const ul = document.createElement("ul");
    ul.style.cssText = "margin:0;padding-left:20px;line-height:1.6;";
    for (const w of warnings) {
        const li = document.createElement("li");
        li.textContent = w;
        ul.appendChild(li);
    }
    perCard.appendChild(ul);
}

/* Tournament play rendering — shows when the card has recent competitive
   appearances in the tournament_appearances table. Section stays hidden
   when the card has no tournament signal. */
function renderTournamentPlay() {
    const container = document.getElementById("tournament-play");
    if (!container) return;
    const tp = cardData["tournament-play"];
    if (!tp || !tp["appearances-90d"]) {
        container.style.display = "none";
        return;
    }
    container.style.display = "";

    const apps = tp["appearances-90d"] || 0;
    const top8 = tp["top8-appearances-90d"] || 0;
    const tours = tp["distinct-tournaments-90d"] || 0;
    const lastSeen = tp["last-seen"] || "";

    const set = (id, text) => {
        const el = document.getElementById(id);
        if (el) el.textContent = text;
    };
    set("tp-apps-90d", String(apps));
    set("tp-top8-90d", String(top8));
    set("tp-tournaments-90d", String(tours));
    set("tp-last-seen", lastSeen);
}

/* Compute "Updated Xh ago" label + stale flag for a UTC ISO timestamp. */
function freshnessLabel(ts) {
    if (!ts) return { label: "Updated: unknown", hoursAgo: Infinity, stale: true };
    // Accept "YYYY-MM-DD", "YYYY-MM-DDTHH:MM:SS", "...Z" variants.
    const s = /Z$/.test(ts) || /[+-]\d\d:?\d\d$/.test(ts) ? ts : ts + "Z";
    const t = Date.parse(s);
    if (!Number.isFinite(t)) return { label: `Updated: ${ts}`, hoursAgo: Infinity, stale: true };
    const hoursAgo = (Date.now() - t) / 3600000;
    let label;
    if (hoursAgo < 1)       label = "Updated <1h ago";
    else if (hoursAgo < 48) label = `Updated ${Math.round(hoursAgo)}h ago`;
    else                    label = `Updated ${Math.round(hoursAgo / 24)}d ago`;
    return { label, hoursAgo, stale: hoursAgo > 36 };
}

function applyStalenessIndicator(container, projAsOf, lastRunAt) {
    // Use most recent of projection as-of vs last pipeline run as the signal.
    const candidates = [projAsOf, lastRunAt].filter(Boolean);
    let pick = null, pickHrs = Infinity;
    for (const c of candidates) {
        const f = freshnessLabel(c);
        if (f.hoursAgo < pickHrs) { pick = f; pickHrs = f.hoursAgo; }
    }
    const f = pick || freshnessLabel(null);

    // Mute block when stale.
    container.style.opacity = f.stale ? "0.65" : "";
    container.style.filter = f.stale ? "grayscale(0.4)" : "";

    // Inject or update a freshness row at the top of the projection body.
    let badge = document.getElementById("proj-freshness");
    if (!badge) {
        badge = document.createElement("div");
        badge.id = "proj-freshness";
        badge.style.cssText =
            "font-size:11px;padding:4px 8px;margin-bottom:6px;border:1px solid;" +
            "font-family:inherit;display:flex;align-items:center;gap:6px;";
        // Insert near the top of the window-body content.
        const body = container.querySelector(".window-body") || container;
        body.insertBefore(badge, body.firstChild);
    }
    while (badge.firstChild) badge.removeChild(badge.firstChild);

    const icon = document.createElement("span");
    icon.textContent = f.stale ? "\u26A0" : "\u25CF";  // warn vs dot
    icon.style.fontWeight = "bold";
    const text = document.createElement("span");
    text.textContent = f.stale
        ? `STALE DATA — ${f.label}. Pipeline may have failed; numbers may be out of date.`
        : f.label;
    badge.appendChild(icon);
    badge.appendChild(text);

    if (f.stale) {
        badge.style.background = "#fff4d6";
        badge.style.borderColor = "#c08000";
        badge.style.color = "#6e4800";
    } else {
        badge.style.background = "#eef6ee";
        badge.style.borderColor = "#6a9a6a";
        badge.style.color = "#385a38";
    }
}

function setupIntervalButtons() {
    // Price chart intervals
    document.querySelectorAll("#price-intervals .interval-btn").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll("#price-intervals .interval-btn").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            currentPriceRange = btn.dataset.range;
            updatePriceChart();
        });
    });

    // Listing chart intervals
    document.querySelectorAll("#listing-intervals .interval-btn").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll("#listing-intervals .interval-btn").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            currentListingRange = btn.dataset.range;
            updateListingChart();
        });
    });
}
