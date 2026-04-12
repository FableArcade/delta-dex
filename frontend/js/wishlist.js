/**
 * Wishlist page controller.
 *
 * SECURITY NOTE: this file uses innerHTML to build table rows. Every piece
 * of user-sourced data (card name, set code, image URL, card id, rationale
 * strings) is passed through the esc() helper below, which HTML-escapes
 * &, <, >, ", '. Numeric fields (price, fit score) are either produced by
 * Number formatting or our own computed values and are not user-controllable.
 * This matches the convention used throughout card_leaderboard.js.
 *
 * Flow:
 *   1. Load the full card index from /api/card_index (once per session).
 *   2. Read the wishlisted card IDs from localStorage via WishlistStore.
 *   3. For each wishlisted card, compute a fit score given the current
 *      budget + horizon.
 *   4. Render as a priority-ordered table.
 *
 * Re-runs on any toolbar change (budget, horizon, sort). Per-row × buttons
 * remove cards. Clicking a row navigates to the card detail page.
 */
(function() {
    "use strict";

    const API_BASE = "/api";
    const PREFS_KEY = "pokemon-analytics.wishlist.prefs";

    let allCardsById = null;
    let setReturns = null;    // fetched from /api/set_returns
    let projections = {};     // fetched from /api/model/projections, keyed by card_id
    let loaded = false;
    let currentMode = "fit";  // "fit" | "roi"

    // ------------------------------------------------------------------
    // Prefs — persist budget + horizon + sort across visits
    // ------------------------------------------------------------------

    function loadPrefs() {
        try {
            const raw = localStorage.getItem(PREFS_KEY);
            if (!raw) return {};
            return JSON.parse(raw) || {};
        } catch (e) { return {}; }
    }
    function savePrefs(p) {
        try { localStorage.setItem(PREFS_KEY, JSON.stringify(p)); } catch (e) {}
    }

    // ------------------------------------------------------------------
    // Formatters
    // ------------------------------------------------------------------

    function money(x) {
        if (x == null || !Number.isFinite(Number(x))) return "\u2014";
        return Number(x).toLocaleString("en-US", {
            style: "currency", currency: "USD",
            minimumFractionDigits: 0, maximumFractionDigits: 0,
        });
    }

    // HTML-escape user-sourced strings before splicing into innerHTML.
    // Same helper as card_leaderboard.js / sealed_leaderboard.js.
    function esc(s) {
        if (s == null) return "";
        return String(s)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    // ------------------------------------------------------------------
    // Fetch
    // ------------------------------------------------------------------

    async function loadCardIndex() {
        if (allCardsById) return allCardsById;
        setStatus("Loading card data + model projections...");
        try {
            const [cardRes, projRes] = await Promise.all([
                fetch(`${API_BASE}/card_index`),
                fetch(`${API_BASE}/model/projections`).catch(() => null),
            ]);
            if (!cardRes.ok) throw new Error(`HTTP ${cardRes.status}`);
            const data = await cardRes.json();
            const cards = Array.isArray(data) ? data : (data.cards || data.rows || []);
            allCardsById = {};
            for (const c of cards) {
                if (c && c.id != null) allCardsById[String(c.id)] = c;
            }
            if (projRes && projRes.ok) {
                const projData = await projRes.json();
                projections = projData.projections || {};
            }
            setStatus(`Loaded ${cards.length} cards · ${Object.keys(projections).length} projections.`);
        } catch (e) {
            console.error("card_index fetch failed", e);
            setStatus(`Error loading card data: ${e.message}`);
            allCardsById = {};
        }
        return allCardsById;
    }

    async function loadSetReturns() {
        if (setReturns) return setReturns;
        try {
            const res = await fetch(`${API_BASE}/set_returns`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            setReturns = data.sets || {};
        } catch (e) {
            // Non-fatal: scorer handles missing setReturns gracefully.
            console.warn("set_returns fetch failed — setAlpha factor disabled:", e);
            setReturns = {};
        }
        return setReturns;
    }

    function setStatus(msg) {
        const el = document.getElementById("wl-status");
        if (el) el.textContent = msg;
    }

    // ------------------------------------------------------------------
    // Toolbar
    // ------------------------------------------------------------------

    function readToolbar() {
        const budgetEl  = document.getElementById("wl-budget");
        const horizonEl = document.getElementById("wl-horizon");
        const sortEl    = document.getElementById("wl-sort");
        return {
            budget: Number(budgetEl?.value) || 0,
            horizon: horizonEl?.value || "medium",
            sort: sortEl?.value || "fit",
        };
    }

    function applyPrefsToToolbar() {
        const p = loadPrefs();
        if (p.budget != null) document.getElementById("wl-budget").value = p.budget;
        if (p.horizon)        document.getElementById("wl-horizon").value = p.horizon;
        if (p.sort)           document.getElementById("wl-sort").value = p.sort;
    }

    function persistToolbar() { savePrefs(readToolbar()); }

    // ------------------------------------------------------------------
    // Sorting
    // ------------------------------------------------------------------

    function sortRows(rows, key) {
        const copy = rows.slice();
        if (key === "fit") {
            copy.sort((a, b) => {
                // Filtered/over-budget cards sink to the bottom
                const af = a.scored.fitScore == null ? -Infinity : a.scored.fitScore;
                const bf = b.scored.fitScore == null ? -Infinity : b.scored.fitScore;
                return bf - af;
            });
        } else if (key === "roi") {
            // Pure ROI mode: sort by projected return × confidence weight
            copy.sort((a, b) => {
                const ar = a.roiScore == null ? -Infinity : a.roiScore;
                const br = b.roiScore == null ? -Infinity : b.roiScore;
                return br - ar;
            });
        } else if (key === "added") {
            copy.sort((a, b) => (b.entry.addedAt || "").localeCompare(a.entry.addedAt || ""));
        } else if (key === "price_asc") {
            copy.sort((a, b) => (a.price || 0) - (b.price || 0));
        } else if (key === "price_desc") {
            copy.sort((a, b) => (b.price || 0) - (a.price || 0));
        }
        return copy;
    }

    /**
     * Compute the Pure ROI score for a card.
     *
     * Uses the model's projected 90-day return × a confidence multiplier
     * that rewards tighter confidence bands. Horizon-dependent but NOT
     * budget-dependent — this is "what's the best investment" independent
     * of how much money you have.
     *
     * Returns { roiScore, projReturn, confLow, confHigh, confWidth, rationale, confLabel }
     */
    function computeRoiScore(card, horizon) {
        const proj = projections[String(card.id)];
        if (!proj) return { roiScore: null, rationale: ["No model projection available"] };

        const projReturn = proj["projected-return"];
        const confLow = proj["confidence-low"];
        const confHigh = proj["confidence-high"];
        const confWidth = proj["confidence-width"] || (confHigh - confLow);

        if (!Number.isFinite(projReturn)) return { roiScore: null, rationale: ["No model projection available"] };

        // Confidence multiplier: tight band (< 15%) = 1.0, medium (15-30%) = 0.7, wide = 0.4
        const confMult = confWidth < 0.15 ? 1.0 : confWidth < 0.30 ? 0.7 : 0.4;
        const confLabel = confWidth < 0.15 ? "HIGH" : confWidth < 0.30 ? "MED" : "LOW";

        // Bonus: if confidence_low > 0 (even pessimistic case is profitable), add 20% weighting
        const convictionBonus = (confLow != null && confLow > 0) ? 1.2 : 1.0;

        // For long horizon, boost cards with cultural moat and low volatility
        // For short horizon, boost cards with strong recent momentum
        // (These come through the model features already, so just use projected return directly)
        let roiScore = projReturn * confMult * convictionBonus;

        // Apply horizon tilt: long horizon = weight toward confidence more,
        // short horizon = weight toward raw projected return
        if (horizon === "long") {
            roiScore = roiScore * (0.7 + 0.3 * confMult);  // stability matters more
        } else if (horizon === "short") {
            roiScore = projReturn * (0.7 + 0.3 * confMult);  // return matters more
        }

        // Build rationale
        const rationale = [];
        const sign = projReturn > 0 ? "+" : "";
        rationale.push(`Model projects ${sign}${(projReturn * 100).toFixed(1)}% over 90 days`);
        if (confLow != null && confHigh != null) {
            rationale.push(`Confidence band: ${(confLow * 100).toFixed(0)}% to ${(confHigh * 100).toFixed(0)}% (${confLabel})`);
        }
        if (confLow != null && confLow > 0) {
            rationale.push("Even pessimistic case is profitable");
        }
        // Show top 2 feature contributions if available
        const contribs = proj["feature-contributions"] || {};
        const topFeatures = Object.entries(contribs).slice(0, 2);
        for (const [feat, val] of topFeatures) {
            const vSign = val > 0 ? "+" : "";
            rationale.push(`${feat.replace(/_/g, " ")}: ${vSign}${(val * 100).toFixed(1)}%`);
        }

        return {
            roiScore,
            projReturn,
            confLow,
            confHigh,
            confWidth,
            confLabel,
            rationale,
        };
    }

    // ------------------------------------------------------------------
    // Render
    // ------------------------------------------------------------------

    function renderEmpty(innerHtml) {
        const tbody = document.getElementById("wl-tbody");
        // innerHtml here is a hardcoded literal from renderEmpty callers —
        // no user data interpolated.
        tbody.innerHTML = `<tr><td colspan="7" class="wl-empty">${innerHtml}</td></tr>`;
        document.getElementById("wl-headline").textContent =
            "Add cards to your wishlist from any card page.";
    }

    function rankClass(r) {
        if (r === 1) return "rank-cell rank-1";
        if (r === 2) return "rank-cell rank-2";
        if (r === 3) return "rank-cell rank-3";
        return "rank-cell";
    }

    function fitChipClass(fit) {
        if (fit == null) return "wl-fit-chip wl-fit-drop";
        if (fit >= 70) return "wl-fit-chip wl-fit-strong";
        if (fit >= 45) return "wl-fit-chip wl-fit-solid";
        return "wl-fit-chip wl-fit-weak";
    }

    function renderHeadline(rows, budget) {
        const headline = document.getElementById("wl-headline");
        if (!rows.length) {
            headline.textContent = "Add cards to your wishlist from any card page.";
            return;
        }

        headline.textContent = "";

        if (currentMode === "roi") {
            // ROI mode: highlight best projected return
            const sortedByProj = rows.slice().filter(r => Number.isFinite(r.projReturn))
                .sort((a, b) => b.roiScore - a.roiScore);
            const topPick = sortedByProj[0];
            if (topPick) {
                const label = document.createElement("strong");
                label.textContent = "Highest ROI: ";
                headline.appendChild(label);
                const sign = topPick.projReturn > 0 ? "+" : "";
                headline.appendChild(document.createTextNode(
                    `${topPick.card["product-name"] || "—"} `
                    + `(${sign}${(topPick.projReturn * 100).toFixed(1)}% proj, ${topPick.confLabel || "?"} conf)`
                ));
            } else {
                headline.textContent = "No model projections available yet. Add more cards and retrain the model.";
            }
            const withProj = rows.filter(r => Number.isFinite(r.projReturn)).length;
            headline.appendChild(document.createTextNode(
                ` · ${rows.length} wishlisted · ${withProj} with model projection`
            ));
        } else {
            // Fit mode (original)
            const topPick = rows.find(r => !r.scored.filteredOut && r.scored.fitScore != null);
            const totalCost = rows.reduce((sum, r) => sum + (r.price || 0), 0);
            const inBudget = budget > 0
                ? rows.filter(r => r.price <= budget).length
                : rows.length;

            if (topPick) {
                const label = document.createElement("strong");
                label.textContent = "Buy first: ";
                headline.appendChild(label);
                headline.appendChild(document.createTextNode(
                    `${topPick.card["product-name"] || "—"} `
                    + `(fit ${topPick.scored.fitScore}/100, ${money(topPick.price)})`
                ));
            } else {
                const label = document.createElement("strong");
                label.textContent = "No cards fit your budget. ";
                headline.appendChild(label);
                headline.appendChild(document.createTextNode("Raise the budget to see rankings."));
            }
            headline.appendChild(document.createTextNode(
                ` · ${rows.length} wishlisted · ${inBudget} under budget · total ${money(totalCost)}`
            ));
        }
    }

    function renderStatusBar(budget, horizon) {
        const horizonEl = document.getElementById("wl-status-horizon");
        const budgetEl  = document.getElementById("wl-status-budget");
        const horizonLabel = horizon === "short"  ? "Short (≤30d)"
                           : horizon === "medium" ? "Medium (90d–6mo)"
                           :                         "Long (1yr+)";
        horizonEl.textContent = `Horizon: ${horizonLabel}`;
        budgetEl.textContent  = `Budget: ${budget > 0 ? money(budget) : "—"}`;
    }

    /**
     * Build a row using DOM methods — no innerHTML, so there's no way for
     * a card name / set code / image URL to inject markup.
     */
    function buildRow(row, rank) {
        const c = row.card;
        const s = row.scored;
        const imgUrl = c["image-url"] || "";
        const name   = c["product-name"] || "\u2014";
        const setCode = c["set-code"] || "";
        const cardId = c.id || "";
        const price  = row.price;
        const budget = row.budget;

        const tr = document.createElement("tr");
        // In ROI mode, don't apply over-budget styling (budget doesn't matter)
        tr.className = (currentMode === "fit" && s.filteredOut) ? "wl-over-budget" : "";
        tr.dataset.cardId = String(cardId);

        // Rank cell
        const tdRank = document.createElement("td");
        tdRank.className = rankClass(rank);
        tdRank.textContent = String(rank);
        tr.appendChild(tdRank);

        // Image cell — createElement with src guards against injection
        const tdImg = document.createElement("td");
        if (imgUrl) {
            const img = document.createElement("img");
            img.className = "wl-img";
            img.src = imgUrl;
            img.alt = "";
            img.loading = "lazy";
            tdImg.appendChild(img);
        } else {
            tdImg.textContent = "\u2014";
        }
        tr.appendChild(tdImg);

        // Card cell (name, set, remove button)
        const tdCard = document.createElement("td");
        const nameDiv = document.createElement("div");
        nameDiv.className = "wl-name";
        nameDiv.textContent = name;
        tdCard.appendChild(nameDiv);

        const setDiv = document.createElement("div");
        setDiv.className = "wl-set";
        setDiv.textContent = setCode + " ";
        const removeBtn = document.createElement("button");
        removeBtn.className = "wl-remove-btn";
        removeBtn.setAttribute("data-remove-id", String(cardId));
        removeBtn.title = "Remove from wishlist";
        removeBtn.textContent = "× remove";
        setDiv.appendChild(removeBtn);
        tdCard.appendChild(setDiv);
        tr.appendChild(tdCard);

        // Price cell
        const tdPrice = document.createElement("td");
        tdPrice.className = "wl-price";
        tdPrice.textContent = money(price);
        tr.appendChild(tdPrice);

        if (currentMode === "roi") {
            // --- ROI mode columns: Proj, Confidence, ROI Score, Why ---

            // Projected return cell
            const tdProj = document.createElement("td");
            if (Number.isFinite(row.projReturn)) {
                const sign = row.projReturn > 0 ? "+" : "";
                const projText = `${sign}${(row.projReturn * 100).toFixed(1)}%`;
                const projSpan = document.createElement("span");
                projSpan.className = "wl-fit-chip " + (
                    row.projReturn > 0.05 ? "wl-fit-strong" :
                    row.projReturn > 0 ? "wl-fit-solid" :
                    row.projReturn > -0.05 ? "wl-fit-weak" :
                    "wl-fit-drop"
                );
                projSpan.textContent = projText;
                tdProj.appendChild(projSpan);
            } else {
                tdProj.textContent = "\u2014";
            }
            tr.appendChild(tdProj);

            // Confidence cell
            const tdConf = document.createElement("td");
            if (row.confLabel) {
                const confSpan = document.createElement("span");
                confSpan.className = "wl-fit-chip " + (
                    row.confLabel === "HIGH" ? "wl-fit-strong" :
                    row.confLabel === "MED" ? "wl-fit-solid" :
                    "wl-fit-weak"
                );
                confSpan.textContent = row.confLabel;
                if (row.confLow != null && row.confHigh != null) {
                    confSpan.title = `${(row.confLow * 100).toFixed(0)}% to ${(row.confHigh * 100).toFixed(0)}%`;
                }
                tdConf.appendChild(confSpan);
            } else {
                tdConf.textContent = "\u2014";
            }
            tr.appendChild(tdConf);

            // ROI Score cell (projected return × confidence multiplier, rescaled to 0-100)
            const tdRoi = document.createElement("td");
            if (Number.isFinite(row.roiScore)) {
                // Map roiScore (typically -0.10 to +0.15) to a 0-100 scale for display
                const display = Math.max(0, Math.min(100, Math.round((row.roiScore + 0.05) * 500)));
                const chip = document.createElement("span");
                chip.className = fitChipClass(display);
                chip.textContent = String(display);
                tdRoi.appendChild(chip);
            } else {
                tdRoi.textContent = "\u2014";
            }
            tr.appendChild(tdRoi);

            // Why cell — ROI rationale
            const tdWhy = document.createElement("td");
            if (row.roiRationale && row.roiRationale.length) {
                const ul = document.createElement("ul");
                ul.className = "wl-rationale";
                for (const r of row.roiRationale) {
                    const li = document.createElement("li");
                    li.textContent = r;
                    ul.appendChild(li);
                }
                tdWhy.appendChild(ul);
            }
            tr.appendChild(tdWhy);

        } else {
            // --- Fit mode columns (original): % budget, Fit, Why ---

            const tdPct = document.createElement("td");
            if (budget > 0 && price > 0) {
                const pct = Math.min(1, Math.max(0, price / budget));
                const bar = document.createElement("span");
                bar.className = "wl-pct-bar";
                const fill = document.createElement("span");
                fill.className = "wl-pct-bar-fill";
                fill.style.width = `${pct * 100}%`;
                bar.appendChild(fill);
                tdPct.appendChild(bar);
                tdPct.appendChild(document.createTextNode(`${Math.round(pct * 100)}%`));
            } else {
                tdPct.textContent = "\u2014";
            }
            tr.appendChild(tdPct);

            const tdFit = document.createElement("td");
            const chip = document.createElement("span");
            chip.className = fitChipClass(s.fitScore);
            chip.textContent = s.fitScore == null ? "\u2014" : String(s.fitScore);
            tdFit.appendChild(chip);
            tr.appendChild(tdFit);

            const tdWhy = document.createElement("td");
            if (s.rationale && s.rationale.length) {
                const ul = document.createElement("ul");
                ul.className = "wl-rationale";
                for (const r of s.rationale) {
                    const li = document.createElement("li");
                    li.textContent = r;
                    ul.appendChild(li);
                }
                tdWhy.appendChild(ul);
            }
            tr.appendChild(tdWhy);
        }

        return tr;
    }

    function render() {
        if (!loaded || !allCardsById) return;

        const { budget, horizon, sort } = readToolbar();
        const tbody = document.getElementById("wl-tbody");
        const list = window.WishlistStore.loadWishlist();

        // Update table head based on mode
        updateTableHead();
        updateToolbarForMode();

        if (!list.length) {
            renderEmpty("<strong>Your wishlist is empty.</strong><br>"
                + "Open any card detail page and click the <strong>☆ Add to Wishlist</strong> button.");
            renderStatusBar(budget, horizon);
            return;
        }

        // Join wishlist ids with card-index records
        const joined = [];
        const missing = [];
        for (const entry of list) {
            const c = allCardsById[String(entry.id)];
            if (!c) { missing.push(entry.id); continue; }
            const price = c["is-sealed"]
                ? Number(c["raw-price"])
                : Number(c["psa-10-price"]);
            const scored = window.WishlistStore.scoreForWishlist(c, { budget, horizon, setReturns });
            const roi = computeRoiScore(c, horizon);
            joined.push({
                entry, card: c, price, budget, scored,
                roiScore: roi.roiScore,
                projReturn: roi.projReturn,
                confLow: roi.confLow,
                confHigh: roi.confHigh,
                confWidth: roi.confWidth,
                confLabel: roi.confLabel,
                roiRationale: roi.rationale,
            });
        }

        if (missing.length) {
            for (const id of missing) window.WishlistStore.removeFromWishlist(id);
        }

        if (!joined.length) {
            renderEmpty("<strong>Your wishlisted cards couldn't be found in the catalog.</strong>");
            renderStatusBar(budget, horizon);
            return;
        }

        // In ROI mode, always sort by ROI score; in Fit mode, respect user sort choice
        const effectiveSort = currentMode === "roi" ? "roi" : sort;
        const sorted = sortRows(joined, effectiveSort);

        while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
        sorted.forEach((row, i) => tbody.appendChild(buildRow(row, i + 1)));

        renderHeadline(sorted, budget);
        renderStatusBar(budget, horizon);
        setStatus(`Showing ${sorted.length} wishlisted card${sorted.length === 1 ? "" : "s"}.`
            + (missing.length ? ` (${missing.length} pruned — no longer in catalog.)` : ""));
    }

    function updateTableHead() {
        const thead = document.getElementById("wl-thead");
        if (!thead) return;
        thead.textContent = "";
        const tr = document.createElement("tr");
        const cols = currentMode === "roi"
            ? [
                { label: "#", width: 40 },
                { label: "Image", width: 70 },
                { label: "Card" },
                { label: "Price", width: 90 },
                { label: "90d Proj", width: 90 },
                { label: "Confidence", width: 100 },
                { label: "ROI Score", width: 80 },
                { label: "Why" },
            ]
            : [
                { label: "#", width: 40 },
                { label: "Image", width: 70 },
                { label: "Card" },
                { label: "Price", width: 90 },
                { label: "% of Budget", width: 120 },
                { label: "Fit", width: 80 },
                { label: "Why" },
            ];
        for (const c of cols) {
            const th = document.createElement("th");
            if (c.width) th.style.width = c.width + "px";
            th.textContent = c.label;
            tr.appendChild(th);
        }
        thead.appendChild(tr);
    }

    function updateToolbarForMode() {
        // In ROI mode, dim the budget input and sort dropdown since they don't apply
        const budgetCtrl = document.getElementById("wl-budget")?.closest(".wl-control");
        const sortCtrl = document.getElementById("wl-sort")?.closest(".wl-control");
        const description = document.getElementById("wl-mode-description");
        if (currentMode === "roi") {
            if (budgetCtrl) budgetCtrl.style.opacity = "0.4";
            if (sortCtrl) sortCtrl.style.opacity = "0.4";
            if (description) description.textContent = "Pure ROI: ranks by model's projected return × confidence, regardless of budget. Best investments, period.";
        } else {
            if (budgetCtrl) budgetCtrl.style.opacity = "1";
            if (sortCtrl) sortCtrl.style.opacity = "1";
            if (description) description.textContent = "Budget Fit: ranks by best investment that fits your budget + horizon. Uses mean-reversion model + budget-fit bonus.";
        }
    }

    // ------------------------------------------------------------------
    // Wiring
    // ------------------------------------------------------------------

    function wire() {
        const budgetEl  = document.getElementById("wl-budget");
        const horizonEl = document.getElementById("wl-horizon");
        const sortEl    = document.getElementById("wl-sort");

        const onChange = () => { persistToolbar(); render(); };
        if (budgetEl)  budgetEl.addEventListener("input", onChange);
        if (horizonEl) horizonEl.addEventListener("change", onChange);
        if (sortEl)    sortEl.addEventListener("change", onChange);

        // Mode tabs
        document.querySelectorAll(".wl-mode-tab").forEach(btn => {
            btn.addEventListener("click", () => {
                currentMode = btn.dataset.mode;
                // Update tab visuals
                document.querySelectorAll(".wl-mode-tab").forEach(b => {
                    const active = b.dataset.mode === currentMode;
                    b.classList.toggle("active", active);
                    b.style.background = active ? "#dfdfdf" : "var(--win-surface)";
                    b.style.boxShadow = active ? "var(--bevel-sunken)" : "var(--bevel-raised)";
                    b.style.color = active ? (currentMode === "roi" ? "#006400" : "#003388") : "#000";
                });
                render();
            });
        });

        const tbody = document.getElementById("wl-tbody");
        tbody.addEventListener("click", (ev) => {
            const removeBtn = ev.target.closest("[data-remove-id]");
            if (removeBtn) {
                ev.stopPropagation();
                const id = removeBtn.getAttribute("data-remove-id");
                window.WishlistStore.removeFromWishlist(id);
                render();
                return;
            }
            const row = ev.target.closest("tr[data-card-id]");
            if (row) {
                const id = row.dataset.cardId;
                if (id) window.location = `/card.html?id=${encodeURIComponent(id)}`;
            }
        });
    }

    // ------------------------------------------------------------------
    // Boot
    // ------------------------------------------------------------------

    async function boot() {
        applyPrefsToToolbar();
        wire();
        await Promise.all([loadCardIndex(), loadSetReturns()]);
        loaded = true;
        render();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }
})();
