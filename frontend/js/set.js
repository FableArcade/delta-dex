/* ============================================================
   set.js — Set Detail Page
   Reads set code from URL path: /sets/{CODE}
   ============================================================ */

const API_BASE = "/api";

const GB = {
    darkest:  "#0f380f",
    dark:     "#306230",
    light:    "#8bac0f",
    lightest: "#9bbc0f",
    bg:       "#0f380f",
};

/* ---- Formatters ---- */

function money2(x) {
    if (x === null || x === undefined || Number.isNaN(x)) return "\u2014";
    return Number(x).toLocaleString("en-US", {
        style: "currency", currency: "USD",
        minimumFractionDigits: 2, maximumFractionDigits: 2
    });
}

function percent(x) {
    if (x === null || x === undefined || Number.isNaN(x)) return "\u2014";
    return (Number(x) * 100).toLocaleString("en-US", {
        minimumFractionDigits: 1, maximumFractionDigits: 1
    }) + "%";
}

function number(x) {
    const n = Number(x);
    if (!Number.isFinite(n)) return "\u2014";
    return n.toLocaleString("en-US");
}

function chipHtml(val, type) {
    const n = Number(val);
    if (!Number.isFinite(n)) return `<span class="chip chip-neu">\u2014</span>`;
    let cls;
    if (type === "gl") {
        cls = Math.abs(n) < 0.05 ? "chip chip-neu" : n >= 0 ? "chip chip-pos" : "chip chip-neg";
    } else {
        cls = n >= 5 ? "chip chip-pos" : n >= 2 ? "chip chip-neu" : "chip chip-neg";
    }
    return `<span class="${cls}">${money2(val)}</span>`;
}

function logoSrc(setCode, apiLogoUrl) {
    if (apiLogoUrl) return apiLogoUrl;
    return `https://mycollectrics.com/images/logos/sets/small/${encodeURIComponent(setCode)}.png`;
}

/* ---- Extract set code from URL ---- */

function getSetCode() {
    const path = window.location.pathname;
    const parts = path.split("/sets/");
    if (parts.length >= 2 && parts[1]) {
        return decodeURIComponent(parts[1].replace(/\/$/, ""));
    }
    // Fallback to query param
    const params = new URLSearchParams(window.location.search);
    return params.get("code") || params.get("set") || "";
}

/* ---- State ---- */
let setData = null;
let cardsData = [];
let raritiesList = [];
let evChart = null;
let currentInterval = "ALL";
let cardSearchTerm = "";
let cardRarityFilter = "";
let cardSort = "psa10_desc";

/* ---- Main loader ---- */

async function init() {
    const code = getSetCode();
    if (!code) {
        document.getElementById("set-content").innerHTML =
            '<div style="text-align:center;padding:40px;color:#cc0000;">No set code found in URL.</div>';
        return;
    }

    const statusMsg = document.getElementById("status-msg");
    statusMsg.textContent = `Loading ${code}...`;

    try {
        // Fetch all three endpoints in parallel
        const [setRes, cardsRes, raritiesRes] = await Promise.all([
            fetch(`${API_BASE}/set/${encodeURIComponent(code)}`),
            fetch(`${API_BASE}/search/cards?setCode=${encodeURIComponent(code)}&sort=psa10_desc&limit=500`),
            fetch(`${API_BASE}/search/rarities?setCode=${encodeURIComponent(code)}`),
        ]);

        if (!setRes.ok) throw new Error(`Set not found (${setRes.status})`);

        setData = await setRes.json();

        if (cardsRes.ok) {
            const cData = await cardsRes.json();
            cardsData = Array.isArray(cData.results) ? cData.results : [];
        }

        if (raritiesRes.ok) {
            const rData = await raritiesRes.json();
            raritiesList = Array.isArray(rData.rarities) ? rData.rarities : [];
        }

        renderPage();
        statusMsg.textContent = setData["set-name"] || code;
        document.getElementById("status-cards").textContent = `${cardsData.length} cards`;
        document.getElementById("window-title").textContent =
            `C:\\POKEMON\\${(setData["set-name"] || code).toUpperCase()}.EXE`;
        document.getElementById("taskbar-label").textContent =
            `${(setData["set-name"] || code).substring(0, 20)}`;
        document.title = `DELTADEX - ${setData["set-name"] || code}`;

    } catch (err) {
        console.error(err);
        document.getElementById("set-content").innerHTML =
            `<div style="text-align:center;padding:40px;color:#cc0000;">Error: ${err.message}</div>`;
        statusMsg.textContent = `Error: ${err.message}`;
    }
}

/* ---- Render full page ---- */

function renderPage() {
    const d = setData;
    const code = d["set-code"];
    const content = document.getElementById("set-content");

    content.innerHTML = `
        <!-- A) Set Header -->
        ${renderSetHeader(d)}

        <!-- B) EV History Chart -->
        ${renderChartSection(d)}

        <!-- C) Rarity Breakdown -->
        ${renderRarityBreakdown(d)}

        <!-- D) Cards in Set -->
        ${renderCardsSection()}
    `;

    // Init chart after DOM is ready
    setTimeout(() => {
        initEvChart(d);
        bindCardControls();
    }, 50);
}

/* ---- A) Set Header ---- */

function renderSetHeader(d) {
    const packCost = d["pack-cost-components"];

    return `
    <div class="window mb-8">
        <div class="window-title" style="background: linear-gradient(90deg, #4b0082, #8040c0);">
            <span class="window-title-text">C:\\SET_INFO.DAT</span>
        </div>
        <div class="window-body">
            <div class="set-header-row">
                <div class="set-header-logo">
                    <img src="${logoSrc(d["set-code"], d["logo-url"])}" alt="${d["set-code"]}">
                </div>
                <div class="set-header-info">
                    <div class="set-header-title">${d["set-name"] || d["set-code"]}</div>
                    <div class="set-header-date">Released: ${d["release-date"] || "\u2014"} &nbsp;|&nbsp; Code: ${d["set-code"]}</div>

                    <div class="stat-panels">
                        <div class="stat-panel">
                            <div class="stat-panel-label">EV Raw / Pack</div>
                            <div class="stat-panel-value">${money2(d["ev-raw-per-pack"])}</div>
                        </div>
                        <div class="stat-panel">
                            <div class="stat-panel-label">EV PSA 10 / Pack</div>
                            <div class="stat-panel-value">${money2(d["ev-psa-10-per-pack"])}</div>
                        </div>
                        <div class="stat-panel">
                            <div class="stat-panel-label">Avg Pack Cost</div>
                            <div class="stat-panel-value">${money2(d["avg-pack-cost"])}</div>
                        </div>
                        <div class="stat-panel">
                            <div class="stat-panel-label">Avg Gain/Loss</div>
                            <div class="stat-panel-value">${chipHtml(d["avg-gain-loss"], "gl")}</div>
                        </div>
                        <div class="stat-panel">
                            <div class="stat-panel-label">Total Set Raw Value</div>
                            <div class="stat-panel-value">${money2(d["total-set-raw-value"])}</div>
                        </div>
                    </div>

                    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px;">
                        ${packCost ? `
                        <div class="groupbox" style="flex:1;min-width:180px;">
                            <div class="groupbox-label">Pack Cost Breakdown</div>
                            <div class="pack-cost-row">
                                <span>Booster Pack</span>
                                <strong>${money2(packCost["avg-booster-pack"])}</strong>
                            </div>
                            <div class="pack-cost-row">
                                <span>Sleeved Booster</span>
                                <strong>${money2(packCost["avg-sleeved-booster-pack"])}</strong>
                            </div>
                            <div class="pack-cost-row">
                                <span>Bundle / Pack</span>
                                <strong>${money2(packCost["avg-booster-bundle-per-pack"])}</strong>
                            </div>
                        </div>` : ""}

                        <div class="groupbox" style="flex:1;min-width:180px;">
                            <div class="groupbox-label">PSA Stats</div>
                            <div class="pack-cost-row">
                                <span>Gem Rate</span>
                                <strong>${percent(d["psa-avg-gem-pct"])}</strong>
                            </div>
                            <div class="pack-cost-row">
                                <span>PSA 10 Pop</span>
                                <strong>${number(d["psa-pop-10-base"])}</strong>
                            </div>
                            <div class="pack-cost-row">
                                <span>Total Pop</span>
                                <strong>${number(d["psa-pop-total-base"])}</strong>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>`;
}

/* ---- B) EV History Chart ---- */

function renderChartSection(d) {
    return `
    <div class="window mb-8">
        <div class="window-title" style="background: linear-gradient(90deg, #006400, #40a040);">
            <span class="window-title-text">C:\\EV_HISTORY.DAT</span>
        </div>
        <div class="window-body">
            <div class="chart-controls">
                <button class="btn chart-interval-btn" data-interval="7">7D</button>
                <button class="btn chart-interval-btn" data-interval="30">30D</button>
                <button class="btn chart-interval-btn" data-interval="90">90D</button>
                <button class="btn chart-interval-btn active" data-interval="ALL">ALL</button>
                <div class="chart-legend">
                    <div class="chart-legend-item">
                        <span class="chart-legend-swatch" style="background:#9bbc0f;"></span>
                        Avg Gain/Loss
                    </div>
                    <div class="chart-legend-item">
                        <span class="chart-legend-swatch" style="background:#8bac0f;opacity:0.6;"></span>
                        EV Raw / Pack
                    </div>
                </div>
            </div>
            <div class="crt-screen">
                <div class="crt-screen-inner">
                    <div class="ev-chart-container">
                        <canvas id="ev-chart"></canvas>
                    </div>
                </div>
            </div>
            <div class="screen-label">DOT MATRIX WITH STEREO SOUND</div>
        </div>
    </div>`;
}

function initEvChart(d) {
    const canvas = document.getElementById("ev-chart");
    if (!canvas) return;

    const history = Array.isArray(d.history) ? [...d.history] : [];
    if (history.length === 0) return;
    history.sort((a, b) => String(a.date).localeCompare(String(b.date)));

    // Bind interval buttons
    document.querySelectorAll(".chart-interval-btn").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll(".chart-interval-btn").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            const interval = btn.dataset.interval;
            currentInterval = interval;
            updateEvChart(history);
        });
    });

    buildEvChart(history);
}

function filterByInterval(history, interval) {
    if (interval === "ALL") return history;
    const days = parseInt(interval);
    if (!days) return history;
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return history.filter(h => h.date >= cutoffStr);
}

function buildEvChart(history) {
    const canvas = document.getElementById("ev-chart");
    if (!canvas) return;

    const filtered = filterByInterval(history, currentInterval);
    if (filtered.length === 0) return;

    const labels = filtered.map(h => h.date);
    const glValues = filtered.map(h => h["avg-gain-loss"] ?? null);
    const evValues = filtered.map(h => h["ev-raw-per-pack"] ?? null);

    const ctx = canvas.getContext("2d");

    if (evChart) evChart.destroy();

    evChart = new Chart(ctx, {
        type: "line",
        data: {
            labels,
            datasets: [
                // $0 baseline
                {
                    data: labels.map(() => 0),
                    borderDash: [4, 4],
                    borderColor: GB.dark,
                    borderWidth: 1,
                    pointRadius: 0,
                    pointHoverRadius: 0,
                    tension: 0,
                    order: 0,
                    isBaseline: true,
                },
                // Avg Gain/Loss
                {
                    label: "Avg Gain/Loss",
                    data: glValues,
                    borderColor: GB.lightest,
                    backgroundColor: "rgba(155,188,15,0.15)",
                    tension: 0.3,
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 5,
                    pointHoverBackgroundColor: GB.lightest,
                    pointHoverBorderColor: "#fff",
                    hitRadius: 20,
                    fill: true,
                    spanGaps: true,
                    order: 1,
                    isBaseline: false,
                },
                // EV Raw Per Pack
                {
                    label: "EV Raw / Pack",
                    data: evValues,
                    borderColor: GB.light,
                    backgroundColor: "transparent",
                    tension: 0.3,
                    borderWidth: 1.5,
                    borderDash: [6, 3],
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHoverBackgroundColor: GB.light,
                    hitRadius: 20,
                    fill: false,
                    spanGaps: true,
                    order: 2,
                    isBaseline: false,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 300 },
            interaction: { mode: "index", intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: {
                    displayColors: false,
                    backgroundColor: "#0f380f",
                    borderColor: GB.lightest,
                    borderWidth: 1,
                    titleColor: GB.lightest,
                    bodyColor: GB.lightest,
                    titleFont: { family: "'Courier New', monospace", size: 11 },
                    bodyFont: { family: "'Courier New', monospace", size: 11 },
                    filter: (item) => !item.dataset?.isBaseline,
                    callbacks: {
                        label: (item) => {
                            if (item.dataset?.isBaseline) return null;
                            const v = item.parsed?.y;
                            if (typeof v !== "number") return "\u2014";
                            const lbl = item.dataset.label || "";
                            const sign = v < 0 ? "-" : "";
                            return `${lbl}: ${sign}$${Math.abs(v).toFixed(2)}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: GB.dark,
                        font: { family: "'Courier New', monospace", size: 10 },
                        maxRotation: 0,
                        maxTicksLimit: 10,
                    },
                    grid: {
                        color: GB.dark,
                        lineWidth: 0.5,
                    },
                    border: { color: GB.dark },
                },
                y: {
                    ticks: {
                        color: GB.dark,
                        font: { family: "'Courier New', monospace", size: 10 },
                        callback: (v) => `$${v.toFixed(2)}`,
                    },
                    grid: {
                        color: GB.dark,
                        lineWidth: 0.5,
                    },
                    border: { color: GB.dark },
                },
            },
        },
    });
}

function updateEvChart(history) {
    if (!evChart) {
        buildEvChart(history);
        return;
    }

    const filtered = filterByInterval(history, currentInterval);
    if (filtered.length === 0) return;

    const labels = filtered.map(h => h.date);
    const glValues = filtered.map(h => h["avg-gain-loss"] ?? null);
    const evValues = filtered.map(h => h["ev-raw-per-pack"] ?? null);

    evChart.data.labels = labels;
    evChart.data.datasets[0].data = labels.map(() => 0);
    evChart.data.datasets[1].data = glValues;
    evChart.data.datasets[2].data = evValues;
    evChart.update();
}

/* ---- C) Rarity Breakdown ---- */

function renderRarityBreakdown(d) {
    const breakdown = d["rarity-breakdown"];
    if (!breakdown || Object.keys(breakdown).length === 0) {
        return "";
    }

    const rows = Object.entries(breakdown).map(([code, r]) => {
        const odds = r["pull-rate-odds"];
        const oddsStr = odds ? `1 in ${odds}` : "\u2014";
        return `
            <tr>
                <td><strong>${r["rarity-name"] || code}</strong></td>
                <td class="text-center text-mono">${number(r["card-count"])}</td>
                <td class="text-center text-mono">${oddsStr}</td>
                <td class="text-center text-mono">${money2(r["avg-raw-price"])}</td>
                <td class="text-center text-mono">${money2(r["avg-psa-10-price"])}</td>
                <td class="text-center text-mono">${money2(r["ev-raw-per-pack"])}</td>
                <td class="text-center text-mono">${money2(r["ev-psa-10-per-pack"])}</td>
                <td class="text-center text-mono">${percent(r["psa-avg-gem-pct"])}</td>
            </tr>`;
    }).join("");

    return `
    <div class="window mb-8">
        <div class="window-title" style="background: linear-gradient(90deg, #8b6914, #c0a040);">
            <span class="window-title-text">C:\\RARITY_BREAKDOWN.DAT</span>
        </div>
        <div class="section-explainer">
            value breakdown by rarity bucket showing pull rates, avg prices, and EV contribution per pack
        </div>
        <div class="window-body-flush">
            <div class="rarity-table-wrap">
                <div class="win-table-wrap">
                    <table class="win-table">
                        <thead>
                            <tr>
                                <th>RARITY</th>
                                <th>CARDS</th>
                                <th>PULL RATE</th>
                                <th>AVG RAW</th>
                                <th>AVG PSA 10</th>
                                <th>EV RAW/PACK</th>
                                <th>EV PSA10/PACK</th>
                                <th>GEM %</th>
                            </tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>`;
}

/* ---- D) Cards in Set ---- */

function renderCardsSection() {
    const rarityOptions = raritiesList.map(r =>
        `<option value="${r["rarity-name"]}">${r["rarity-name"]}</option>`
    ).join("");

    return `
    <div class="window mb-8">
        <div class="window-title" style="background: linear-gradient(90deg, #800000, #c04040);">
            <span class="window-title-text">C:\\CARDS_IN_SET.DAT</span>
        </div>
        <div class="window-body">
            <div class="cards-toolbar">
                <input type="text" class="win-input" id="card-search" placeholder="Search cards..." style="width:180px;">
                <select class="win-input" id="card-rarity-filter" style="width:140px;">
                    <option value="">All Rarities</option>
                    ${rarityOptions}
                </select>
                <select class="win-input" id="card-sort" style="width:140px;">
                    <option value="psa10_desc">PSA 10: High to Low</option>
                    <option value="raw_desc">Raw: High to Low</option>
                    <option value="raw_asc">Raw: Low to High</option>
                    <option value="name_asc">Name: A-Z</option>
                    <option value="number_asc">Number: Asc</option>
                </select>
                <span id="card-count-label" style="font-size:11px;color:var(--win-text-disabled);margin-left:auto;"></span>
            </div>
            <div id="cards-grid" class="cards-grid"></div>
        </div>
    </div>`;
}

function bindCardControls() {
    const searchInput = document.getElementById("card-search");
    const raritySelect = document.getElementById("card-rarity-filter");
    const sortSelect = document.getElementById("card-sort");

    if (searchInput) {
        searchInput.addEventListener("input", () => {
            cardSearchTerm = searchInput.value.toLowerCase();
            renderCards();
        });
    }

    if (raritySelect) {
        raritySelect.addEventListener("change", () => {
            cardRarityFilter = raritySelect.value;
            renderCards();
        });
    }

    if (sortSelect) {
        sortSelect.addEventListener("change", () => {
            cardSort = sortSelect.value;
            renderCards();
        });
    }

    renderCards();
}

function renderCards() {
    const grid = document.getElementById("cards-grid");
    const countLabel = document.getElementById("card-count-label");
    if (!grid) return;

    let filtered = [...cardsData];

    // Search
    if (cardSearchTerm) {
        filtered = filtered.filter(c =>
            (c["product-name"] || "").toLowerCase().includes(cardSearchTerm) ||
            (c["card-number"] || "").toLowerCase().includes(cardSearchTerm)
        );
    }

    // Rarity filter
    if (cardRarityFilter) {
        filtered = filtered.filter(c => c["rarity-name"] === cardRarityFilter);
    }

    // Sort
    filtered.sort((a, b) => {
        switch (cardSort) {
            case "raw_desc":
                return (b["raw-price"] || 0) - (a["raw-price"] || 0);
            case "raw_asc":
                return (a["raw-price"] || 0) - (b["raw-price"] || 0);
            case "psa10_desc":
                return (b["psa-10-price"] || 0) - (a["psa-10-price"] || 0);
            case "name_asc":
                return (a["product-name"] || "").localeCompare(b["product-name"] || "");
            case "number_asc":
                return (a["card-number"] || "").localeCompare(b["card-number"] || "", undefined, { numeric: true });
            default:
                return 0;
        }
    });

    if (countLabel) {
        countLabel.textContent = `${filtered.length} card${filtered.length !== 1 ? "s" : ""}`;
    }

    if (filtered.length === 0) {
        grid.innerHTML = '<div style="grid-column:1/-1;text-align:center;padding:20px;color:#808080;">No cards found.</div>';
        return;
    }

    grid.innerHTML = filtered.map(c => {
        const imgUrl = c["image-url"] || "";
        const imgTag = imgUrl
            ? `<img src="${imgUrl}" alt="${c["product-name"] || ""}" loading="lazy">`
            : `<div style="width:100%;height:180px;display:flex;align-items:center;justify-content:center;color:#808080;font-size:11px;">No Image</div>`;

        return `
            <a href="/card.html?id=${encodeURIComponent(c["id"])}" class="card-tile">
                <div class="card-tile-img-wrap">${imgTag}</div>
                <div class="card-tile-info">
                    <div class="card-tile-name" title="${c["product-name"] || ""}">${c["product-name"] || "\u2014"}</div>
                    <div style="font-size:10px;color:var(--win-text-disabled);margin-bottom:3px;">${c["rarity-name"] || ""} &middot; #${c["card-number"] || ""}</div>
                    <div class="card-tile-prices">
                        <span>
                            <span class="price-label">Raw</span>
                            <strong>${money2(c["raw-price"])}</strong>
                        </span>
                        <span style="text-align:right;">
                            <span class="price-label">PSA 10</span>
                            <strong>${money2(c["psa-10-price"])}</strong>
                        </span>
                    </div>
                </div>
            </a>`;
    }).join("");
}

/* ---- Boot ---- */
init();
