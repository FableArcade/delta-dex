const API_BASE = "/api";

// Game Boy color palette
const GB = {
    darkest:  "#0f380f",
    dark:     "#306230",
    light:    "#8bac0f",
    lightest: "#9bbc0f",
    bg:       "#0f380f",
};

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

function chipClass(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "chip chip-neu";
    if (Math.abs(n) < 0.05) return "chip chip-neu";
    return n >= 0 ? "chip chip-pos" : "chip chip-neg";
}

function evChipClass(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "chip chip-neu";
    if (n >= 5) return "chip chip-pos";
    if (n >= 2) return "chip chip-neu";
    return "chip chip-neg";
}

function rankClass(r) {
    if (r === 1) return "rank rank-1";
    if (r === 2) return "rank rank-2";
    if (r === 3) return "rank rank-3";
    return "rank";
}

function sortByRank(rows, key) {
    return [...rows].sort((a, b) => {
        const av = a[key], bv = b[key];
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        return av - bv;
    });
}

// Mini chart cache
const miniCharts = new Map();

function destroyMiniChart(key) {
    const c = miniCharts.get(key);
    if (c) { c.destroy(); miniCharts.delete(key); }
}

async function drawMiniCharts(metricKey) {
    if (!window.Chart) return;
    const canvases = document.querySelectorAll(`canvas.miniChart[data-metric="${metricKey}"]`);

    for (const canvas of canvases) {
        const setCode = decodeURIComponent(canvas.dataset.set || "");
        if (!setCode) continue;

        const chartKey = `${setCode}::${metricKey}`;
        destroyMiniChart(chartKey);

        try {
            const res = await fetch(`${API_BASE}/set/${encodeURIComponent(setCode)}`);
            if (!res.ok) continue;
            const setData = await res.json();
            const hist = Array.isArray(setData.history) ? [...setData.history] : [];
            if (hist.length < 2) continue;
            hist.sort((a, b) => String(a.date).localeCompare(String(b.date)));

            const labels = hist.map(h => h.date);
            const values = hist.map(h => {
                const v = h[metricKey];
                return (typeof v === "number" && Number.isFinite(v)) ? v : null;
            });
            if (values.filter(v => v != null).length < 2) continue;

            const ctx = canvas.getContext("2d");
            const showBaseline = (metricKey === "avg-gain-loss");
            const datasets = [];

            if (showBaseline) {
                datasets.push({
                    data: labels.map(() => 0),
                    borderDash: [3, 3],
                    borderColor: GB.dark,
                    borderWidth: 1,
                    pointRadius: 0,
                    pointHoverRadius: 0,
                    tension: 0,
                    order: 0,
                    isBaseline: true,
                });
            }

            datasets.push({
                data: values,
                borderColor: GB.lightest,
                backgroundColor: GB.light + "40",
                tension: 0.25,
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 4,
                pointHoverBackgroundColor: GB.lightest,
                pointHoverBorderColor: GB.lightest,
                hitRadius: 30,
                spanGaps: false,
                fill: true,
                order: 1,
                isBaseline: false,
            });

            const chart = new Chart(ctx, {
                type: "line",
                data: { labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: false,
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
                            titleFont: { family: "W95FA, monospace", size: 10 },
                            bodyFont: { family: "W95FA, monospace", size: 10 },
                            filter: (ctx) => showBaseline ? !ctx.dataset?.isBaseline : true,
                            callbacks: {
                                title: (items) => {
                                    const item = showBaseline
                                        ? items?.find(i => !i.dataset?.isBaseline)
                                        : items?.[0];
                                    return item ? labels[item.dataIndex] || "" : "";
                                },
                                label: (item) => {
                                    if (item.dataset?.isBaseline) return null;
                                    const v = item.parsed?.y;
                                    if (typeof v !== "number") return "\u2014";
                                    const sign = v < 0 ? "-" : "";
                                    return `${sign}$${Math.abs(v).toFixed(2)}`;
                                }
                            }
                        }
                    },
                    scales: {
                        x: { display: false },
                        y: { display: false }
                    }
                }
            });

            miniCharts.set(chartKey, chart);
        } catch (e) {
            console.warn("mini chart error:", setCode, metricKey, e);
        }
    }
}

function logoSrc(setCode) {
    return `https://mycollectrics.com/images/logos/sets/small/${encodeURIComponent(setCode)}.png`;
}

function setRow(r, rankKey, valueHtml, trendMetric, extraCols) {
    const setCode = r["set-code"];
    const rank = r[rankKey] ?? "\u2014";
    const cols = extraCols || "";

    const trendCell = trendMetric
        ? `<td>
             <div class="mini-gb-screen">
               <div class="mini-gb-inner">
                 <canvas class="miniChart" data-set="${encodeURIComponent(setCode)}" data-metric="${trendMetric}"></canvas>
               </div>
             </div>
           </td>`
        : "";

    return `
    <tr class="rowLink" data-href="/sets/${encodeURIComponent(setCode)}" onclick="if(!event.target.closest('a'))window.location=this.dataset.href">
        <td class="text-center"><span class="${rankClass(rank)}">${rank}</span></td>
        <td>
            <div class="set-logo-cell">
                <img class="set-logo" src="${logoSrc(setCode)}" alt="${setCode}" loading="lazy">
            </div>
        </td>
        <td class="text-center">${valueHtml}</td>
        ${trendCell}
        ${cols}
    </tr>`;
}

function renderGainLoss(rows) {
    const tbody = document.getElementById("tbody-gl");
    if (!tbody) return;
    const sorted = sortByRank(rows, "rank-avg-gain-loss");
    tbody.innerHTML = sorted.map(r =>
        setRow(r, "rank-avg-gain-loss",
            `<span class="${chipClass(r["avg-gain-loss"])}">${money2(r["avg-gain-loss"])}</span>`,
            "avg-gain-loss")
    ).join("");
    drawMiniCharts("avg-gain-loss");
}

function renderEV(rows) {
    const tbody = document.getElementById("tbody-ev");
    if (!tbody) return;
    const sorted = sortByRank(rows, "rank-ev-raw-per-pack");
    tbody.innerHTML = sorted.map(r =>
        setRow(r, "rank-ev-raw-per-pack",
            `<span class="${evChipClass(r["ev-raw-per-pack"])}">${money2(r["ev-raw-per-pack"])}</span>`,
            "ev-raw-per-pack")
    ).join("");
    drawMiniCharts("ev-raw-per-pack");
}

function renderSetValue(rows) {
    const tbody = document.getElementById("tbody-sv");
    if (!tbody) return;
    const sorted = sortByRank(rows, "rank-total-set-raw-value");
    tbody.innerHTML = sorted.map(r =>
        setRow(r, "rank-total-set-raw-value",
            `<span class="${evChipClass(r["total-set-raw-value"])}">${money2(r["total-set-raw-value"])}</span>`,
            "total-set-raw-value")
    ).join("");
    drawMiniCharts("total-set-raw-value");
}

function renderGemRate(rows) {
    const tbody = document.getElementById("tbody-gp");
    if (!tbody) return;
    const sorted = sortByRank(rows, "rank-psa-avg-gem-pct");
    tbody.innerHTML = sorted.map(r => {
        const extra = `
            <td class="text-center text-mono">${number(r["psa-pop-10-base"])}</td>
            <td class="text-center text-mono">${number(r["psa-pop-total-base"])}</td>`;
        return setRow(r, "rank-psa-avg-gem-pct",
            `<span class="chip chip-neu">${percent(r["psa-avg-gem-pct"])}</span>`,
            null, extra);
    }).join("");
}

async function loadLeaderboard() {
    const statusMsg = document.getElementById("status-msg");
    if (statusMsg) statusMsg.textContent = "Loading data...";

    try {
        const res = await fetch(`${API_BASE}/leaderboard`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        const meta = document.getElementById("meta");
        if (meta && data["generated-at"]) {
            meta.textContent = `updated ${data["generated-at"]}`;
        }

        const rows = Array.isArray(data.rows) ? data.rows : [];

        renderGainLoss(rows);
        renderEV(rows);
        renderSetValue(rows);
        renderGemRate(rows);

        if (statusMsg) statusMsg.textContent = `Loaded ${rows.length} sets`;
    } catch (err) {
        console.error(err);
        if (statusMsg) statusMsg.textContent = `Error: ${err.message}`;
        for (const id of ["tbody-gl", "tbody-ev", "tbody-sv", "tbody-gp"]) {
            const el = document.getElementById(id);
            if (el) el.innerHTML = `<tr><td colspan="5" style="text-align:center;color:#cc0000;">Failed to load. Check connection.</td></tr>`;
        }
    }
}

loadLeaderboard();
