const API_BASE = "/api";

let currentSetData = null;

// HTML-escape user-controlled strings before splatting into innerHTML.
function esc(s) {
    if (s == null) return "";
    return String(s)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}

function money(x) {
    if (x === null || x === undefined || !Number.isFinite(Number(x))) return "\u2014";
    return Number(x).toLocaleString("en-US", {
        style: "currency", currency: "USD",
        minimumFractionDigits: 2, maximumFractionDigits: 2
    });
}

function pct(x) {
    if (!Number.isFinite(x)) return "\u2014";
    return (x * 100).toFixed(1) + "%";
}

function chipClass(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "";
    if (Math.abs(n) < 0.01) return "chip chip-neu";
    return n >= 0 ? "chip chip-pos" : "chip chip-neg";
}

// Populate set dropdown from /api/leaderboard
async function loadSets() {
    const select = document.getElementById("set-select");
    const statusMsg = document.getElementById("status-msg");

    try {
        const res = await fetch(`${API_BASE}/leaderboard`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const rows = Array.isArray(data.rows) ? data.rows : (Array.isArray(data) ? data : []);

        rows.forEach(r => {
            const code = r["set-code"] || r.setCode || "";
            const name = r["set-name"] || r.setName || code;
            if (!code) return;
            const opt = document.createElement("option");
            opt.value = code;
            opt.textContent = `${name} (${code})`;
            select.appendChild(opt);
        });

        statusMsg.textContent = `${rows.length} sets available`;
    } catch (err) {
        console.error(err);
        statusMsg.textContent = `Error loading sets: ${err.message}`;
    }
}

// Fetch set rarity breakdown
async function loadSetData(setCode) {
    const statusMsg = document.getElementById("status-msg");
    const raritySection = document.getElementById("rarity-section");
    const resultsSection = document.getElementById("results-section");

    statusMsg.textContent = `Loading ${setCode}...`;
    raritySection.style.display = "none";
    resultsSection.style.display = "none";

    try {
        const res = await fetch(`${API_BASE}/set/${encodeURIComponent(setCode)}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        currentSetData = data;
        renderRarityTable(data);
        raritySection.style.display = "block";

        document.getElementById("status-set").textContent = setCode;
        statusMsg.textContent = `Set ${setCode} loaded`;
    } catch (err) {
        console.error(err);
        statusMsg.textContent = `Error: ${err.message}`;
        currentSetData = null;
    }
}

// Normalize rarity-breakdown — the API returns it as a DICT keyed by rarity
// code, not an array. Convert to a flat array of objects (one per rarity).
function normalizeRarities(data) {
    const rb = data.rarities || data["rarity-breakdown"] || data.rarityBreakdown;
    if (!rb) return [];
    if (Array.isArray(rb)) return rb;
    return Object.entries(rb).map(([code, r]) => ({
        rarityCode: code,
        rarityName: r["rarity-name"] || r.rarity || r.name || code,
        cardCount: r["card-count"] || r.count || r.cardCount || 0,
        pullRate: Number(r["pull-rate"] || r.pullRate) || 0,
        pullRateOdds: r["pull-rate-odds"] || "",
        avgRawPrice: Number(r["avg-raw-price"] || r["avg-value"] || r.avgValue) || 0,
        avgPsa10Price: Number(r["avg-psa-10-price"]) || 0,
        evRawPerPack: Number(r["ev-raw-per-pack"]) || 0,
        evPsa10PerPack: Number(r["ev-psa-10-per-pack"]) || 0,
    }));
}

function renderRarityTable(data) {
    const tbody = document.getElementById("rarity-tbody");
    const rarities = normalizeRarities(data);

    if (rarities.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:#808080;">No rarity data available</td></tr>';
        return;
    }

    rarities.sort((a, b) => (a.pullRate || 0) - (b.pullRate || 0));

    tbody.innerHTML = rarities.map(r => {
        const name = esc(r.rarityName);
        const oddsStr = r.pullRateOdds ? ` (${esc(r.pullRateOdds)})` : "";
        return `
        <tr>
            <td>${name}</td>
            <td class="text-center text-mono">${r.cardCount}</td>
            <td class="text-center text-mono">${pct(r.pullRate)}${oddsStr}</td>
            <td class="text-right text-mono">${money(r.avgRawPrice)}</td>
        </tr>`;
    }).join("");
}

function calculate() {
    if (!currentSetData) return;

    const numPacks = Math.max(1, parseInt(document.getElementById("pack-count").value) || 36);
    const rarities = normalizeRarities(currentSetData);
    const packCost = Number(
        currentSetData["avg-pack-cost"] ||
        currentSetData["pack-cost"] ||
        currentSetData.packCost ||
        4.50
    );
    const resultsSection = document.getElementById("results-section");

    if (rarities.length === 0) {
        document.getElementById("status-msg").textContent = "No rarity data to calculate";
        return;
    }

    // Expected pulls + total raw value
    let pullsHtml = "";
    let totalExpectedValue = 0;

    rarities.forEach(r => {
        const expectedCount = r.pullRate * numPacks;
        const expectedVal   = expectedCount * r.avgRawPrice;
        totalExpectedValue += expectedVal;

        pullsHtml += `
        <div style="display:flex;justify-content:space-between;padding:2px 0;font-size:12px;border-bottom:1px solid #e8e8e8;">
            <span>${esc(r.rarityName)}</span>
            <span class="text-mono" style="font-weight:bold;">${expectedCount.toFixed(2)}</span>
        </div>`;
    });

    document.getElementById("expected-pulls").innerHTML = pullsHtml;

    const totalCost = numPacks * packCost;
    const gain = totalExpectedValue - totalCost;

    document.getElementById("ev-raw").textContent = money(totalExpectedValue);
    document.getElementById("ev-cost").textContent = money(totalCost);

    const gainEl = document.getElementById("ev-gain");
    gainEl.textContent = money(gain);
    gainEl.className = "result-value " + chipClass(gain);

    // Probability bars — P(at least 1) = 1 − (1 − p)^n
    let probHtml = "";
    rarities.forEach(r => {
        const probAtLeast1 = r.pullRate > 0 ? 1 - Math.pow(1 - r.pullRate, numPacks) : 0;
        const barWidth = Math.min(100, probAtLeast1 * 100);

        probHtml += `
        <div class="prob-row">
            <span class="prob-label">${esc(r.rarityName)}</span>
            <div class="progress-wrap" style="flex:1;">
                <div class="progress-bar" style="width:${barWidth}%;"></div>
            </div>
            <span class="prob-pct">${(probAtLeast1 * 100).toFixed(1)}%</span>
        </div>`;
    });

    document.getElementById("probability-bars").innerHTML = probHtml;

    resultsSection.style.display = "block";
    document.getElementById("status-msg").textContent = `Calculated for ${numPacks} packs at ${money(packCost)}/pack`;
}

// Events
document.getElementById("set-select").addEventListener("change", function() {
    const code = this.value;
    if (code) {
        loadSetData(code);
    } else {
        document.getElementById("rarity-section").style.display = "none";
        document.getElementById("results-section").style.display = "none";
        document.getElementById("status-set").textContent = "No set selected";
        currentSetData = null;
    }
});

document.getElementById("calc-btn").addEventListener("click", calculate);

document.getElementById("pack-count").addEventListener("keydown", function(e) {
    if (e.key === "Enter") calculate();
});

// Init
loadSets();
