const API_BASE = "/api";

function money(x) {
    if (x === null || x === undefined || !Number.isFinite(Number(x))) return "\u2014";
    return Number(x).toLocaleString("en-US", {
        style: "currency", currency: "USD",
        minimumFractionDigits: 2, maximumFractionDigits: 2
    });
}

// Populate rarity dropdown
async function loadRarities() {
    try {
        const res = await fetch(`${API_BASE}/search/rarities`);
        if (!res.ok) return;
        const data = await res.json();
        const rarities = Array.isArray(data) ? data : (data.rarities || []);
        const select = document.getElementById("rarity-filter");
        rarities.forEach(r => {
            const name = typeof r === "string" ? r : (r.name || r.rarity || "");
            if (!name) return;
            const opt = document.createElement("option");
            opt.value = name;
            opt.textContent = name;
            select.appendChild(opt);
        });
    } catch (e) {
        console.warn("Could not load rarities:", e);
    }
}

function renderCard(card) {
    const imgUrl = card["image-url"] || card.imageUrl || "";
    const name = card.name || "\u2014";
    const setCode = card["set-code"] || card.setCode || "";
    const rarity = card.rarity || "";
    const rawPrice = Number(card["raw-price"] || card.rawPrice) || 0;
    const psa10Price = Number(card["psa10-price"] || card.psa10Price) || 0;
    const cardId = card.id || card._id || "";

    return `
    <div class="window card-tile" onclick="window.location='/card.html?id=${encodeURIComponent(cardId)}'">
        <div class="window-title" style="font-size:11px; padding: 2px 4px;">
            <span class="window-title-text" style="font-size:10px;">${setCode}</span>
            <div class="window-controls">
                <button class="window-btn" style="width:12px;height:11px;font-size:7px;">X</button>
            </div>
        </div>
        <div class="window-body">
            ${imgUrl ? `<img src="${imgUrl}" alt="${name}" loading="lazy">` : '<div style="height:120px;display:flex;align-items:center;justify-content:center;color:#808080;">No Image</div>'}
            <div class="card-name" title="${name}">${name}</div>
            <div class="card-meta">${rarity}</div>
            <div class="card-prices">
                <div>Raw: <b>${money(rawPrice)}</b></div>
                <div>PSA 10: <b>${money(psa10Price)}</b></div>
            </div>
        </div>
    </div>`;
}

async function performSearch() {
    const query = document.getElementById("search-input").value.trim();
    const rarity = document.getElementById("rarity-filter").value;
    const sortVal = document.getElementById("sort-filter").value;
    const grid = document.getElementById("results-grid");
    const statusMsg = document.getElementById("status-msg");
    const statusCount = document.getElementById("status-count");

    if (!query) {
        grid.innerHTML = '<div style="grid-column:1/-1;text-align:center;color:#808080;padding:40px;">Enter a search term above to find cards.</div>';
        statusCount.textContent = "0 results";
        return;
    }

    statusMsg.textContent = "Searching...";
    grid.innerHTML = '<div style="grid-column:1/-1;text-align:center;color:#808080;padding:40px;">Searching...</div>';

    // Map sort value to API params
    let sortParam = "";
    switch (sortVal) {
        case "psa10-desc": sortParam = "psa10-desc"; break;
        case "psa10-asc": sortParam = "psa10-asc"; break;
        case "raw-desc": sortParam = "raw-desc"; break;
        case "raw-asc": sortParam = "raw-asc"; break;
        case "name-asc": sortParam = "name-asc"; break;
        case "name-desc": sortParam = "name-desc"; break;
        default: sortParam = "psa10-desc";
    }

    const params = new URLSearchParams();
    params.set("q", query);
    if (rarity) params.set("rarity", rarity);
    if (sortParam) params.set("sort", sortParam);
    params.set("limit", "100");

    try {
        const res = await fetch(`${API_BASE}/search/cards?${params.toString()}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const cards = Array.isArray(data) ? data : (data.cards || data.results || []);

        if (cards.length === 0) {
            grid.innerHTML = '<div style="grid-column:1/-1;text-align:center;color:#808080;padding:40px;">No cards found matching your search.</div>';
            statusMsg.textContent = "Search complete";
            statusCount.textContent = "0 results";
            return;
        }

        grid.innerHTML = cards.map(renderCard).join("");
        statusMsg.textContent = "Search complete";
        statusCount.textContent = `${cards.length} result${cards.length !== 1 ? "s" : ""}`;

    } catch (err) {
        console.error(err);
        statusMsg.textContent = `Error: ${err.message}`;
        grid.innerHTML = '<div style="grid-column:1/-1;text-align:center;color:#cc0000;padding:40px;">Search failed. Check connection.</div>';
    }
}

// Event listeners
document.getElementById("search-btn").addEventListener("click", performSearch);
document.getElementById("search-input").addEventListener("keydown", function(e) {
    if (e.key === "Enter") performSearch();
});

// Init
loadRarities();
