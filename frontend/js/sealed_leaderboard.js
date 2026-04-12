const API_BASE = "/api";

function money(x) {
    if (x === null || x === undefined || !Number.isFinite(Number(x))) return "\u2014";
    return Number(x).toLocaleString("en-US", {
        style: "currency", currency: "USD",
        minimumFractionDigits: 2, maximumFractionDigits: 2
    });
}

function rankClass(r) {
    if (r === 1) return "rank rank-1";
    if (r === 2) return "rank rank-2";
    if (r === 3) return "rank rank-3";
    return "rank";
}

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

// Color palette for window title bars per group
const titleColors = [
    "linear-gradient(90deg, #800000, #c04040)",
    "linear-gradient(90deg, #006400, #40a040)",
    "linear-gradient(90deg, #4b0082, #8040c0)",
    "linear-gradient(90deg, #8b6914, #c0a040)",
    "linear-gradient(90deg, #000080, #1084d0)",
    "linear-gradient(90deg, #800080, #c040c0)",
    "linear-gradient(90deg, #004040, #40a0a0)",
    "linear-gradient(90deg, #804000, #c08040)",
];

function renderSealedGroup(groupName, products, index) {
    const color = titleColors[index % titleColors.length];

    // Sort products by price descending
    const sorted = [...products].sort((a, b) => {
        const ap = Number(a.price || a.marketPrice) || 0;
        const bp = Number(b.price || b.marketPrice) || 0;
        return bp - ap;
    });

    let rowsHtml = "";
    sorted.forEach((p, i) => {
        const rank = i + 1;
        const name    = esc(p.name || p.productName || "\u2014");
        const setCode = esc(p["set-code"] || p.setCode || p.set || "\u2014");
        const price   = Number(p.price || p.marketPrice) || 0;
        const imgUrl  = esc(p["image-url"] || p.imageUrl || "");
        const id      = p.id || p["id"] || "";
        const href    = id ? `/card.html?id=${encodeURIComponent(id)}` : "";

        const imgCell = imgUrl
            ? `<img src="${imgUrl}" alt="" style="width:48px;height:64px;object-fit:contain;image-rendering:auto;" loading="lazy">`
            : "\u2014";

        // Make the row clickable when we have an id, with a hover affordance
        const rowAttrs = id
            ? `class="rowLink" data-href="${href}" style="cursor:pointer;"`
            : "";

        rowsHtml += `
        <tr ${rowAttrs}>
            <td class="text-center"><span class="${rankClass(rank)}">${rank}</span></td>
            <td>${imgCell}</td>
            <td>${name}</td>
            <td>${setCode}</td>
            <td class="text-right text-mono">${money(price)}</td>
        </tr>`;
    });

    return `
    <div class="window" style="margin-bottom: 8px;">
        <div class="window-title" style="background: ${color};">
            <span class="window-title-text">C:\\SEALED\\${groupName.toUpperCase().replace(/\s+/g, "_")}.DAT</span>
            <div class="window-controls">
                <button class="window-btn">_</button>
                <button class="window-btn">&square;</button>
                <button class="window-btn">X</button>
            </div>
        </div>
        <div class="section-explainer">
            ${groupName} &mdash; ${sorted.length} products tracked &middot; click any row for stats
        </div>
        <div class="window-body-flush">
            <div class="win-table-wrap">
                <div class="table-scroll">
                    <table class="win-table">
                        <thead>
                            <tr>
                                <th style="width:50px">#</th>
                                <th style="width:60px">IMAGE</th>
                                <th>PRODUCT NAME</th>
                                <th>SET</th>
                                <th>PRICE</th>
                            </tr>
                        </thead>
                        <tbody>${rowsHtml}</tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>`;
}

async function loadSealedLeaderboard() {
    const container = document.getElementById("sealed-container");
    const statusMsg = document.getElementById("status-msg");
    statusMsg.textContent = "Loading sealed data...";

    try {
        const res = await fetch(`${API_BASE}/sealed_leaderboard`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        // API returns groups — either as object { type: [...] } or array of groups
        let groups = {};
        let totalProducts = 0;

        if (data["sealed-type"]) {
            // API format: { "sealed-type": { "Booster Box": { count, rows: [...] }, ... } }
            const sealedType = data["sealed-type"];
            for (const [typeName, typeData] of Object.entries(sealedType)) {
                const rows = typeData.rows || typeData;
                if (Array.isArray(rows)) {
                    groups[typeName] = rows.map(r => ({
                        id: r.id || r["id"] || "",
                        name: r["product-name"] || r.name || "",
                        "set-code": r["set-code"] || r["set-name"] || "",
                        "image-url": r["image-url"] || r.imageUrl || "",
                        price: r["raw-price"] || r.price || 0,
                    }));
                }
            }
        } else if (Array.isArray(data)) {
            data.forEach(item => {
                const type = item.type || item.category || "Other";
                if (!groups[type]) groups[type] = [];
                groups[type].push(item);
            });
        } else {
            groups = data;
        }

        let html = '<div class="grid-2">';
        let idx = 0;

        for (const [groupName, products] of Object.entries(groups)) {
            if (!Array.isArray(products) || products.length === 0) continue;
            totalProducts += products.length;
            html += renderSealedGroup(groupName, products, idx);
            idx++;
        }

        html += "</div>";

        if (idx === 0) {
            container.innerHTML = '<div style="text-align:center;color:#808080;padding:20px;">No sealed data found.</div>';
        } else {
            container.innerHTML = html;
        }

        statusMsg.textContent = `Loaded ${idx} categories`;
        document.getElementById("status-count").textContent = `${totalProducts} products`;

        // Delegated row click → navigate to the sealed product detail page.
        // Attached after the HTML is built so all rows are in the DOM.
        container.addEventListener("click", function(e) {
            const tr = e.target.closest("tr.rowLink");
            if (!tr) return;
            const href = tr.dataset.href;
            if (href) window.location = href;
        });

    } catch (err) {
        console.error(err);
        statusMsg.textContent = `Error: ${err.message}`;
        container.innerHTML = '<div style="text-align:center;color:#cc0000;padding:20px;">Failed to load sealed data. Check connection.</div>';
    }
}

loadSealedLeaderboard();
