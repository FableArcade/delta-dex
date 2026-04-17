// Market regime chip — injects a bull/bear/sideways indicator into the
// menubar on every page, and shows a warning banner on the card
// leaderboard when the regime is bearish. Runs automatically on DOM ready.

(function () {
    "use strict";
    const API_BASE = "/api";
    const REGIME_STYLE = {
        bull:     { bg: "#d9f4d0", fg: "#145214", border: "#2d7a2d", label: "BULL",     icon: "▲" },
        bear:     { bg: "#f4d0d0", fg: "#521414", border: "#a02828", label: "BEAR",     icon: "▼" },
        sideways: { bg: "#efe8c8", fg: "#5a4b14", border: "#8a7828", label: "SIDEWAYS", icon: "→" },
    };

    function fmtPct(v) {
        if (v == null || !isFinite(v)) return "—";
        const s = v >= 0 ? "+" : "";
        return `${s}${(v * 100).toFixed(1)}%`;
    }

    function injectChip(regime) {
        if (document.getElementById("regime-chip")) return;   // avoid double-inject
        const bar = document.querySelector(".menubar");
        if (!bar) return;

        const style = REGIME_STYLE[regime.regime] || REGIME_STYLE.sideways;
        const chip = document.createElement("span");
        chip.id = "regime-chip";
        chip.title = (regime.description || "") +
            `  ·  30d ${fmtPct(regime["chase-30d-median-return"])}` +
            `  ·  90d ${fmtPct(regime["chase-90d-median-return"])}` +
            `  ·  basket ${regime["basket-size"] || 0} cards`;
        chip.style.cssText = [
            "margin-left: auto",
            "padding: 3px 10px",
            "font-size: 10px",
            "font-weight: bold",
            "letter-spacing: 0.05em",
            `background: ${style.bg}`,
            `color: ${style.fg}`,
            `border: 1px solid ${style.border}`,
            "border-radius: 2px",
            "font-family: inherit",
            "cursor: help",
            "user-select: none",
        ].join(";");
        chip.textContent = `${style.icon} ${style.label}  ${fmtPct(regime["chase-30d-median-return"])}`;
        bar.appendChild(chip);
    }

    function injectBearWarning(regime) {
        if (regime.regime !== "bear") return;
        if (!document.getElementById("card-tabs") &&
            !document.querySelector("[data-view='mustbuy']")) return;
        if (document.getElementById("regime-warning")) return;

        const banner = document.createElement("div");
        banner.id = "regime-warning";
        banner.style.cssText = [
            "margin: 8px 0",
            "padding: 10px 14px",
            "background: #fff4d6",
            "border: 1px solid #d0a030",
            "color: #604500",
            "font-size: 12px",
            "line-height: 1.5",
        ].join(";");
        // Build with DOM nodes — no innerHTML — so no XSS risk from future
        // server-controlled fields like `description`.
        const strong1 = document.createElement("strong");
        strong1.textContent = "⚠ Market regime is BEAR. ";
        const body = document.createTextNode(
            `Chase Index is in drawdown (30d ${fmtPct(regime["chase-30d-median-return"])}, ` +
            `90d ${fmtPct(regime["chase-90d-median-return"])}). The model's hit rates ` +
            `historically drop `
        );
        const strong2 = document.createElement("strong");
        strong2.textContent = "15-25%";
        const tail = document.createTextNode(
            ` during these periods. Picks below are still the best available — but consider ` +
            `sizing smaller, requiring confidence_low > 0, or waiting for the regime to turn.`
        );
        banner.appendChild(strong1);
        banner.appendChild(body);
        banner.appendChild(strong2);
        banner.appendChild(tail);

        const anchor = document.querySelector(".opp-tabs")
                    || document.getElementById("card-table")
                    || document.querySelector(".menubar");
        if (anchor && anchor.parentNode) {
            anchor.parentNode.insertBefore(banner, anchor);
        }
    }

    async function init() {
        try {
            const r = await fetch(`${API_BASE}/market/regime`);
            if (!r.ok) return;
            const regime = await r.json();
            injectChip(regime);
            injectBearWarning(regime);
        } catch (e) {
            /* ornamental — never block the page */
        }
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
