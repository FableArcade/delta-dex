"""PriceCharting HTML scraper.

Pulls raw + graded prices for every card in the ``cards`` table and writes a
snapshot row per (card_id, date) into ``price_history``.

Usage::

    python3 -m pipeline.scrapers.pricecharting_scraper
    python3 -m pipeline.scrapers.pricecharting_scraper --limit 10
    python3 -m pipeline.scrapers.pricecharting_scraper --card-id sv3pt5-161
"""

from __future__ import annotations

import argparse
import datetime as _dt
import logging
import re
import sys
from typing import Optional

from bs4 import BeautifulSoup

from db.connection import get_db
from pipeline.scrapers.base_scraper import BaseScraper, ScraperError


PRICE_LABEL_FIELD_MAP: dict[str, str] = {
    "ungraded": "raw_price",
    "grade 7": "psa_7_price",
    "grade 8": "psa_8_price",
    "grade 9": "psa_9_price",
    "psa 10": "psa_10_price",
}

_MONEY_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")
_VOLUME_RE = re.compile(r"([\d,]+)\s+sales?", re.IGNORECASE)


def _parse_money(text: str) -> Optional[float]:
    """Extract a dollar amount from an arbitrary string."""
    if not text:
        return None
    match = _MONEY_RE.search(text)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


class PriceChartingScraper(BaseScraper):
    name = "pricecharting"
    rate_limit = 2.0  # 2s/req
    rate_limit_jitter = 0.5

    BASE_URL = "https://www.pricecharting.com"

    # ------------------------------------------------------------------
    # Single card
    # ------------------------------------------------------------------

    def _card_url(self, card_id: str) -> str:
        """Resolve the canonical PriceCharting URL for a card.

        PriceCharting does NOT accept raw numeric IDs at /game/{id} — they
        404. The real URL is a slug like /game/pokemon-{set}/{card-slug}-{num}
        which we store per-card in ``cards.pc_canonical_url`` (populated by
        the bootstrap pass). Fall back to the unslugged form only when the
        canonical URL is missing, and let the caller handle the inevitable
        404 rather than silently using a broken URL.
        """
        with get_db() as db:
            row = db.execute(
                "SELECT pc_canonical_url FROM cards WHERE id = ?",
                (card_id,),
            ).fetchone()
        if row and row["pc_canonical_url"]:
            return row["pc_canonical_url"]
        # Last-resort fallback — will 404 on most cards, but lets the
        # bootstrap pass discover canonical URLs for new cards.
        return f"{self.BASE_URL}/game/{card_id}"

    def scrape_card(self, card_id: str) -> dict:
        """Fetch and parse pricing data for a single card id.

        Returns a dict with ``raw_price``, ``psa_7_price``, ``psa_8_price``,
        ``psa_9_price``, ``psa_10_price``, and ``sales_volume``.
        """
        url = self._card_url(card_id)
        self.logger.info("scrape_card %s -> %s", card_id, url)
        soup = self._get(url)

        result: dict = {
            "raw_price": None,
            "psa_7_price": None,
            "psa_8_price": None,
            "psa_9_price": None,
            "psa_10_price": None,
            "sales_volume": None,
        }

        self._extract_price_table(soup, result)

        if all(result[f] is None for f in PRICE_LABEL_FIELD_MAP.values()):
            # Fallback: look for .price class elements near labels.
            self._extract_price_class(soup, result)

        result["sales_volume"] = self._extract_volume(soup)

        return result

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _extract_price_table(self, soup: BeautifulSoup, out: dict) -> None:
        """Extract prices from PriceCharting's TCG price table.

        The TCG layout uses HORIZONTAL price rows where one row carries the
        grade labels and the next row carries the matching prices by column:

            <tr> Ungraded | Grade 7 | Grade 8 | Grade 9 | Grade 9.5 | PSA 10 </tr>
            <tr>  $110    |  $149   |  $180   |  $240   |  $360     |  $710  </tr>

        The prior vertical (label-in-col-0, price-in-col-1) walk broke with
        this layout. Strategy: for each row that looks like a label row
        (has ≥ 2 cells and at least one matches a known label), grab the
        NEXT row and zip label columns with price columns by index.
        """
        rows = soup.find_all("tr")
        for i, row in enumerate(rows):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            # Check if this row is a "label row" — multiple cells whose text
            # looks like price-grade labels.
            labels = [c.get_text(" ", strip=True).lower() for c in cells]
            matched_fields = [self._match_label(lbl) for lbl in labels]
            label_hits = sum(1 for f in matched_fields if f)

            if label_hits >= 2 and i + 1 < len(rows):
                # Horizontal layout: pull the next row as prices by column.
                price_cells = rows[i + 1].find_all(["td", "th"])
                for col_idx, field in enumerate(matched_fields):
                    if not field or col_idx >= len(price_cells):
                        continue
                    price = _parse_money(price_cells[col_idx].get_text(" ", strip=True))
                    if price is not None and out.get(field) is None:
                        out[field] = price
                continue

            # Otherwise try the vertical (label, price) layout as a fallback.
            label = labels[0]
            field = self._match_label(label)
            if not field:
                continue
            for cell in cells[1:]:
                price = _parse_money(cell.get_text(" ", strip=True))
                if price is not None:
                    out[field] = price
                    break

    def _extract_price_class(self, soup: BeautifulSoup, out: dict) -> None:
        """Fallback parser: look for elements with class='price' labeled by siblings."""
        for el in soup.select(".price"):
            text = el.get_text(" ", strip=True)
            price = _parse_money(text)
            if price is None:
                continue
            # Try to find a nearby label (previous sibling / parent text).
            container = el.parent
            label_text = ""
            if container:
                label_text = container.get_text(" ", strip=True).lower()
            field = self._match_label(label_text)
            if field and out.get(field) is None:
                out[field] = price

    @staticmethod
    def _match_label(label: str) -> Optional[str]:
        label = label.lower()
        for key, field in PRICE_LABEL_FIELD_MAP.items():
            if key in label:
                return field
        return None

    def _extract_volume(self, soup: BeautifulSoup) -> Optional[int]:
        """Best-effort parse of recent sales volume."""
        text = soup.get_text(" ", strip=True)
        match = _VOLUME_RE.search(text)
        if not match:
            return None
        try:
            return int(match.group(1).replace(",", ""))
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _already_updated_today(self, card_id: str, snapshot_date: str) -> bool:
        with get_db() as db:
            row = db.execute(
                "SELECT 1 FROM price_history WHERE card_id = ? AND date = ?",
                (card_id, snapshot_date),
            ).fetchone()
        return row is not None

    def _write_row(self, card_id: str, snapshot_date: str, data: dict) -> None:
        raw = data.get("raw_price")
        psa_10 = data.get("psa_10_price")
        vs_raw = None
        vs_raw_pct = None
        if raw and psa_10:
            vs_raw = psa_10 - raw
            vs_raw_pct = (vs_raw / raw) * 100.0 if raw else None

        with get_db() as db:
            db.execute(
                """
                INSERT OR REPLACE INTO price_history (
                    card_id, date,
                    raw_price, psa_7_price, psa_8_price, psa_9_price, psa_10_price,
                    psa_10_vs_raw, psa_10_vs_raw_pct,
                    sales_volume, interpolated, interpolation_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
                """,
                (
                    card_id,
                    snapshot_date,
                    data.get("raw_price"),
                    data.get("psa_7_price"),
                    data.get("psa_8_price"),
                    data.get("psa_9_price"),
                    data.get("psa_10_price"),
                    vs_raw,
                    vs_raw_pct,
                    data.get("sales_volume"),
                ),
            )

    # ------------------------------------------------------------------
    # Bulk run
    # ------------------------------------------------------------------

    def _load_cards(self, limit: Optional[int] = None) -> list[dict]:
        # Only scrape cards with a known PC canonical URL. Unresolved cards
        # should be backfilled by the bootstrap pass, not pounded daily with
        # 404s that blow our rate budget.
        with get_db() as db:
            rows = db.execute(
                "SELECT id, product_name, set_code, card_number "
                "FROM cards "
                "WHERE sealed_product = 'N' "
                "  AND pc_canonical_url IS NOT NULL "
                "ORDER BY set_code, card_number"
            ).fetchall()
        cards = [dict(r) for r in rows]
        if limit:
            cards = cards[:limit]
        return cards

    def scrape_all_cards(
        self,
        date: "Optional[_dt.date | str]" = None,
        *,
        limit: Optional[int] = None,
    ) -> dict:
        """Iterate all non-sealed cards and write a price_history snapshot."""
        # Accept either a date object (CLI usage) or an ISO string (orchestrator).
        if date is None:
            snapshot_date = _dt.date.today().isoformat()
        elif isinstance(date, str):
            snapshot_date = date
        else:
            snapshot_date = date.isoformat()
        cards = self._load_cards(limit=limit)
        self.logger.info(
            "scrape_all_cards date=%s cards=%d", snapshot_date, len(cards)
        )

        processed = 0
        skipped = 0
        errors = 0

        for card in cards:
            card_id = card["id"]
            try:
                if self._already_updated_today(card_id, snapshot_date):
                    skipped += 1
                    self.logger.debug("skip (already updated today): %s", card_id)
                    continue
                data = self.scrape_card(card_id)
                if all(
                    data[f] is None for f in PRICE_LABEL_FIELD_MAP.values()
                ):
                    self.logger.warning(
                        "no prices found for %s (%s)", card_id, card.get("product_name")
                    )
                self._write_row(card_id, snapshot_date, data)
                processed += 1
                self.logger.info(
                    "ok %s raw=%s psa10=%s",
                    card_id,
                    data.get("raw_price"),
                    data.get("psa_10_price"),
                )
            except Exception as exc:  # keep going on individual failures
                errors += 1
                self.logger.error("error scraping %s: %s", card_id, exc)
                continue

        summary = {
            "processed": processed,
            "skipped": skipped,
            "errors": errors,
            "date": snapshot_date,
        }
        self.logger.info("scrape_all_cards done %s", summary)
        return summary


# ======================================================================
# CLI
# ======================================================================


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Scrape PriceCharting prices into price_history."
    )
    p.add_argument("--limit", type=int, default=None, help="Limit to N cards (testing)")
    p.add_argument(
        "--card-id",
        type=str,
        default=None,
        help="Scrape a single card id (test mode, prints result, no DB write)",
    )
    p.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass the 24h HTML cache",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging"
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(name)-28s | %(levelname)-5s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with PriceChartingScraper(use_cache=not args.no_cache) as scraper:
        if args.card_id:
            try:
                data = scraper.scrape_card(args.card_id)
            except ScraperError as exc:
                scraper.logger.error("scrape failed: %s", exc)
                return 1
            print(data)
            return 0

        summary = scraper.scrape_all_cards(limit=args.limit)
        print(summary)
        return 0 if summary["errors"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
