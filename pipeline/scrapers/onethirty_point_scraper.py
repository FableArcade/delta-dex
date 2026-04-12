"""130point.com HTML scraper — aggregates eBay sold listings.

130point is a free public aggregator of eBay sold listings. We respect that by
running a generous 3-second per-request throttle and caching HTML for 24h.

For every card in the database we:

    1. Build a search query from the card name + number
    2. Hit ``https://130point.com/sales/?search={q}&type=auction``
    3. Parse the sold-listings table
    4. Classify each row as raw / PSA 10 / PSA 9 / PSA 8 / PSA 7 / other-graded
    5. Aggregate the past 7 days of sales into counts + averages
    6. Write a snapshot row into ``ebay_history``

Usage::

    python3 -m pipeline.scrapers.onethirty_point_scraper
    python3 -m pipeline.scrapers.onethirty_point_scraper --limit 5
    python3 -m pipeline.scrapers.onethirty_point_scraper --card-id sv3pt5-161
"""

from __future__ import annotations

import argparse
import datetime as _dt
import logging
import re
import sys
import urllib.parse
from statistics import mean
from typing import Optional

from bs4 import BeautifulSoup

from db.connection import get_db
from pipeline.scrapers.base_scraper import BaseScraper, ScraperError


_MONEY_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")

# Date formats 130point may render.
_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%b %d, %Y",
    "%b %d %Y",
    "%d %b %Y",
)

# Grade classification regexes (ordered: longest/most-specific first).
_GRADE_REGEXES: list[tuple[str, re.Pattern[str]]] = [
    ("psa_10", re.compile(r"\bpsa\s*10\b", re.IGNORECASE)),
    ("psa_9", re.compile(r"\bpsa\s*9\b", re.IGNORECASE)),
    ("psa_8", re.compile(r"\bpsa\s*8\b", re.IGNORECASE)),
    ("psa_7", re.compile(r"\bpsa\s*7\b", re.IGNORECASE)),
    ("cgc_10", re.compile(r"\bcgc\s*(?:pristine\s*)?10\b", re.IGNORECASE)),
    ("bgs_10", re.compile(r"\bbgs\s*(?:pristine\s*|black\s*label\s*)?10\b", re.IGNORECASE)),
    ("other_graded", re.compile(
        r"\b(psa|cgc|bgs|sgc|ace)\s*\d+(?:\.\d)?\b", re.IGNORECASE,
    )),
    ("raw", re.compile(r"\b(raw|ungraded)\b", re.IGNORECASE)),
]


def _parse_money(text: str) -> Optional[float]:
    if not text:
        return None
    match = _MONEY_RE.search(text)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _parse_date(text: str) -> Optional[_dt.date]:
    if not text:
        return None
    text = text.strip()
    for fmt in _DATE_FORMATS:
        try:
            return _dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    # Try to extract an ISO-ish date from noisier text
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        try:
            return _dt.date.fromisoformat(m.group(1))
        except ValueError:
            return None
    return None


class OneThirtyPointScraper(BaseScraper):
    name = "onethirty_point"
    rate_limit = 3.0  # be polite — free aggregator
    rate_limit_jitter = 0.5

    BASE_URL = "https://130point.com/sales/"
    WINDOW_DAYS = 7

    # ------------------------------------------------------------------
    # Query building
    # ------------------------------------------------------------------

    @staticmethod
    def _build_query(card: dict) -> str:
        """Build a search query for a card dict.

        Prefer ``ebay_q_phrase`` / ``ebay_q_num`` if they exist, otherwise
        fall back to ``product_name`` + ``card_number``.
        """
        phrase = (card.get("ebay_q_phrase") or "").strip()
        q_num = (card.get("ebay_q_num") or "").strip()
        if phrase and q_num:
            return f"{phrase} {q_num}".strip()
        if phrase:
            return phrase
        name = (card.get("product_name") or "").strip()
        number = card.get("card_number")
        if number is not None:
            return f"{name} {number}".strip()
        return name

    def _search_url(self, query: str) -> str:
        params = {"search": query, "type": "auction"}
        return f"{self.BASE_URL}?{urllib.parse.urlencode(params)}"

    # ------------------------------------------------------------------
    # Grade classification
    # ------------------------------------------------------------------

    @staticmethod
    def classify_listing_grade(title: str) -> str:
        """Classify a listing title into a grade bucket.

        Returns one of:
            psa_10, psa_9, psa_8, psa_7, cgc_10, bgs_10,
            other_graded, raw, unknown
        """
        if not title:
            return "unknown"
        for label, regex in _GRADE_REGEXES:
            if regex.search(title):
                return label
        # If no grading company is mentioned at all, treat as raw.
        if not re.search(r"\b(psa|cgc|bgs|sgc|ace)\b", title, re.IGNORECASE):
            return "raw"
        return "unknown"

    # ------------------------------------------------------------------
    # HTML parsing
    # ------------------------------------------------------------------

    def _parse_sales_table(self, soup: BeautifulSoup) -> list[dict]:
        """Extract a list of sold-listing dicts from the results page."""
        listings: list[dict] = []

        # Try a structured table first.
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            header_cells = [
                c.get_text(" ", strip=True).lower() for c in rows[0].find_all(["th", "td"])
            ]
            if not any("price" in h or "sold" in h for h in header_cells):
                continue

            # Try to find column indexes.
            def _idx(*needles: str) -> Optional[int]:
                for i, h in enumerate(header_cells):
                    if any(n in h for n in needles):
                        return i
                return None

            price_idx = _idx("price", "sold")
            date_idx = _idx("date", "ended")
            title_idx = _idx("title", "item", "listing")
            seller_idx = _idx("seller")

            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                get = lambda i: (
                    cells[i].get_text(" ", strip=True)
                    if i is not None and i < len(cells)
                    else ""
                )
                title = get(title_idx)
                link_el = cells[title_idx].find("a") if title_idx is not None and title_idx < len(cells) else None
                link = link_el.get("href") if link_el else None

                listing = {
                    "price": _parse_money(get(price_idx)),
                    "date": _parse_date(get(date_idx)),
                    "title": title,
                    "seller": get(seller_idx),
                    "link": link,
                }
                if listing["price"] is None and not title:
                    continue
                listings.append(listing)

            if listings:
                return listings

        # Fallback: any anchor containing an eBay item url grouped with a money value.
        for item in soup.select("tr, li, .sale, .result"):
            text = item.get_text(" ", strip=True)
            price = _parse_money(text)
            if price is None:
                continue
            date = _parse_date(text)
            link_el = item.find("a")
            listings.append(
                {
                    "price": price,
                    "date": date,
                    "title": text[:200],
                    "seller": None,
                    "link": link_el.get("href") if link_el else None,
                }
            )

        return listings

    # ------------------------------------------------------------------
    # Per-card scrape + aggregate
    # ------------------------------------------------------------------

    def scrape_card_sales(self, card_dict: dict) -> dict:
        """Scrape 130point for a card and aggregate the past 7 days of sales."""
        query = self._build_query(card_dict)
        if not query:
            raise ScraperError(f"Cannot build search query for card {card_dict}")
        url = self._search_url(query)
        self.logger.info("scrape_card_sales q=%r -> %s", query, url)

        soup = self._get(url)
        listings = self._parse_sales_table(soup)

        today = _dt.date.today()
        cutoff = today - _dt.timedelta(days=self.WINDOW_DAYS)

        buckets: dict[str, list[float]] = {
            "raw": [],
            "psa_10": [],
            "psa_9": [],
            "psa_8": [],
            "psa_7": [],
            "other_graded": [],
        }

        kept = 0
        for row in listings:
            price = row.get("price")
            if price is None or price <= 0:
                continue
            date = row.get("date")
            # If we can't parse the date, assume it's inside the window
            # (130point's front page is date-sorted newest first).
            if date is not None and (date < cutoff or date > today):
                continue
            grade = self.classify_listing_grade(row.get("title") or "")
            # Collapse cgc_10 / bgs_10 into "other_graded" for the schema.
            if grade in ("cgc_10", "bgs_10"):
                grade = "other_graded"
            if grade == "unknown":
                continue
            if grade not in buckets:
                continue
            buckets[grade].append(price)
            kept += 1

        def _avg(vals: list[float]) -> Optional[float]:
            return round(mean(vals), 2) if vals else None

        aggregated = {
            "query": query,
            "listings_seen": len(listings),
            "listings_in_window": kept,
            "ended_raw": len(buckets["raw"]),
            "ended_psa_10": len(buckets["psa_10"]),
            "ended_psa_9": len(buckets["psa_9"]),
            "ended_psa_8": len(buckets["psa_8"]),
            "ended_psa_7": len(buckets["psa_7"]),
            "ended_other_graded": len(buckets["other_graded"]),
            "ended_avg_raw_price": _avg(buckets["raw"]),
            "ended_avg_psa_10_price": _avg(buckets["psa_10"]),
            "ended_avg_psa_9_price": _avg(buckets["psa_9"]),
            "ended_avg_psa_8_price": _avg(buckets["psa_8"]),
            "ended_avg_psa_7_price": _avg(buckets["psa_7"]),
            "ended_avg_other_graded_price": _avg(buckets["other_graded"]),
        }
        return aggregated

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _already_updated_today(self, card_id: str, snapshot_date: str) -> bool:
        with get_db() as db:
            row = db.execute(
                "SELECT 1 FROM ebay_history WHERE card_id = ? AND date = ?",
                (card_id, snapshot_date),
            ).fetchone()
        return row is not None

    def _write_row(
        self,
        card_id: str,
        snapshot_date: str,
        from_date: str,
        data: dict,
    ) -> None:
        ended_total = (
            data.get("ended_raw", 0)
            + data.get("ended_psa_10", 0)
            + data.get("ended_psa_9", 0)
            + data.get("ended_psa_8", 0)
            + data.get("ended_psa_7", 0)
            + data.get("ended_other_graded", 0)
        )
        ended_graded = ended_total - data.get("ended_raw", 0)

        with get_db() as db:
            db.execute(
                """
                INSERT OR REPLACE INTO ebay_history (
                    card_id, date, from_date,
                    ended, ended_raw, ended_graded,
                    ended_psa_10, ended_psa_9, ended_other_10,
                    ended_avg_raw_price, ended_avg_psa_10_price,
                    ended_avg_psa_9_price, ended_avg_other_10_price,
                    interpolated
                ) VALUES (
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    0
                )
                """,
                (
                    card_id,
                    snapshot_date,
                    from_date,
                    float(ended_total),
                    float(data.get("ended_raw", 0)),
                    float(ended_graded),
                    float(data.get("ended_psa_10", 0)),
                    float(data.get("ended_psa_9", 0)),
                    float(data.get("ended_other_graded", 0)),
                    data.get("ended_avg_raw_price"),
                    data.get("ended_avg_psa_10_price"),
                    data.get("ended_avg_psa_9_price"),
                    data.get("ended_avg_other_graded_price"),
                ),
            )

    # ------------------------------------------------------------------
    # Bulk run
    # ------------------------------------------------------------------

    def _load_cards(
        self,
        limit: Optional[int] = None,
        card_id: Optional[str] = None,
    ) -> list[dict]:
        with get_db() as db:
            if card_id:
                rows = db.execute(
                    "SELECT * FROM cards WHERE id = ?", (card_id,)
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT * FROM cards WHERE sealed_product = 'N' "
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
        # Accept either a date object (CLI usage) or an ISO string (orchestrator).
        if date is None:
            snapshot_date_obj = _dt.date.today()
        elif isinstance(date, str):
            snapshot_date_obj = _dt.date.fromisoformat(date)
        else:
            snapshot_date_obj = date
        snapshot_date = snapshot_date_obj.isoformat()
        from_date = (
            snapshot_date_obj - _dt.timedelta(days=self.WINDOW_DAYS)
        ).isoformat()

        cards = self._load_cards(limit=limit)
        self.logger.info(
            "scrape_all_cards date=%s from=%s cards=%d",
            snapshot_date,
            from_date,
            len(cards),
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
                data = self.scrape_card_sales(card)
                self._write_row(card_id, snapshot_date, from_date, data)
                processed += 1
                self.logger.info(
                    "ok %s in_window=%d raw=%d psa10=%d psa9=%d",
                    card_id,
                    data.get("listings_in_window", 0),
                    data.get("ended_raw", 0),
                    data.get("ended_psa_10", 0),
                    data.get("ended_psa_9", 0),
                )
            except Exception as exc:
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
        description="Scrape 130point sold listings into ebay_history."
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

    with OneThirtyPointScraper(use_cache=not args.no_cache) as scraper:
        if args.card_id:
            cards = scraper._load_cards(card_id=args.card_id)
            if not cards:
                scraper.logger.error("card %s not found", args.card_id)
                return 1
            try:
                data = scraper.scrape_card_sales(cards[0])
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
