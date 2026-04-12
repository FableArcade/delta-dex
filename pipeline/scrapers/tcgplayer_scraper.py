"""TCGPlayer HTML scraper -- pulls Near Mint market prices from the public
price guide and individual product pages.

TCGPlayer sits behind Cloudflare.  We use realistic Chrome 146 headers, a
5-second rate limit, and prefer bulk price-guide scraping (one request per
set) over per-card lookups.  If Cloudflare blocks us we log a warning and
move on -- we do not try to bypass the protection.

Writes results to the ``justtcg_history`` table with the TCGPlayer NM market
price stored in the ``j_raw_price`` column (used as a proxy source).
"""

from __future__ import annotations

import argparse
import logging
import random
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from bs4 import BeautifulSoup

from db.connection import get_db
from pipeline.scrapers.base_scraper import BaseScraper


# ----------------------------------------------------------------------
# User-Agent pool (Chrome 146 family on macOS/Windows/Linux)
# ----------------------------------------------------------------------

_USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.7342.60 Safari/537.36",
]


def _default_headers(user_agent: Optional[str] = None) -> Dict[str, str]:
    """Return a realistic Chrome browser header set."""
    ua = user_agent or random.choice(_USER_AGENTS)
    return {
        "User-Agent": ua,
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Ch-Ua": '"Chromium";v="146", "Not?A_Brand";v="24", "Google Chrome";v="146"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.tcgplayer.com/",
        "DNT": "1",
    }


# ----------------------------------------------------------------------
# Scraper
# ----------------------------------------------------------------------


class TCGPlayerScraper(BaseScraper):
    """Scrape TCGPlayer market prices (NM) for Pokemon singles."""

    name = "tcgplayer_scraper"
    rate_limit = 0.2  # 1 request per 5 seconds -- TCGPlayer has CF protection

    BASE_URL = "https://www.tcgplayer.com"
    SEARCH_URL = f"{BASE_URL}/search/pokemon/product"
    PRICE_GUIDE_URL = f"{BASE_URL}/price-guide/pokemon"

    # Known set-slug overrides for sets whose slugs don't match their codes.
    # Populated as new sets are discovered.
    SET_SLUG_OVERRIDES: Dict[str, str] = {
        "SV9": "scarlet-and-violet-journey-together",
        "SV8": "scarlet-and-violet-surging-sparks",
        "SV7": "scarlet-and-violet-stellar-crown",
        "SV6": "scarlet-and-violet-twilight-masquerade",
        "MEW": "scarlet-and-violet-151",
        "PAL": "scarlet-and-violet-paldea-evolved",
        "SVI": "scarlet-and-violet",
    }

    def __init__(self) -> None:
        super().__init__()
        self._user_agent = random.choice(_USER_AGENTS)

    # ------------------------------------------------------------------
    # HTTP helpers with CF-aware fallback
    # ------------------------------------------------------------------

    def _rotate_user_agent(self) -> None:
        self._user_agent = random.choice(_USER_AGENTS)

    def _browser_headers(self) -> Dict[str, str]:
        return _default_headers(self._user_agent)

    def _fetch_html(self, url: str, params: Optional[dict] = None) -> Optional[str]:
        """Fetch a page, returning HTML text or None if Cloudflare blocks us."""
        # Use the BaseScraper httpx client + per-request throttle, but supply
        # our own Chrome 146 browser headers (BaseScraper._fetch hard-codes a
        # different UA pool and is wrapped in a retry decorator that doesn't
        # play nicely with the Cloudflare 403/429/503 swallow logic below).
        self._throttle()
        try:
            resp = self.client.get(
                url, params=params, headers=self._browser_headers()
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status in (403, 429, 503):
                self.logger.warning(
                    "TCGPlayer blocked (%d) for %s -- Cloudflare protection; skipping",
                    status, url,
                )
                self._rotate_user_agent()
                return None
            self.logger.warning("TCGPlayer HTTP %d for %s: %s", status, url, exc)
            return None
        except httpx.RequestError as exc:
            self.logger.warning("TCGPlayer request failed for %s: %s", url, exc)
            return None

        text = resp.text or ""
        # Simple Cloudflare challenge detection
        low = text.lower()
        if (
            "just a moment" in low
            or "cf-browser-verification" in low
            or "challenge-platform" in low
            or "attention required" in low
        ):
            self.logger.warning(
                "TCGPlayer returned a Cloudflare challenge page for %s; skipping", url,
            )
            self._rotate_user_agent()
            return None

        return text

    # ------------------------------------------------------------------
    # Slug helpers
    # ------------------------------------------------------------------

    def _set_slug(self, set_code: str, set_name: Optional[str] = None) -> str:
        """Return the TCGPlayer price-guide slug for a set."""
        if set_code in self.SET_SLUG_OVERRIDES:
            return self.SET_SLUG_OVERRIDES[set_code]
        if set_name:
            slug = re.sub(r"[^\w\s-]", "", set_name.lower())
            slug = re.sub(r"\s+", "-", slug).strip("-")
            return slug
        return set_code.lower()

    # ------------------------------------------------------------------
    # Single-card lookup (fallback)
    # ------------------------------------------------------------------

    def scrape_card_price(self, card: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Look up a single card on TCGPlayer search and return price data.

        Returns a dict with keys:
            card_id, market_price, low_price, mid_price
        or ``None`` on failure / no match / Cloudflare block.
        """
        card_id = card.get("id")
        name = card.get("product_name") or ""
        number = card.get("card_number")

        q_parts: List[str] = []
        if name:
            q_parts.append(str(name))
        if number is not None:
            q_parts.append(str(number))
        q = " ".join(q_parts).strip()

        if not q:
            return None

        params = {"q": q, "view": "grid"}
        html = self._fetch_html(self.SEARCH_URL, params=params)
        if html is None:
            return None

        prices = self._parse_search_results(html, card)
        if prices is None:
            return None

        return {
            "card_id": card_id,
            "market_price": prices.get("market_price"),
            "low_price": prices.get("low_price"),
            "mid_price": prices.get("mid_price"),
        }

    def _parse_search_results(
        self, html: str, card: Dict[str, Any]
    ) -> Optional[Dict[str, Optional[float]]]:
        """Parse TCGPlayer search results for the first matching product."""
        soup = BeautifulSoup(html, "html.parser")

        card_name = (card.get("product_name") or "").lower()
        card_number = str(card.get("card_number") or "").strip()

        results = soup.select(
            ".search-result, .product-card__product, .product-card, [data-testid='product-card']"
        )

        best_price: Optional[Dict[str, Optional[float]]] = None
        for result in results:
            title_el = result.select_one(
                ".product-card__title, .search-result__title, h3, h4, a"
            )
            title = title_el.get_text(strip=True).lower() if title_el else ""

            # Loose match -- require the first significant name word
            name_words = [w for w in card_name.split() if len(w) > 2]
            if name_words and name_words[0] not in title:
                continue

            # If we have a card number, try to match it too
            if card_number and card_number not in title and card_number not in result.get_text():
                # Not a deal-breaker; keep searching but track as fallback
                if best_price is None:
                    best_price = self._extract_prices_from_element(result)
                continue

            prices = self._extract_prices_from_element(result)
            if prices and prices.get("market_price") is not None:
                return prices

        return best_price

    @staticmethod
    def _extract_prices_from_element(el) -> Dict[str, Optional[float]]:
        """Pull market / low / mid dollar amounts from a listing element."""
        prices: Dict[str, Optional[float]] = {
            "market_price": None,
            "low_price": None,
            "mid_price": None,
        }

        # Market price -- look for labels like "Market Price", ".market-price"
        for node in el.select(
            ".product-card__market-price--value, .market-price, "
            ".product-card__market-price, [data-testid='market-price']"
        ):
            val = _parse_price(node.get_text())
            if val is not None:
                prices["market_price"] = val
                break

        # Generic fallback: first dollar amount in the card
        if prices["market_price"] is None:
            val = _parse_price(el.get_text())
            if val is not None:
                prices["market_price"] = val

        # Low / mid from label+value rows if present
        for label_el in el.select(".price-range, .price-points, .price__label, td"):
            text = label_el.get_text(" ", strip=True).lower()
            if "low" in text and prices["low_price"] is None:
                prices["low_price"] = _parse_price(text)
            if "mid" in text and prices["mid_price"] is None:
                prices["mid_price"] = _parse_price(text)

        return prices

    # ------------------------------------------------------------------
    # Bulk price-guide scraping (PREFERRED)
    # ------------------------------------------------------------------

    def scrape_set_prices(self, set_code: str) -> List[Dict[str, Any]]:
        """Bulk-fetch a set's price-guide page and return a list of card price dicts.

        Returns a list of:
            {card_id, market_price, low_price, mid_price}
        Cards that couldn't be matched are skipped.
        """
        set_info = self._get_set_info(set_code)
        if not set_info:
            self.logger.warning("Unknown set %s -- skipping", set_code)
            return []

        slug = self._set_slug(set_code, set_info.get("set_name"))
        url = f"{self.PRICE_GUIDE_URL}/{slug}"
        self.logger.info("Fetching TCGPlayer price guide: %s", url)

        html = self._fetch_html(url)
        if html is None:
            return []

        cards_in_set = self._get_cards_for_set(set_code)
        if not cards_in_set:
            self.logger.debug("No cards for set %s, skipping parse", set_code)
            return []

        return self._parse_price_guide(html, cards_in_set)

    def _parse_price_guide(
        self, html: str, cards_in_set: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Parse a TCGPlayer price-guide HTML table into per-card price rows."""
        soup = BeautifulSoup(html, "html.parser")

        # Build lookup keys: number and name
        by_number: Dict[str, Dict[str, Any]] = {}
        by_name: Dict[str, Dict[str, Any]] = {}
        for c in cards_in_set:
            num = c.get("card_number")
            if num is not None:
                by_number[str(num).strip()] = c
            nm = (c.get("product_name") or "").lower().strip()
            if nm:
                by_name[nm] = c

        results: List[Dict[str, Any]] = []
        seen_card_ids: set = set()

        # TCGPlayer price guide tables
        table = (
            soup.select_one("table.priceGuideTable")
            or soup.select_one("table.price-guide-table")
            or soup.select_one("table.priceGuide__table")
            or soup.select_one("table")
        )
        if not table:
            self.logger.warning("No price-guide table found on page")
            return results

        header_row = table.select_one("thead tr") or table.select_one("tr")
        if not header_row:
            return results

        headers = [
            th.get_text(" ", strip=True).lower()
            for th in header_row.find_all(["th", "td"])
        ]

        col_map = self._map_price_columns(headers)
        if not col_map:
            self.logger.warning(
                "Could not map TCGPlayer price-guide columns from headers: %s", headers,
            )
            return results

        body_rows = table.select("tbody tr") or table.select("tr")[1:]
        for row in body_rows:
            cells = row.find_all("td")
            if not cells:
                continue
            text_cells = [td.get_text(" ", strip=True) for td in cells]

            # Identify the card row
            name_idx = col_map.get("name")
            number_idx = col_map.get("number")

            name_text = (
                text_cells[name_idx].lower().strip() if name_idx is not None
                and name_idx < len(text_cells) else ""
            )
            number_text = (
                text_cells[number_idx].strip() if number_idx is not None
                and number_idx < len(text_cells) else ""
            )
            number_clean = re.sub(r"[^\d]", "", number_text)

            card: Optional[Dict[str, Any]] = None
            if number_clean and number_clean in by_number:
                card = by_number[number_clean]
            elif name_text:
                # Exact match first
                if name_text in by_name:
                    card = by_name[name_text]
                else:
                    # Fuzzy: any card whose name appears in the row name
                    for nm_key, c in by_name.items():
                        if nm_key and nm_key in name_text:
                            card = c
                            break

            if card is None or card["id"] in seen_card_ids:
                continue

            market = self._cell_price(text_cells, col_map.get("market"))
            low = self._cell_price(text_cells, col_map.get("low"))
            mid = self._cell_price(text_cells, col_map.get("mid"))

            if market is None and low is None and mid is None:
                continue

            results.append(
                {
                    "card_id": card["id"],
                    "market_price": market,
                    "low_price": low,
                    "mid_price": mid,
                }
            )
            seen_card_ids.add(card["id"])

        self.logger.info(
            "Parsed %d price rows from price guide (%d cards in set)",
            len(results), len(cards_in_set),
        )
        return results

    @staticmethod
    def _map_price_columns(headers: List[str]) -> Dict[str, int]:
        """Map header labels to column indices for a TCGPlayer price-guide table."""
        col: Dict[str, int] = {}
        for i, h in enumerate(headers):
            h_clean = h.strip().lower()
            if h_clean in ("#", "no.", "number", "card #", "card number"):
                col["number"] = i
            elif h_clean in (
                "product", "product name", "card name", "name", "description"
            ):
                col["name"] = i
            elif "market" in h_clean:
                col["market"] = i
            elif h_clean in ("low", "low price", "market low"):
                col["low"] = i
            elif h_clean in ("mid", "mid price", "median"):
                col["mid"] = i
        return col

    @staticmethod
    def _cell_price(cells: List[str], idx: Optional[int]) -> Optional[float]:
        if idx is None or idx >= len(cells):
            return None
        return _parse_price(cells[idx])

    # ------------------------------------------------------------------
    # Whole pipeline
    # ------------------------------------------------------------------

    def scrape_all_sets(
        self,
        date: str,
        limit: Optional[int] = None,
        only_set: Optional[str] = None,
    ) -> Dict[str, int]:
        """Iterate over all sets, scrape, and write to ``justtcg_history``."""
        sets = self._get_all_sets()
        if only_set:
            sets = [s for s in sets if s["set_code"] == only_set]
        if limit is not None:
            sets = sets[:limit]

        self.logger.info(
            "TCGPlayer scrape: %d sets on %s", len(sets), date,
        )

        processed = 0
        errors = 0
        written = 0

        for idx, s in enumerate(sets, start=1):
            set_code = s["set_code"]
            self.logger.info("[%d/%d] TCGPlayer set %s", idx, len(sets), set_code)
            try:
                rows = self.scrape_set_prices(set_code)
                for i, row in enumerate(rows, start=1):
                    try:
                        self._upsert(row["card_id"], date, row.get("market_price"))
                        written += 1
                        processed += 1
                    except Exception as exc:
                        errors += 1
                        self.logger.warning(
                            "FAIL upsert %s: %s", row.get("card_id"), exc,
                        )
                    if i % 10 == 0:
                        self.logger.info(
                            "  set %s: %d/%d cards written", set_code, i, len(rows),
                        )
            except Exception as exc:
                errors += 1
                self.logger.warning("FAIL set %s: %s", set_code, exc)

            # Rotate UA between sets for extra realism
            self._rotate_user_agent()

        self.logger.info(
            "TCGPlayer done: %d processed, %d written, %d errors",
            processed, written, errors,
        )
        return {"processed": processed, "written": written, "errors": errors}

    # BaseScraper compatibility -- default entry point
    def collect(self, date: str) -> Dict[str, int]:
        return self.scrape_all_sets(date)

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_all_sets() -> List[Dict[str, Any]]:
        with get_db() as db:
            rows = db.execute(
                "SELECT set_code, set_name FROM sets ORDER BY set_code"
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _get_set_info(set_code: str) -> Optional[Dict[str, Any]]:
        with get_db() as db:
            row = db.execute(
                "SELECT set_code, set_name FROM sets WHERE set_code = ?", (set_code,)
            ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _get_cards_for_set(set_code: str) -> List[Dict[str, Any]]:
        with get_db() as db:
            rows = db.execute(
                "SELECT * FROM cards WHERE set_code = ? AND sealed_product = 'N' "
                "ORDER BY card_number",
                (set_code,),
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _upsert(card_id: str, date: str, market_price: Optional[float]) -> None:
        if market_price is None:
            return
        with get_db() as db:
            db.execute(
                """
                INSERT INTO justtcg_history (card_id, date, j_raw_price)
                VALUES (?, ?, ?)
                ON CONFLICT(card_id, date) DO UPDATE SET
                    j_raw_price = excluded.j_raw_price
                """,
                (card_id, date, market_price),
            )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _parse_price(text: str) -> Optional[float]:
    """Pull the first dollar amount from a string."""
    if text is None:
        return None
    m = re.search(r"\$\s*([\d,]+\.?\d*)", text)
    if not m:
        return None
    try:
        return round(float(m.group(1).replace(",", "")), 2)
    except ValueError:
        return None


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------


def _setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not root.handlers:
        h = logging.StreamHandler()
        h.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(name)-22s | %(levelname)-5s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root.addHandler(h)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TCGPlayer HTML scraper for Pokemon singles",
    )
    parser.add_argument("--date", default=None, help="Collection date YYYY-MM-DD (UTC today default)")
    parser.add_argument("--limit", type=int, default=None, help="Max number of sets to scrape")
    parser.add_argument("--set", dest="set_code", default=None, help="Scrape only this set code")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    _setup_logging(args.log_level)

    date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with TCGPlayerScraper() as scraper:
        scraper.scrape_all_sets(date, limit=args.limit, only_set=args.set_code)


if __name__ == "__main__":
    main()
