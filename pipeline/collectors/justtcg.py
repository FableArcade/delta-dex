"""JustTCG collector -- scrapes NM pricing from justtcg.com."""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from config.settings import settings
from db.connection import get_db
from pipeline.collectors.base import BaseCollector


class JustTCGCollector(BaseCollector):
    name = "justtcg"
    rate_limit = 0.5  # 1 request per 2 seconds

    BASE_URL = "https://justtcg.com"
    SEARCH_URL = f"{BASE_URL}/search"

    # ------------------------------------------------------------------
    # Scraping
    # ------------------------------------------------------------------

    def _search_card(self, card: dict) -> float | None:
        """Search JustTCG for a card and return NM price in dollars."""
        search_text = card.get("search_text") or card.get("product_name", "")
        if not search_text:
            return None

        resp = self._request(self.SEARCH_URL, params={"q": search_text})
        soup = BeautifulSoup(resp.text, "html.parser")
        return self._parse_search_results(soup, card)

    def _parse_search_results(self, soup: BeautifulSoup, card: dict) -> float | None:
        """Extract NM price from the search results page.

        JustTCG renders product cards with price information.  We look for
        the first matching result and pull the NM (Near Mint) price.
        """
        # Strategy 1: Product cards with explicit NM price labels
        for product in soup.select(".product-card, .product, .card, [data-product]"):
            # Check if this result matches our card
            title_el = product.select_one(
                ".product-title, .product-name, .card-title, h3, h4, a"
            )
            if not title_el:
                continue

            title_text = title_el.get_text(strip=True).lower()
            card_name = (card.get("product_name") or "").lower()

            # Loose match: at least the first significant word of card name
            name_words = [w for w in card_name.split() if len(w) > 2]
            if name_words and not any(w in title_text for w in name_words[:3]):
                continue

            # Look for NM price
            price = self._extract_nm_price(product)
            if price is not None:
                return price

        # Strategy 2: Table-style results
        for row in soup.select("table tr, .price-row, .listing-row"):
            text = row.get_text(strip=True).lower()
            if "nm" in text or "near mint" in text:
                price = self._extract_price_from_text(row.get_text())
                if price is not None:
                    return price

        # Strategy 3: First price on page as fallback (only if few results)
        all_prices = soup.select(".price, .product-price, [data-price]")
        if len(all_prices) == 1:
            return self._extract_price_from_text(all_prices[0].get_text())

        return None

    def _extract_nm_price(self, container) -> float | None:
        """Look for NM-condition price within a container element."""
        # Check for condition-specific elements
        for el in container.select(".condition, .variant, .price-row, td, span, div"):
            text = el.get_text(strip=True).lower()
            if "nm" in text or "near mint" in text:
                price = self._extract_price_from_text(el.get_text())
                if price is not None:
                    return price

        # Check data attributes
        for el in container.select("[data-condition]"):
            cond = (el.get("data-condition") or "").lower()
            if "nm" in cond or "near mint" in cond:
                price_el = el.select_one(".price, [data-price]")
                if price_el:
                    return self._extract_price_from_text(price_el.get_text())
                # Try data-price attribute
                dp = el.get("data-price")
                if dp:
                    return self._parse_dollar(dp)

        # Fallback: first price in the container
        price_el = container.select_one(".price, .product-price, [data-price]")
        if price_el:
            dp = price_el.get("data-price")
            if dp:
                return self._parse_dollar(dp)
            return self._extract_price_from_text(price_el.get_text())

        return None

    @staticmethod
    def _extract_price_from_text(text: str) -> float | None:
        """Pull the first dollar amount from a text string."""
        m = re.search(r"\$\s*([\d,]+\.?\d*)", text)
        if m:
            try:
                return round(float(m.group(1).replace(",", "")), 2)
            except ValueError:
                pass
        return None

    @staticmethod
    def _parse_dollar(val: str) -> float | None:
        cleaned = re.sub(r"[^\d.]", "", val.strip())
        try:
            return round(float(cleaned), 2)
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # collect()
    # ------------------------------------------------------------------

    def collect(self, date: str) -> dict:
        cards = self.get_cards()
        self.logger.info("Starting JustTCG collection for %s  (%d cards)", date, len(cards))

        processed = 0
        errors = 0

        for card in cards:
            card_id = card["id"]
            try:
                price = self._search_card(card)

                if price is not None:
                    self._upsert(card_id, date, price)
                    processed += 1
                    self.logger.debug("OK  %s  nm_price=%.2f", card_id, price)
                else:
                    self.logger.debug("SKIP %s  (no NM price found)", card_id)
                    processed += 1  # Not an error, just no data

            except Exception as exc:
                errors += 1
                self.logger.warning("FAIL %s: %s", card_id, exc)

        self.logger.info("JustTCG done: %d processed, %d errors", processed, errors)
        return {"processed": processed, "errors": errors}

    # ------------------------------------------------------------------
    # DB
    # ------------------------------------------------------------------

    @staticmethod
    def _upsert(card_id: str, date: str, nm_price: float) -> None:
        with get_db() as db:
            db.execute(
                """
                INSERT INTO justtcg_history (card_id, date, j_raw_price)
                VALUES (?, ?, ?)
                ON CONFLICT(card_id, date) DO UPDATE SET
                    j_raw_price = excluded.j_raw_price
                """,
                (card_id, date, nm_price),
            )
