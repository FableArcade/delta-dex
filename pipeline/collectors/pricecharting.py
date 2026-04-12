"""PriceCharting collector -- API with scraping fallback."""

from __future__ import annotations

import re
from datetime import datetime

from bs4 import BeautifulSoup

from config.settings import settings
from db.connection import get_db
from pipeline.collectors.base import BaseCollector, CollectorError


class PriceChartingCollector(BaseCollector):
    name = "pricecharting"
    rate_limit = 1.0  # 1 req/sec for API

    API_BASE = "https://www.pricecharting.com/api/product"
    WEB_BASE = "https://www.pricecharting.com/game/pokemon"

    def __init__(self) -> None:
        super().__init__()
        self._use_api = bool(settings.pricecharting_api_key)
        if not self._use_api:
            self.rate_limit = 0.5  # 1 req / 2 sec for scraping
            self.logger.info("No API key -- falling back to web scraping mode")

    # ------------------------------------------------------------------
    # API path
    # ------------------------------------------------------------------

    def _fetch_api(self, product_id: str) -> dict:
        """Fetch price data via the PriceCharting JSON API."""
        resp = self._request(
            self.API_BASE,
            params={"t": settings.pricecharting_api_key, "id": product_id},
        )
        data = resp.json()
        return self._normalise_api(data)

    @staticmethod
    def _normalise_api(data: dict) -> dict:
        """Map PriceCharting API fields to our schema."""

        def _cents_to_dollars(val) -> float | None:
            """API returns prices in cents."""
            if val is None:
                return None
            try:
                return round(float(val) / 100.0, 2)
            except (TypeError, ValueError):
                return None

        raw = _cents_to_dollars(data.get("loose-price") or data.get("price"))
        psa_7 = _cents_to_dollars(data.get("graded-price"))       # generic graded ~ PSA 7
        psa_8 = _cents_to_dollars(data.get("manual-only-price"))  # manual-only ~ PSA 8
        psa_9 = _cents_to_dollars(data.get("cib-price"))          # CIB maps to PSA 9
        psa_10 = _cents_to_dollars(data.get("new-price"))         # New maps to PSA 10
        sales_vol = data.get("sales-volume")

        return {
            "raw_price": raw,
            "psa_7_price": psa_7,
            "psa_8_price": psa_8,
            "psa_9_price": psa_9,
            "psa_10_price": psa_10,
            "sales_volume": int(sales_vol) if sales_vol is not None else None,
        }

    # ------------------------------------------------------------------
    # Scraping path
    # ------------------------------------------------------------------

    def _fetch_scrape(self, product_id: str) -> dict:
        """Scrape a PriceCharting product page."""
        url = f"{self.WEB_BASE}/{product_id}"
        resp = self._request(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        return self._parse_product_page(soup)

    @staticmethod
    def _parse_price(text: str | None) -> float | None:
        if not text:
            return None
        cleaned = re.sub(r"[^\d.]", "", text.strip())
        try:
            return round(float(cleaned), 2)
        except ValueError:
            return None

    def _parse_product_page(self, soup: BeautifulSoup) -> dict:
        """Extract prices from the product detail page."""
        prices: dict[str, float | None] = {
            "raw_price": None,
            "psa_7_price": None,
            "psa_8_price": None,
            "psa_9_price": None,
            "psa_10_price": None,
            "sales_volume": None,
        }

        # Price table rows: look for common price IDs
        id_map = {
            "used_price": "raw_price",
            "complete_price": "psa_9_price",
            "new_price": "psa_10_price",
            "graded_price": "psa_7_price",
            "manual_price": "psa_8_price",
        }
        for html_id, key in id_map.items():
            el = soup.find("td", id=html_id) or soup.find("span", id=html_id)
            if el:
                prices[key] = self._parse_price(el.get_text())

        # Fallback: look for price table with dt/dd or table rows
        for row in soup.select(".price-table tr, .prices tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                if "ungraded" in label or "raw" in label or "loose" in label:
                    prices["raw_price"] = prices["raw_price"] or self._parse_price(value)
                elif "psa 10" in label or "gem mint" in label or "new" in label:
                    prices["psa_10_price"] = prices["psa_10_price"] or self._parse_price(value)
                elif "psa 9" in label or "mint" in label or "complete" in label:
                    prices["psa_9_price"] = prices["psa_9_price"] or self._parse_price(value)
                elif "psa 8" in label:
                    prices["psa_8_price"] = prices["psa_8_price"] or self._parse_price(value)
                elif "graded" in label:
                    prices["psa_7_price"] = prices["psa_7_price"] or self._parse_price(value)

        # Sales volume
        vol_el = soup.find(string=re.compile(r"sales", re.I))
        if vol_el:
            parent = vol_el.find_parent()
            if parent:
                m = re.search(r"(\d[\d,]*)", parent.get_text())
                if m:
                    prices["sales_volume"] = int(m.group(1).replace(",", ""))

        return prices

    # ------------------------------------------------------------------
    # Derived metrics
    # ------------------------------------------------------------------

    @staticmethod
    def _add_derived(prices: dict) -> dict:
        raw = prices.get("raw_price")
        psa_10 = prices.get("psa_10_price")
        if raw and psa_10 and raw > 0:
            prices["psa_10_vs_raw"] = round(psa_10 - raw, 2)
            prices["psa_10_vs_raw_pct"] = round((psa_10 - raw) / raw * 100, 2)
        else:
            prices["psa_10_vs_raw"] = None
            prices["psa_10_vs_raw_pct"] = None
        return prices

    # ------------------------------------------------------------------
    # collect()
    # ------------------------------------------------------------------

    def collect(self, date: str) -> dict:
        cards = self.get_cards()
        self.logger.info("Starting PriceCharting collection for %s  (%d cards)", date, len(cards))

        processed = 0
        errors = 0

        for card in cards:
            card_id = card["id"]
            try:
                if self._use_api:
                    prices = self._fetch_api(card_id)
                else:
                    prices = self._fetch_scrape(card_id)

                prices = self._add_derived(prices)
                self._upsert(card_id, date, prices)
                processed += 1
                self.logger.debug("OK  %s  raw=%s  psa10=%s", card_id, prices.get("raw_price"), prices.get("psa_10_price"))

            except Exception as exc:
                errors += 1
                self.logger.warning("FAIL %s: %s", card_id, exc)

        self.logger.info("PriceCharting done: %d processed, %d errors", processed, errors)
        return {"processed": processed, "errors": errors}

    # ------------------------------------------------------------------
    # DB
    # ------------------------------------------------------------------

    @staticmethod
    def _upsert(card_id: str, date: str, prices: dict) -> None:
        with get_db() as db:
            db.execute(
                """
                INSERT INTO price_history
                    (card_id, date, raw_price, psa_7_price, psa_8_price,
                     psa_9_price, psa_10_price, psa_10_vs_raw, psa_10_vs_raw_pct,
                     sales_volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(card_id, date) DO UPDATE SET
                    raw_price      = excluded.raw_price,
                    psa_7_price    = excluded.psa_7_price,
                    psa_8_price    = excluded.psa_8_price,
                    psa_9_price    = excluded.psa_9_price,
                    psa_10_price   = excluded.psa_10_price,
                    psa_10_vs_raw  = excluded.psa_10_vs_raw,
                    psa_10_vs_raw_pct = excluded.psa_10_vs_raw_pct,
                    sales_volume   = excluded.sales_volume
                """,
                (
                    card_id, date,
                    prices.get("raw_price"),
                    prices.get("psa_7_price"),
                    prices.get("psa_8_price"),
                    prices.get("psa_9_price"),
                    prices.get("psa_10_price"),
                    prices.get("psa_10_vs_raw"),
                    prices.get("psa_10_vs_raw_pct"),
                    prices.get("sales_volume"),
                ),
            )
