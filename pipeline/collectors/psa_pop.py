"""PSA Pop Report collector -- scrapes population data per set."""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from db.connection import get_db
from pipeline.collectors.base import BaseCollector


class PSAPopCollector(BaseCollector):
    name = "psa_pop"
    rate_limit = 0.2  # 1 request per 5 seconds -- be polite

    PSA_BASE = "https://www.psacard.com"

    # ------------------------------------------------------------------
    # Scraping
    # ------------------------------------------------------------------

    def _fetch_pop_page(self, url: str) -> str:
        """Fetch a PSA Pop Report page."""
        resp = self._request(url, headers={
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        return resp.text

    def _parse_pop_table(self, html: str, cards_in_set: list[dict]) -> list[dict]:
        """Parse the PSA Pop Report HTML table and match rows to our cards.

        Returns a list of dicts with keys:
            card_id, psa_8_base, psa_9_base, psa_10_base, total_base, gem_pct
        """
        soup = BeautifulSoup(html, "html.parser")
        results: list[dict] = []

        # Build lookup by card number and name for matching
        card_lookup: dict[str, dict] = {}
        for c in cards_in_set:
            num = c.get("card_number")
            if num is not None:
                card_lookup[str(num)] = c
            # Also index by product name (lowered) for fuzzy matching
            name_key = (c.get("product_name") or "").lower().strip()
            if name_key:
                card_lookup[f"name:{name_key}"] = c

        # PSA Pop tables: look for the main data table
        table = (
            soup.select_one("table.pop-report-table")
            or soup.select_one("table#pop-report")
            or soup.select_one("table.data-table")
            or soup.select_one("table")
        )
        if not table:
            self.logger.warning("No pop report table found")
            return results

        # Identify column indices from header row
        header_row = table.select_one("thead tr") or table.select_one("tr")
        if not header_row:
            return results

        headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

        col_map = self._map_columns(headers)
        if col_map is None:
            self.logger.warning("Could not map PSA Pop columns from headers: %s", headers)
            return results

        # Parse data rows
        body_rows = table.select("tbody tr") or table.select("tr")[1:]
        for row in body_rows:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) <= max(col_map.values()):
                continue

            # Try to match row to one of our cards
            card = self._match_row_to_card(cells, col_map, card_lookup)
            if card is None:
                continue

            pop = self._extract_populations(cells, col_map)
            if pop:
                pop["card_id"] = card["id"]
                results.append(pop)

        return results

    @staticmethod
    def _map_columns(headers: list[str]) -> dict | None:
        """Map header labels to column indices.

        We need at minimum: a name/number column and grade columns 8, 9, 10.
        PSA Pop tables typically have columns: #, Name, Auth, 1, 1.5, 2, ... 10, Total.
        """
        col: dict[str, int] = {}

        for i, h in enumerate(headers):
            h_stripped = h.strip()
            if h_stripped in ("#", "no.", "number", "card #", "card"):
                col["number"] = i
            elif h_stripped in ("name", "card name", "subject", "description"):
                col["name"] = i
            elif h_stripped == "8":
                col["psa_8"] = i
            elif h_stripped == "9":
                col["psa_9"] = i
            elif h_stripped == "10":
                col["psa_10"] = i
            elif h_stripped in ("total", "pop", "total pop"):
                col["total"] = i

        # Must have at least PSA 10 and one identifier
        if "psa_10" not in col:
            return None
        if "number" not in col and "name" not in col:
            return None

        return col

    def _match_row_to_card(
        self, cells: list[str], col_map: dict, card_lookup: dict
    ) -> dict | None:
        """Try to match a pop report row to one of our tracked cards."""
        # Match by card number
        if "number" in col_map:
            num_text = cells[col_map["number"]].strip()
            num_clean = re.sub(r"[^\d]", "", num_text)
            if num_clean and num_clean in card_lookup:
                return card_lookup[num_clean]

        # Match by name
        if "name" in col_map:
            name_text = cells[col_map["name"]].strip().lower()
            key = f"name:{name_text}"
            if key in card_lookup:
                return card_lookup[key]
            # Fuzzy: check if any card name is contained in the row name
            for k, v in card_lookup.items():
                if k.startswith("name:") and k[5:] and k[5:] in name_text:
                    return v

        return None

    def _extract_populations(self, cells: list[str], col_map: dict) -> dict | None:
        """Pull PSA 8/9/10 and total populations from a row."""

        def parse_int(val: str) -> int | None:
            cleaned = re.sub(r"[^\d]", "", val.strip())
            return int(cleaned) if cleaned else None

        psa_8 = parse_int(cells[col_map["psa_8"]]) if "psa_8" in col_map else None
        psa_9 = parse_int(cells[col_map["psa_9"]]) if "psa_9" in col_map else None
        psa_10 = parse_int(cells[col_map["psa_10"]]) if "psa_10" in col_map else None
        total = parse_int(cells[col_map["total"]]) if "total" in col_map else None

        # Need at least PSA 10
        if psa_10 is None:
            return None

        # Compute gem percentage
        gem_pct = None
        if total and total > 0 and psa_10 is not None:
            gem_pct = round(psa_10 / total * 100, 2)

        return {
            "psa_8_base": psa_8,
            "psa_9_base": psa_9,
            "psa_10_base": psa_10,
            "total_base": total,
            "gem_pct": gem_pct,
        }

    # ------------------------------------------------------------------
    # collect()
    # ------------------------------------------------------------------

    def collect(self, date: str) -> dict:
        sets = self._get_sets_with_pop_urls()
        self.logger.info(
            "Starting PSA Pop collection for %s  (%d sets with pop URLs)", date, len(sets)
        )

        processed = 0
        errors = 0

        for s in sets:
            set_code = s["set_code"]
            pop_url = s["psa_pop_url"]
            self.logger.info("Fetching pop report for set %s: %s", set_code, pop_url)

            try:
                # Get cards for this set
                cards_in_set = self._get_cards_for_set(set_code)
                if not cards_in_set:
                    self.logger.debug("No cards for set %s, skipping", set_code)
                    continue

                html = self._fetch_pop_page(pop_url)
                pop_results = self._parse_pop_table(html, cards_in_set)

                for pop in pop_results:
                    try:
                        self._upsert(pop, date)
                        processed += 1
                    except Exception as exc:
                        errors += 1
                        self.logger.warning(
                            "FAIL upsert %s: %s", pop.get("card_id"), exc
                        )

                self.logger.info(
                    "Set %s: matched %d / %d cards",
                    set_code, len(pop_results), len(cards_in_set),
                )

            except Exception as exc:
                errors += 1
                self.logger.warning("FAIL set %s: %s", set_code, exc)

        self.logger.info("PSA Pop done: %d processed, %d errors", processed, errors)
        return {"processed": processed, "errors": errors}

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_sets_with_pop_urls() -> list[dict]:
        with get_db() as db:
            rows = db.execute(
                "SELECT set_code, psa_pop_url FROM sets WHERE psa_pop_url IS NOT NULL AND psa_pop_url != ''"
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _get_cards_for_set(set_code: str) -> list[dict]:
        with get_db() as db:
            rows = db.execute(
                "SELECT * FROM cards WHERE set_code = ? AND sealed_product = 'N' ORDER BY card_number",
                (set_code,),
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _upsert(pop: dict, date: str) -> None:
        with get_db() as db:
            db.execute(
                """
                INSERT INTO psa_pop_history
                    (card_id, date, psa_8_base, psa_9_base, psa_10_base,
                     total_base, gem_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(card_id, date) DO UPDATE SET
                    psa_8_base  = excluded.psa_8_base,
                    psa_9_base  = excluded.psa_9_base,
                    psa_10_base = excluded.psa_10_base,
                    total_base  = excluded.total_base,
                    gem_pct     = excluded.gem_pct
                """,
                (
                    pop["card_id"], date,
                    pop.get("psa_8_base"),
                    pop.get("psa_9_base"),
                    pop.get("psa_10_base"),
                    pop.get("total_base"),
                    pop.get("gem_pct"),
                ),
            )
