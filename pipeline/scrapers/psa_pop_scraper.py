"""PSA Pop Report HTML scraper.

Scrapes population snapshots from ``psacard.com/pop`` per set and writes
them to ``psa_pop_history``.  PSA rate-limits aggressively, so this runs
at 1 request per 5 seconds and rotates browser User-Agents.

The set URL is taken from ``sets.psa_pop_url`` in the database; if that
column is NULL we skip the set.  The scraper matches PSA rows to our
cards by card number with a name fuzzy-match fallback.
"""

from __future__ import annotations

import argparse
import logging
import random
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from bs4 import BeautifulSoup

from db.connection import get_db
from pipeline.scrapers.base_scraper import BaseScraper


# ----------------------------------------------------------------------
# User-Agent pool (Chrome 146 family)
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
        "Referer": "https://www.psacard.com/",
        "DNT": "1",
    }


# ----------------------------------------------------------------------
# Scraper
# ----------------------------------------------------------------------


class PSAPopScraper(BaseScraper):
    """Scrape PSA Pop Report population counts for tracked sets."""

    name = "psa_pop_scraper"
    rate_limit = 0.2  # 1 request per 5 seconds -- be polite

    PSA_BASE = "https://www.psacard.com"

    def __init__(self) -> None:
        super().__init__()
        self._user_agent = random.choice(_USER_AGENTS)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _rotate_user_agent(self) -> None:
        self._user_agent = random.choice(_USER_AGENTS)

    def _browser_headers(self) -> Dict[str, str]:
        return _default_headers(self._user_agent)

    def _fetch_pop_page(self, url: str) -> Optional[str]:
        """Fetch a PSA Pop Report page, returning HTML text or None on failure."""
        try:
            resp = self._request(url, headers=self._browser_headers())
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status == 404:
                self.logger.warning("PSA Pop page not found (404): %s", url)
                return None
            if status in (403, 429, 503):
                self.logger.warning(
                    "PSA blocked (%d) for %s -- rate-limited or challenged", status, url,
                )
                self._rotate_user_agent()
                return None
            self.logger.warning("PSA HTTP %d for %s", status, url)
            return None
        except httpx.RequestError as exc:
            self.logger.warning("PSA request failed for %s: %s", url, exc)
            return None

        text = resp.text or ""
        low = text.lower()
        if "just a moment" in low or "cf-browser-verification" in low:
            self.logger.warning("PSA returned a Cloudflare challenge for %s", url)
            self._rotate_user_agent()
            return None

        return text

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_pop_table(
        self, html: str, cards_in_set: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Parse a PSA Pop Report HTML table and produce per-card rows.

        Each returned dict has:
            card_id, psa_8_base, psa_9_base, psa_10_base, total_base, gem_pct
        """
        soup = BeautifulSoup(html, "html.parser")
        results: List[Dict[str, Any]] = []

        # Lookups
        by_number: Dict[str, Dict[str, Any]] = {}
        by_name: Dict[str, Dict[str, Any]] = {}
        for c in cards_in_set:
            num = c.get("card_number")
            if num is not None:
                by_number[str(num).strip()] = c
            nm = (c.get("product_name") or "").lower().strip()
            if nm:
                by_name[nm] = c

        # PSA Pop Report main table
        table = (
            soup.select_one("table.pop-report-table")
            or soup.select_one("table#pop-report")
            or soup.select_one("table.data-table")
            or soup.select_one("table")
        )
        if not table:
            self.logger.warning("No PSA pop-report table found")
            return results

        header_row = table.select_one("thead tr") or table.select_one("tr")
        if not header_row:
            return results

        headers = [
            th.get_text(" ", strip=True).lower()
            for th in header_row.find_all(["th", "td"])
        ]
        col_map = self._map_columns(headers)
        if col_map is None:
            self.logger.warning(
                "Could not map PSA Pop columns from headers: %s", headers,
            )
            return results

        body_rows = table.select("tbody tr") or table.select("tr")[1:]

        seen_ids: set = set()
        for row in body_rows:
            cells = row.find_all("td")
            if not cells:
                continue
            text_cells = [td.get_text(" ", strip=True) for td in cells]
            if len(text_cells) <= max(col_map.values()):
                continue

            card = self._match_row_to_card(text_cells, col_map, by_number, by_name)
            if card is None or card["id"] in seen_ids:
                continue

            pop = self._extract_populations(text_cells, col_map)
            if pop is None:
                continue

            pop["card_id"] = card["id"]
            results.append(pop)
            seen_ids.add(card["id"])

        return results

    @staticmethod
    def _map_columns(headers: List[str]) -> Optional[Dict[str, int]]:
        """Map header labels to column indices.

        Required: PSA 10 column and an identifier column (number or name).
        """
        col: Dict[str, int] = {}
        for i, h in enumerate(headers):
            h_clean = h.strip().lower()
            if h_clean in ("#", "no.", "number", "card #", "card"):
                col["number"] = i
            elif h_clean in (
                "name", "card name", "subject", "description", "card / description",
                "card/description",
            ):
                col["name"] = i
            elif h_clean == "8":
                col["psa_8"] = i
            elif h_clean == "9":
                col["psa_9"] = i
            elif h_clean == "10":
                col["psa_10"] = i
            elif h_clean in ("total", "pop", "total pop", "total graded"):
                col["total"] = i

        if "psa_10" not in col:
            return None
        if "number" not in col and "name" not in col:
            return None
        return col

    def _match_row_to_card(
        self,
        cells: List[str],
        col_map: Dict[str, int],
        by_number: Dict[str, Dict[str, Any]],
        by_name: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Match a pop-report row to one of our tracked cards."""
        # 1. Card number
        if "number" in col_map and col_map["number"] < len(cells):
            num_text = cells[col_map["number"]]
            num_clean = re.sub(r"[^\d]", "", num_text or "")
            if num_clean and num_clean in by_number:
                return by_number[num_clean]

        # 2. Exact-ish name match
        if "name" in col_map and col_map["name"] < len(cells):
            name_text = (cells[col_map["name"]] or "").lower().strip()
            if not name_text:
                return None
            if name_text in by_name:
                return by_name[name_text]

            # 3. Fuzzy contains
            for nm_key, card in by_name.items():
                if nm_key and nm_key in name_text:
                    return card

            # 4. Token match -- require the first significant word
            name_tokens = [w for w in name_text.split() if len(w) > 2]
            for nm_key, card in by_name.items():
                card_tokens = [w for w in nm_key.split() if len(w) > 2]
                if not card_tokens or not name_tokens:
                    continue
                if card_tokens[0] == name_tokens[0]:
                    return card

        return None

    @staticmethod
    def _extract_populations(
        cells: List[str], col_map: Dict[str, int]
    ) -> Optional[Dict[str, Any]]:
        """Extract PSA 8/9/10 counts and total graded."""

        def parse_int(val: str) -> Optional[int]:
            if val is None:
                return None
            cleaned = re.sub(r"[^\d]", "", val.strip())
            return int(cleaned) if cleaned else None

        def cell_or_none(key: str) -> Optional[str]:
            idx = col_map.get(key)
            if idx is None or idx >= len(cells):
                return None
            return cells[idx]

        psa_8 = parse_int(cell_or_none("psa_8") or "")
        psa_9 = parse_int(cell_or_none("psa_9") or "")
        psa_10 = parse_int(cell_or_none("psa_10") or "")
        total = parse_int(cell_or_none("total") or "")

        if psa_10 is None:
            return None

        gem_pct: Optional[float] = None
        if total and total > 0:
            gem_pct = round(psa_10 / total * 100, 2)

        return {
            "psa_8_base": psa_8,
            "psa_9_base": psa_9,
            "psa_10_base": psa_10,
            "total_base": total,
            "gem_pct": gem_pct,
        }

    # ------------------------------------------------------------------
    # Set-level scrape
    # ------------------------------------------------------------------

    def scrape_set_pop(self, set_code: str) -> List[Dict[str, Any]]:
        """Fetch and parse one set's PSA pop report. Returns list of dicts."""
        set_info = self._get_set_info(set_code)
        if not set_info:
            self.logger.warning("Unknown set %s", set_code)
            return []

        pop_url = set_info.get("psa_pop_url")
        if not pop_url:
            self.logger.debug("Set %s has no psa_pop_url, skipping", set_code)
            return []

        self.logger.info("Fetching PSA pop report for %s: %s", set_code, pop_url)
        html = self._fetch_pop_page(pop_url)
        if html is None:
            return []

        cards_in_set = self._get_cards_for_set(set_code)
        if not cards_in_set:
            self.logger.debug("No cards for set %s in DB", set_code)
            return []

        rows = self._parse_pop_table(html, cards_in_set)
        self.logger.info(
            "Set %s: matched %d / %d cards from pop report",
            set_code, len(rows), len(cards_in_set),
        )
        return rows

    # ------------------------------------------------------------------
    # All sets + write to DB
    # ------------------------------------------------------------------

    def scrape_all_sets(
        self,
        date: Optional[str] = None,
        limit: Optional[int] = None,
        only_set: Optional[str] = None,
    ) -> Dict[str, int]:
        """Iterate every set with a psa_pop_url and write populations to DB."""
        date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sets = self._get_sets_with_pop_urls()
        if only_set:
            sets = [s for s in sets if s["set_code"] == only_set]
        if limit is not None:
            sets = sets[:limit]

        self.logger.info(
            "PSA Pop scrape: %d sets on %s", len(sets), date,
        )

        processed = 0
        written = 0
        errors = 0

        for idx, s in enumerate(sets, start=1):
            set_code = s["set_code"]
            self.logger.info("[%d/%d] PSA Pop set %s", idx, len(sets), set_code)
            try:
                rows = self.scrape_set_pop(set_code)
                for i, pop in enumerate(rows, start=1):
                    try:
                        self._upsert(pop, date)
                        written += 1
                        processed += 1
                    except Exception as exc:
                        errors += 1
                        self.logger.warning(
                            "FAIL upsert %s: %s", pop.get("card_id"), exc,
                        )
                    if i % 10 == 0:
                        self.logger.info(
                            "  set %s: %d/%d rows written", set_code, i, len(rows),
                        )
            except Exception as exc:
                errors += 1
                self.logger.warning("FAIL set %s: %s", set_code, exc)

            self._rotate_user_agent()

        self.logger.info(
            "PSA Pop done: %d processed, %d written, %d errors",
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
    def _get_sets_with_pop_urls() -> List[Dict[str, Any]]:
        with get_db() as db:
            rows = db.execute(
                "SELECT set_code, set_name, psa_pop_url FROM sets "
                "WHERE psa_pop_url IS NOT NULL AND psa_pop_url != '' "
                "ORDER BY set_code"
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _get_set_info(set_code: str) -> Optional[Dict[str, Any]]:
        with get_db() as db:
            row = db.execute(
                "SELECT set_code, set_name, psa_pop_url FROM sets WHERE set_code = ?",
                (set_code,),
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
    def _upsert(pop: Dict[str, Any], date: str) -> None:
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
        description="PSA Pop Report HTML scraper (weekly cadence)",
    )
    parser.add_argument("--date", default=None, help="Snapshot date YYYY-MM-DD (UTC today default)")
    parser.add_argument("--limit", type=int, default=None, help="Max number of sets to scrape")
    parser.add_argument("--set", dest="set_code", default=None, help="Scrape only this set code")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    _setup_logging(args.log_level)

    date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with PSAPopScraper() as scraper:
        scraper.scrape_all_sets(date, limit=args.limit, only_set=args.set_code)


if __name__ == "__main__":
    main()
