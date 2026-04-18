"""eBay Browse API collector with OAuth2 auth and listing classification."""

from __future__ import annotations

import base64
import re
from datetime import datetime, timedelta, timezone

from config.settings import settings
from db.connection import get_db
from pipeline.collectors.base import BaseCollector, CollectorError


# ------------------------------------------------------------------
# Grading classification helpers
# ------------------------------------------------------------------

_GRADE_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("psa_10",   re.compile(r"\bpsa\s*10\b", re.I)),
    ("psa_9",    re.compile(r"\bpsa\s*9\b", re.I)),
    ("other_10", re.compile(r"\b(?:cgc|bgs|ace)\s*(?:10|pristine|gem)\b", re.I)),
    ("graded",   re.compile(r"\b(?:psa|cgc|bgs|ace|sgc)\s*\d", re.I)),
]


def classify_listing(title: str) -> str:
    """Return the most specific grade bucket for a listing title.

    Returns one of: ``psa_10``, ``psa_9``, ``other_10``, ``graded``, ``raw``.
    """
    for label, pattern in _GRADE_PATTERNS:
        if pattern.search(title):
            return label
    return "raw"


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return round(float(val), 2)
    except (TypeError, ValueError):
        return None


class EBayCollector(BaseCollector):
    name = "ebay"
    rate_limit = 5.0  # 5 req/sec burst; daily budget managed separately
    max_retries = 3

    # Endpoints auto-switch based on App ID — sandbox keys contain -SBX-,
    # production keys contain -PRD-. Sandbox returns test data only; real
    # listings require Production keys (which require marketplace-deletion
    # compliance or exemption).
    BROWSE_API_PROD = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    BROWSE_API_SANDBOX = "https://api.sandbox.ebay.com/buy/browse/v1/item_summary/search"
    TOKEN_URL_PROD = "https://api.ebay.com/identity/v1/oauth2/token"
    TOKEN_URL_SANDBOX = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
    SCOPE = "https://api.ebay.com/oauth/api_scope"

    DAILY_BUDGET = 6500  # eBay Browse API daily call budget — raised from 5000 to cover ~1,800 new promo cards (Apr 2026)

    def __init__(self) -> None:
        super().__init__()
        self._access_token: str | None = None
        self._token_expiry: datetime | None = None
        self._calls_today: int = 0
        # Auto-detect sandbox vs production from App ID format.
        app_id = settings.ebay_app_id or ""
        self._is_sandbox = "-SBX-" in app_id
        if self._is_sandbox:
            self.BROWSE_API = self.BROWSE_API_SANDBOX
            self.TOKEN_URL = self.TOKEN_URL_SANDBOX
            self.logger.warning(
                "eBay collector using SANDBOX endpoints — returns test data only, "
                "not real Pokemon card listings. Swap to Production keys for live data."
            )
        else:
            self.BROWSE_API = self.BROWSE_API_PROD
            self.TOKEN_URL = self.TOKEN_URL_PROD

    # ------------------------------------------------------------------
    # OAuth2 client-credentials
    # ------------------------------------------------------------------

    def _get_access_token(self) -> str:
        """Return a valid OAuth2 access token, refreshing if expired."""
        now = datetime.now(timezone.utc)
        if self._access_token and self._token_expiry and now < self._token_expiry:
            return self._access_token

        if not settings.ebay_app_id or not settings.ebay_cert_id:
            raise CollectorError("EBAY_APP_ID and EBAY_CERT_ID are required")

        credentials = f"{settings.ebay_app_id}:{settings.ebay_cert_id}"
        b64_creds = base64.b64encode(credentials.encode()).decode()

        self._throttle()
        resp = self.client.post(
            self.TOKEN_URL,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {b64_creds}",
            },
            data={
                "grant_type": "client_credentials",
                "scope": self.SCOPE,
            },
        )
        resp.raise_for_status()
        body = resp.json()

        self._access_token = body["access_token"]
        expires_in = int(body.get("expires_in", 7200))
        self._token_expiry = now + timedelta(seconds=expires_in - 60)
        self.logger.info("eBay OAuth token refreshed, expires in %ds", expires_in)
        return self._access_token

    def _auth_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _search_card(self, card: dict, sold: bool = False) -> list[dict]:
        """Search eBay for a card. Returns raw item summaries.

        If ``sold`` is True, searches completed/sold items (requires
        filter ``buyingOptions:{FIXED_PRICE|AUCTION},conditions:{USED|NEW}``).
        """
        if self._calls_today >= self.DAILY_BUDGET:
            self.logger.warning("Daily eBay API budget exhausted (%d calls)", self._calls_today)
            return []

        q_phrase = card.get("ebay_q_phrase") or card.get("product_name", "")
        q_num = card.get("ebay_q_num") or ""
        query = f"{q_phrase} {q_num}".strip()
        if not query:
            return []

        params: dict = {
            "q": query,
            "limit": "200",
        }

        category = card.get("ebay_category_id")
        if category:
            params["category_ids"] = category

        filters = ["deliveryCountry:US"]
        if sold:
            filters.append("buyingOptions:{FIXED_PRICE|AUCTION}")
            # Sold items in last 24h
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
                "%Y-%m-%dT00:00:00Z"
            )
            filters.append(f"itemEndDate:[{yesterday}]")

        params["filter"] = ",".join(filters)

        # Single page only (200 items) — enough to estimate count/density
        # for liquidity features. We don't need exhaustive listings; we need
        # a representative slice per card. The first 200 items are sorted
        # by relevance + recency, which is what matters for "is this card
        # liquid right now." Prior implementation paginated through ALL
        # results (up to ~80 pages for popular cards like Charizard with
        # 16k listings), burning the daily budget 6-12x over its limit.
        resp = self._request(self.BROWSE_API, params=params, headers=self._auth_headers())
        self._calls_today += 1
        body = resp.json()
        items = body.get("itemSummaries", [])
        # Also record the TOTAL count reported by eBay — that's the real
        # liquidity signal even if we only have 200 items in hand.
        total_from_api = body.get("total", len(items))
        if total_from_api and len(items) > 0:
            # Stash the true total on the first item so aggregator can see it
            items[0]["_total_from_api"] = total_from_api

        return items

    # ------------------------------------------------------------------
    # Classification & aggregation
    # ------------------------------------------------------------------

    def _aggregate_listings(self, items: list[dict], use_api_total: bool = True) -> dict:
        """Classify listings and compute counts and average prices by bucket.

        When ``use_api_total`` is True (active listings), uses eBay's reported
        total count instead of len(items). When False (ended/sold listings),
        uses len(items) as the count since we only want the 24h snapshot.
        """
        buckets: dict[str, list[dict]] = {
            "raw": [], "graded": [], "psa_10": [], "psa_9": [], "other_10": [],
        }

        for item in items:
            title = item.get("title", "")
            bucket = classify_listing(title)
            buckets[bucket].append(item)
            # graded is a super-bucket
            if bucket in ("psa_10", "psa_9", "other_10"):
                buckets["graded"].append(item)

        def avg_price(items_list: list[dict]) -> float | None:
            prices = []
            for it in items_list:
                p = it.get("price", {})
                v = _safe_float(p.get("value"))
                if v is not None:
                    prices.append(v)
            return round(sum(prices) / len(prices), 2) if prices else None

        total = len(items)
        if use_api_total and items and "_total_from_api" in items[0]:
            total = int(items[0]["_total_from_api"])
        return {
            "total": total,
            "raw_count": len(buckets["raw"]),
            "graded_count": len(buckets["graded"]),
            "psa_10_count": len(buckets["psa_10"]),
            "psa_9_count": len(buckets["psa_9"]),
            "other_10_count": len(buckets["other_10"]),
            "avg_raw_price": avg_price(buckets["raw"]),
            "avg_psa_10_price": avg_price(buckets["psa_10"]),
            "avg_psa_9_price": avg_price(buckets["psa_9"]),
            "avg_other_10_price": avg_price(buckets["other_10"]),
        }

    # ------------------------------------------------------------------
    # Snapshot logic
    # ------------------------------------------------------------------

    def _get_yesterday_snapshot(self, card_id: str, date: str) -> dict | None:
        yesterday = (
            datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        with get_db() as db:
            row = db.execute(
                "SELECT * FROM ebay_history WHERE card_id = ? AND date = ?",
                (card_id, yesterday),
            ).fetchone()
        return dict(row) if row else None

    def _build_snapshot(
        self,
        card_id: str,
        date: str,
        active_agg: dict,
        ended_agg: dict,
    ) -> dict:
        yesterday = self._get_yesterday_snapshot(card_id, date)
        active_to = float(active_agg["total"])
        active_from = float(yesterday["active_to"]) if yesterday else active_to

        ended_total = float(ended_agg["total"])
        new_total = active_to - active_from + ended_total
        if new_total < 0:
            new_total = 0.0

        ended_rate = round(ended_total / active_from, 4) if active_from > 0 else None

        return {
            "card_id": card_id,
            "date": date,
            "from_date": (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"),
            "active_from": active_from,
            "active_to": active_to,
            "ended": ended_total,
            "new": new_total,
            "ended_rate": ended_rate,
            "ended_raw": float(ended_agg.get("raw_count", 0)),
            "new_raw": float(active_agg.get("raw_count", 0)),
            "ended_graded": float(ended_agg.get("graded_count", 0)),
            "new_graded": float(active_agg.get("graded_count", 0)),
            "ended_psa_10": float(ended_agg.get("psa_10_count", 0)),
            "new_psa_10": float(active_agg.get("psa_10_count", 0)),
            "ended_psa_9": float(ended_agg.get("psa_9_count", 0)),
            "new_psa_9": float(active_agg.get("psa_9_count", 0)),
            "ended_other_10": float(ended_agg.get("other_10_count", 0)),
            "new_other_10": float(active_agg.get("other_10_count", 0)),
            "ended_avg_raw_price": ended_agg.get("avg_raw_price"),
            "ended_avg_psa_10_price": ended_agg.get("avg_psa_10_price"),
            "ended_avg_psa_9_price": ended_agg.get("avg_psa_9_price"),
            "ended_avg_other_10_price": ended_agg.get("avg_other_10_price"),
        }

    # ------------------------------------------------------------------
    # Card prioritisation (high-value first)
    # ------------------------------------------------------------------

    def _prioritise_cards(self, cards: list[dict]) -> list[dict]:
        """Sort cards so high-value ones get API budget first.

        Uses latest raw_price from price_history; cards without price data
        go last.
        """
        price_map: dict[str, float] = {}
        with get_db() as db:
            rows = db.execute(
                """
                SELECT card_id, raw_price
                FROM price_history
                WHERE date = (SELECT MAX(date) FROM price_history)
                  AND raw_price IS NOT NULL
                """
            ).fetchall()
            for r in rows:
                price_map[r["card_id"]] = r["raw_price"]

        def sort_key(c: dict) -> float:
            return -(price_map.get(c["id"], 0.0))

        return sorted(cards, key=sort_key)

    # ------------------------------------------------------------------
    # collect()
    # ------------------------------------------------------------------

    def collect(self, date: str) -> dict:
        cards = self._prioritise_cards(self.get_cards())
        self.logger.info(
            "Starting eBay collection for %s  (%d cards, budget=%d)",
            date, len(cards), self.DAILY_BUDGET,
        )
        self._calls_today = 0
        processed = 0
        errors = 0

        for card in cards:
            if self._calls_today >= self.DAILY_BUDGET:
                self.logger.warning("Budget exhausted after %d cards", processed)
                break

            card_id = card["id"]
            try:
                # Active listings — use API total for real supply count
                active_items = self._search_card(card, sold=False)
                if not active_items:
                    # Empty response = rate limited or no data. SKIP, don't
                    # write zeros that would create a fake supply dip.
                    errors += 1
                    self.logger.debug("SKIP %s: empty active response", card_id)
                    continue
                active_agg = self._aggregate_listings(active_items, use_api_total=True)

                # Recently ended/sold — use len(items) for 24h snapshot
                ended_items = self._search_card(card, sold=True)
                ended_agg = self._aggregate_listings(ended_items, use_api_total=False)

                snapshot = self._build_snapshot(card_id, date, active_agg, ended_agg)
                self._upsert(snapshot)
                processed += 1
                self.logger.debug(
                    "OK  %s  active=%d ended=%d",
                    card_id, active_agg["total"], ended_agg["total"],
                )

            except Exception as exc:
                errors += 1
                self.logger.warning("FAIL %s: %s", card_id, exc)

        self.logger.info(
            "eBay done: %d processed, %d errors, %d API calls used",
            processed, errors, self._calls_today,
        )
        return {"processed": processed, "errors": errors}

    # ------------------------------------------------------------------
    # DB
    # ------------------------------------------------------------------

    @staticmethod
    def _upsert(snap: dict) -> None:
        cols = [
            "card_id", "date", "from_date", "active_from", "active_to",
            "ended", "new", "ended_rate",
            "ended_raw", "new_raw", "ended_graded", "new_graded",
            "ended_psa_10", "new_psa_10", "ended_psa_9", "new_psa_9",
            "ended_other_10", "new_other_10",
            "ended_avg_raw_price", "ended_avg_psa_10_price",
            "ended_avg_psa_9_price", "ended_avg_other_10_price",
        ]
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        updates = ", ".join(
            f"{c} = excluded.{c}" for c in cols if c not in ("card_id", "date")
        )

        with get_db() as db:
            db.execute(
                f"""
                INSERT INTO ebay_history ({col_names})
                VALUES ({placeholders})
                ON CONFLICT(card_id, date) DO UPDATE SET {updates}
                """,
                tuple(snap.get(c) for c in cols),
            )
