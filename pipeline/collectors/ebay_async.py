"""Async parallel eBay Browse API collector.

Drop-in replacement for EBayCollector.collect() that runs card searches
concurrently (default 5 in-flight). Same OAuth2 auth, same DB schema,
same classification logic — just 5x faster.

Usage:
    from pipeline.collectors.ebay_async import AsyncEBayCollector
    coll = AsyncEBayCollector()
    result = coll.collect_sync("2026-04-16")  # blocking wrapper
"""

from __future__ import annotations

import asyncio
import base64
import logging
from datetime import datetime, timedelta, timezone

import httpx

from config.settings import settings
from db.connection import get_db
from pipeline.collectors.ebay import (
    EBayCollector,
    classify_listing,
    _safe_float,
)

logger = logging.getLogger("collector.ebay_async")


class AsyncEBayCollector(EBayCollector):
    """EBayCollector with async parallel collection."""

    CONCURRENCY = 5          # max in-flight card pairs (active + ended)
    REQ_INTERVAL = 0.22      # seconds between requests (~4.5 req/sec effective)

    def __init__(self) -> None:
        super().__init__()
        self._async_client: httpx.AsyncClient | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._req_lock = asyncio.Lock() if asyncio else None
        self._last_req_time: float = 0

    # ------------------------------------------------------------------
    # Async HTTP
    # ------------------------------------------------------------------

    async def _get_async_client(self) -> httpx.AsyncClient:
        if self._async_client is None or self._async_client.is_closed:
            self._async_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True,
                headers={"User-Agent": "PokemonAnalytics/1.0"},
            )
        return self._async_client

    async def _async_throttle(self):
        """Rate-limit across all concurrent tasks."""
        async with self._req_lock:
            now = asyncio.get_event_loop().time()
            wait = self.REQ_INTERVAL - (now - self._last_req_time)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_req_time = asyncio.get_event_loop().time()

    async def _async_get_token(self) -> str:
        """Get/refresh OAuth2 token (async version)."""
        now = datetime.now(timezone.utc)
        if self._access_token and self._token_expiry and now < self._token_expiry:
            return self._access_token

        if not settings.ebay_app_id or not settings.ebay_cert_id:
            raise RuntimeError("EBAY_APP_ID and EBAY_CERT_ID are required")

        credentials = f"{settings.ebay_app_id}:{settings.ebay_cert_id}"
        b64_creds = base64.b64encode(credentials.encode()).decode()

        client = await self._get_async_client()
        await self._async_throttle()
        resp = await client.post(
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
        logger.info("OAuth token refreshed, expires in %ds", expires_in)
        return self._access_token

    async def _async_search(self, card: dict, sold: bool = False) -> list[dict]:
        """Async version of _search_card."""
        if self._calls_today >= self.DAILY_BUDGET:
            return []

        q_phrase = card.get("ebay_q_phrase") or card.get("product_name", "")
        q_num = card.get("ebay_q_num") or ""
        query = f"{q_phrase} {q_num}".strip()
        if not query:
            return []

        params: dict = {"q": query, "limit": "200"}
        category = card.get("ebay_category_id")
        if category:
            params["category_ids"] = category

        filters = ["deliveryCountry:US"]
        if sold:
            filters.append("buyingOptions:{FIXED_PRICE|AUCTION}")
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
                "%Y-%m-%dT00:00:00Z"
            )
            filters.append(f"itemEndDate:[{yesterday}]")
        params["filter"] = ",".join(filters)

        token = await self._async_get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
            "Content-Type": "application/json",
        }

        client = await self._get_async_client()

        # Retry loop for 429s
        for attempt in range(self.max_retries):
            await self._async_throttle()
            self._calls_today += 1
            try:
                resp = await client.get(self.BROWSE_API, params=params, headers=headers)
                resp.raise_for_status()
                body = resp.json()
                items = body.get("itemSummaries", [])
                total_from_api = body.get("total", len(items))
                if total_from_api and len(items) > 0:
                    items[0]["_total_from_api"] = total_from_api
                return items
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning("429 rate limited, waiting %ds (attempt %d/%d)",
                                   wait, attempt + 1, self.max_retries)
                    await asyncio.sleep(wait)
                    continue
                raise

        return []  # all retries exhausted

    # ------------------------------------------------------------------
    # Per-card collection task
    # ------------------------------------------------------------------

    async def _collect_card(self, card: dict, date: str) -> bool:
        """Collect a single card (active + ended). Returns True on success."""
        async with self._semaphore:
            card_id = card["id"]
            try:
                active_items = await self._async_search(card, sold=False)
                if not active_items:
                    # Empty = rate limited. Skip, don't write zeros.
                    logger.debug("SKIP %s: empty active response", card_id)
                    return False
                active_agg = self._aggregate_listings(active_items, use_api_total=True)

                ended_items = await self._async_search(card, sold=True)
                ended_agg = self._aggregate_listings(ended_items, use_api_total=False)

                snapshot = self._build_snapshot(card_id, date, active_agg, ended_agg)
                self._upsert(snapshot)
                logger.debug("OK  %s  active=%d ended=%d",
                             card_id, active_agg["total"], ended_agg["total"])
                return True
            except Exception as exc:
                logger.warning("FAIL %s: %s", card_id, exc)
                return False

    # ------------------------------------------------------------------
    # Main async collect
    # ------------------------------------------------------------------

    async def collect_async(self, date: str) -> dict:
        """Collect all cards concurrently."""
        self._semaphore = asyncio.Semaphore(self.CONCURRENCY)
        self._req_lock = asyncio.Lock()
        self._last_req_time = 0

        cards = self._prioritise_cards(self.get_cards())
        logger.info(
            "Starting ASYNC eBay collection for %s  (%d cards, budget=%d, concurrency=%d)",
            date, len(cards), self.DAILY_BUDGET, self.CONCURRENCY,
        )
        self._calls_today = 0

        # Filter to budget
        max_cards = self.DAILY_BUDGET // 2  # 2 calls per card
        if len(cards) > max_cards:
            logger.info("Trimming to %d cards (budget limit)", max_cards)
            cards = cards[:max_cards]

        tasks = [self._collect_card(card, date) for card in cards]
        results = await asyncio.gather(*tasks)

        processed = sum(1 for r in results if r)
        errors = sum(1 for r in results if not r)

        logger.info(
            "eBay ASYNC done: %d processed, %d errors, %d API calls used",
            processed, errors, self._calls_today,
        )

        if self._async_client and not self._async_client.is_closed:
            await self._async_client.aclose()

        return {"processed": processed, "errors": errors}

    def collect_sync(self, date: str) -> dict:
        """Blocking wrapper for collect_async."""
        return asyncio.run(self.collect_async(date))

    # Override the sync collect to use async
    def collect(self, date: str) -> dict:
        return self.collect_sync(date)
