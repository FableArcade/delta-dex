"""Base collector with rate limiting, retries, and logging."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from config.settings import settings
from db.connection import get_db

logger = logging.getLogger(__name__)


class CollectorError(Exception):
    """Raised when a collector encounters an unrecoverable error."""


class BaseCollector(ABC):
    """Abstract base for all data collectors.

    Subclasses must set ``name``, ``rate_limit``, and implement ``collect``.
    """

    name: str = "base"
    rate_limit: float = 1.0  # requests per second
    max_retries: int = 3
    backoff_min: float = 1.0
    backoff_max: float = 30.0

    def __init__(self) -> None:
        self._last_request_time: float = 0.0
        self._client: httpx.Client | None = None
        self.logger = logging.getLogger(f"collector.{self.name}")
        self.logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s | %(name)-22s | %(levelname)-5s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            self.logger.addHandler(handler)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @property
    def client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True,
                headers={"User-Agent": "PokemonAnalytics/1.0"},
            )
        return self._client

    def _throttle(self) -> None:
        """Sleep to honour ``rate_limit`` (requests per second)."""
        if self.rate_limit <= 0:
            return
        min_interval = 1.0 / self.rate_limit
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.monotonic()

    def _request(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict | None = None,
        headers: dict | None = None,
        json: dict | None = None,
    ) -> httpx.Response:
        """Issue an HTTP request with rate-limiting and exponential-backoff retries."""

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(
                multiplier=self.backoff_min, max=self.backoff_max
            ),
            retry=retry_if_exception_type(
                (httpx.TimeoutException, httpx.HTTPStatusError, httpx.ConnectError)
            ),
            before_sleep=before_sleep_log(self.logger, logging.WARNING),
            reraise=True,
        )
        def _do_request() -> httpx.Response:
            self._throttle()
            self.logger.debug(">> %s %s params=%s", method, url, params)
            resp = self.client.request(
                method, url, params=params, headers=headers, json=json
            )
            resp.raise_for_status()
            return resp

        return _do_request()

    # ------------------------------------------------------------------
    # Card helpers
    # ------------------------------------------------------------------

    def get_cards(self, sealed: bool = False) -> list[dict]:
        """Return all trackable cards from the database."""
        with get_db() as db:
            if sealed:
                rows = db.execute(
                    "SELECT * FROM cards ORDER BY set_code, card_number"
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT * FROM cards WHERE sealed_product = 'N' "
                    "ORDER BY set_code, card_number"
                ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def collect(self, date: str) -> dict:
        """Run the collection for *date* (YYYY-MM-DD).

        Returns a summary dict: ``{"processed": int, "errors": int}``.
        """

    def close(self) -> None:
        if self._client and not self._client.is_closed:
            self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
