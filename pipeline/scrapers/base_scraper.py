"""Base HTML scraper with UA rotation, caching, retries, and polite rate limiting.

This complements ``pipeline.collectors.base.BaseCollector`` — collectors talk to
structured APIs, whereas scrapers pull and parse raw HTML. Both share the same
tenacity/backoff and rate-limit philosophy, but scrapers add:

    * User-Agent rotation (realistic modern Chrome UAs)
    * Request jitter to look less bot-like
    * File-based HTML caching with a 24h TTL
    * BeautifulSoup parsing helper (``_get``) and raw-text helper (``_get_text``)

Subclasses should set ``name`` and optionally override ``rate_limit`` /
``cache_ttl_hours``.
"""

from __future__ import annotations

import hashlib
import logging
import random
import time
from abc import ABC
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import settings

logger = logging.getLogger(__name__)


# Five realistic, modern Chrome User-Agents (desktop).
USER_AGENTS: list[str] = [
    # Chrome 124 / macOS Sonoma
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome 124 / Windows 11
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome 125 / Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    # Chrome 123 / Ubuntu
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Chrome 124 / macOS Ventura
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.6367.119 Safari/537.36",
]


CACHE_ROOT = Path(__file__).resolve().parents[2] / "data" / "cache"


class ScraperError(Exception):
    """Raised when a scraper hits an unrecoverable error."""


class BaseScraper(ABC):
    """Abstract base for HTML scrapers.

    Subclasses should override ``name`` and can tune ``rate_limit``,
    ``rate_limit_jitter``, ``cache_ttl_hours``, and retry params.
    """

    name: str = "base_scraper"
    rate_limit: float = 2.0  # seconds per request (not req/sec!)
    rate_limit_jitter: float = 0.5  # +/- seconds of random jitter
    cache_ttl_hours: float = 24.0
    max_retries: int = 3
    backoff_min: float = 1.0
    backoff_max: float = 30.0
    timeout_seconds: float = 30.0

    def __init__(self, use_cache: bool = True) -> None:
        self._client: Optional[httpx.Client] = None
        self._last_request_time: float = 0.0
        self.use_cache = use_cache
        self.cache_dir = CACHE_ROOT / self.name
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(f"scraper.{self.name}")
        self.logger.setLevel(
            getattr(logging, settings.log_level.upper(), logging.INFO)
        )
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s | %(name)-28s | %(levelname)-5s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            self.logger.addHandler(handler)
            self.logger.propagate = False

    # ------------------------------------------------------------------
    # HTTP client
    # ------------------------------------------------------------------

    def _default_headers(self) -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            # Advertise only gzip/deflate — httpx doesn't ship brotli decoding
            # by default, and PriceCharting serves brotli when you ask for it,
            # which leads to unreadable bytes downstream. Keep it simple.
            "Accept-Encoding": "gzip, deflate",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "DNT": "1",
            "Connection": "keep-alive",
        }

    @property
    def client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(
                timeout=httpx.Timeout(self.timeout_seconds, connect=10.0),
                follow_redirects=True,
                headers=self._default_headers(),
            )
        return self._client

    # ------------------------------------------------------------------
    # Throttling
    # ------------------------------------------------------------------

    def _throttle(self) -> None:
        """Sleep to honour the per-request rate limit with random jitter."""
        if self.rate_limit <= 0:
            return
        jitter = random.uniform(-self.rate_limit_jitter, self.rate_limit_jitter)
        target_interval = max(0.0, self.rate_limit + jitter)
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < target_interval:
            time.sleep(target_interval - elapsed)
        self._last_request_time = time.monotonic()

    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------

    def _cache_path(self, url: str) -> Path:
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]
        return self.cache_dir / f"{digest}.html"

    def _cache_read(self, url: str) -> Optional[str]:
        if not self.use_cache:
            return None
        path = self._cache_path(url)
        if not path.exists():
            return None
        age_seconds = time.time() - path.stat().st_mtime
        if age_seconds > self.cache_ttl_hours * 3600:
            self.logger.debug("cache expired for %s (age=%.0fs)", url, age_seconds)
            return None
        try:
            return path.read_text(encoding="utf-8")
        except Exception as exc:  # pragma: no cover - disk issues
            self.logger.warning("cache read failed for %s: %s", url, exc)
            return None

    def _cache_write(self, url: str, content: str) -> None:
        if not self.use_cache:
            return
        try:
            self._cache_path(url).write_text(content, encoding="utf-8")
        except Exception as exc:  # pragma: no cover
            self.logger.warning("cache write failed for %s: %s", url, exc)

    # ------------------------------------------------------------------
    # Core fetch
    # ------------------------------------------------------------------

    def _fetch(self, url: str, *, params: Optional[dict] = None) -> str:
        """Fetch raw HTML with cache, throttling, and retries."""
        cached = self._cache_read(url)
        if cached is not None:
            self.logger.debug("cache hit: %s", url)
            return cached

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(
                multiplier=self.backoff_min, max=self.backoff_max
            ),
            retry=retry_if_exception_type(
                (
                    httpx.TimeoutException,
                    httpx.HTTPStatusError,
                    httpx.ConnectError,
                    httpx.RemoteProtocolError,
                )
            ),
            before_sleep=before_sleep_log(self.logger, logging.WARNING),
            reraise=True,
        )
        def _do_request() -> httpx.Response:
            self._throttle()
            # Rotate UA per-request for extra realism.
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            self.logger.debug(">> GET %s", url)
            resp = self.client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            return resp

        resp = _do_request()
        text = resp.text
        self._cache_write(url, text)
        return text

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, *, params: Optional[dict] = None) -> BeautifulSoup:
        """Fetch a URL and return a parsed BeautifulSoup tree."""
        html = self._fetch(url, params=params)
        return BeautifulSoup(html, "html.parser")

    def _get_text(self, url: str, *, params: Optional[dict] = None) -> str:
        """Fetch a URL and return raw HTML text."""
        return self._fetch(url, params=params)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._client and not self._client.is_closed:
            self._client.close()

    def __enter__(self) -> "BaseScraper":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
