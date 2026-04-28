"""
One-shot bootstrap from PriceCharting.

Pulls PC's full historical price chart + product image for every card via
the PC search-by-name endpoint.  Writes:

  * cards.image_url            (PC product image, if currently blank)
  * cards.pc_canonical_url     (cached canonical URL for fast re-fetch)
  * price_history rows         (one per chart-data date, INSERT OR REPLACE)

PC's product page embeds chart history inline as JSON in
`VGPC.chart_data = {...};` with six series:

  used        -> raw_price       (Ungraded)
  cib         -> psa_7_price     (Grade 7)
  new         -> psa_8_price     (Grade 8)
  graded      -> psa_9_price     (Grade 9)
  boxonly     -> (no column)     (Grade 9.5 — not in schema, skipped)
  manualonly  -> psa_10_price    (PSA 10)

Each series is a list of [unix_ms, price_cents] points. Monthly snapshots,
typically ~13 points covering ~1 year.

Usage:

    cd /Users/yoson/pokemon-analytics
    python3 -m scripts.bootstrap_pc_history_and_images                    # all cards
    python3 -m scripts.bootstrap_pc_history_and_images --blank-images     # only the ~903 cards with no image
    python3 -m scripts.bootstrap_pc_history_and_images --limit 5          # smoke test
    python3 -m scripts.bootstrap_pc_history_and_images --resume           # skip cards we already have a pc_canonical_url for
    python3 -m scripts.bootstrap_pc_history_and_images --card-id 9085069  # single card

Polite throttle: 3.0s/req with jitter.  Resumable.  Logs to data/logs/.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import random
import re
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup

# Make `db` and friends importable when invoked as `python -m scripts.bootstrap...`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection import get_db


# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

BASE_URL = "https://www.pricecharting.com"
SEARCH_PATH = "/search-products"
THROTTLE_SECONDS = 3.0
THROTTLE_JITTER = 0.5
TIMEOUT_SECONDS = 30.0

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)

# Map a chart_data series key to a price_history column.
# 'boxonly' (Grade 9.5) intentionally has no column in the current schema.
SERIES_TO_COLUMN: dict[str, str] = {
    "used": "raw_price",
    "cib": "psa_7_price",
    "new": "psa_8_price",
    "graded": "psa_9_price",
    "manualonly": "psa_10_price",
}

CHART_DATA_RE = re.compile(r"VGPC\.chart_data\s*=\s*(\{.*?\});", re.DOTALL)
PRODUCT_ID_RE = re.compile(r'product-id\s*=\s*["\'](\d+)["\']')
IMAGE_RE = re.compile(
    r"https://storage\.googleapis\.com/images\.pricecharting\.com/[^\"'\s]+/1600\.jpg"
)

LOG_DIR = Path(__file__).resolve().parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------

logger = logging.getLogger("bootstrap_pc")


def _configure_logging(verbose: bool = False) -> None:
    log_file = LOG_DIR / f"bootstrap_pc_{dt.date.today().isoformat()}.log"
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-5s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    if not any(getattr(h, "_bootstrap_pc", False) for h in root.handlers):
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        sh._bootstrap_pc = True  # type: ignore[attr-defined]
        root.addHandler(sh)
    fh = logging.FileHandler(str(log_file))
    fh.setFormatter(fmt)
    root.addHandler(fh)


# ----------------------------------------------------------------------
# Slugification — PC's canonical URL pattern is /game/{set-slug}/{name-slug}-{number}
# ----------------------------------------------------------------------


def _slugify_set(set_name: str) -> str:
    """Convert 'Pokemon Scarlet & Violet 151' -> 'pokemon-scarlet-&-violet-151'."""
    s = (set_name or "").lower().strip()
    # PC keeps the literal '&' in URLs (no entity escaping in the path).
    # We just convert spaces around it to dashes.
    s = re.sub(r"\s+", "-", s)
    return s


def _slugify_name(product_name: str, card_number: Optional[int]) -> str:
    """Convert 'Voltorb [Cosmos Professor Program] #100' + 100 -> 'voltorb-cosmos-professor-program-100'.

    If `card_number` is None we fall back to the `#N` embedded in product_name
    (the cards table has NULL card_number for many older imports).
    """
    s = (product_name or "").lower()
    # Extract embedded #N as a fallback for card_number.
    embedded = None
    m = re.search(r"#\s*(\d+)", s)
    if m:
        try:
            embedded = int(m.group(1))
        except ValueError:
            embedded = None
    # Strip the #N from the name body — we'll re-append it once at the end.
    s = re.sub(r"#\s*\d+", "", s)
    # Drop bracket characters but KEEP their contents (variant tags).
    s = s.replace("[", " ").replace("]", " ")
    # Strip apostrophes and other punctuation that don't appear in PC slugs.
    s = re.sub(r"[''\"`]", "", s)
    # Keep alphanumerics, spaces, dashes, ampersands; replace anything else.
    s = re.sub(r"[^a-z0-9&\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" ", "-")
    s = re.sub(r"-+", "-", s).strip("-")
    final_number = card_number if card_number is not None else embedded
    if final_number is not None:
        s = f"{s}-{final_number}"
    return s


def _canonical_url_for_card(card: dict[str, Any]) -> Optional[str]:
    set_name = card.get("set_name") or ""
    product_name = card.get("product_name") or ""
    if not set_name or not product_name:
        return None
    set_slug = _slugify_set(set_name)
    name_slug = _slugify_name(product_name, card.get("card_number"))
    if not set_slug or not name_slug:
        return None
    return f"{BASE_URL}/game/{set_slug}/{name_slug}"


# ----------------------------------------------------------------------
# HTTP
# ----------------------------------------------------------------------


class PCClient:
    """Tiny stateful client: throttled, follows redirects, single httpx.Client.

    Handles transient 403/429 by exponential backoff (PC has a per-IP rate limit
    that occasionally trips on bursty traffic — a single retry after a sleep
    almost always succeeds).
    """

    def __init__(self) -> None:
        self._client = httpx.Client(
            timeout=httpx.Timeout(TIMEOUT_SECONDS, connect=10.0),
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                # NOTE: no `br` — httpx doesn't decode brotli without the optional
                # `brotli` package, and PC will send brotli if it's offered. gzip
                # is handled natively.
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            },
        )
        self._last_request_at = 0.0

    def _throttle(self) -> None:
        target = THROTTLE_SECONDS + random.uniform(-THROTTLE_JITTER, THROTTLE_JITTER)
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < target:
            time.sleep(target - elapsed)
        self._last_request_at = time.monotonic()

    def get(self, url: str) -> httpx.Response:
        """Throttled GET with exponential backoff on 403/429."""
        backoff = 15.0
        for attempt in range(4):
            self._throttle()
            resp = self._client.get(url)
            if resp.status_code not in (403, 429):
                return resp
            # Rate-limited. Sleep and retry; do NOT count as a failure.
            logger.warning(
                "rate-limited (%d) on %s — sleeping %.0fs (attempt %d/4)",
                resp.status_code, url, backoff, attempt + 1,
            )
            time.sleep(backoff)
            backoff *= 2
        # Final attempt — return whatever we got, caller will treat as failure.
        return resp

    def close(self) -> None:
        self._client.close()


# ----------------------------------------------------------------------
# Page fetch + parse
# ----------------------------------------------------------------------


def _fetch_product_page(
    client: PCClient, card: dict[str, Any]
) -> Optional[httpx.Response]:
    """Fetch the canonical PC product page for a card.

    Strategy:
      1. If we have a cached `pc_canonical_url` from a prior run, use it.
      2. Otherwise build the canonical slug from set_name + product_name + number.
      3. Verify the response is a real product page (not redirected to /search-products).

    Returns None when the card has no exact PC match.
    """
    # 1. Cached canonical URL (fast path on resume)
    cached = card.get("pc_canonical_url")
    if cached:
        try:
            resp = client.get(cached)
            if resp.status_code == 200 and "/game/" in str(resp.url):
                return resp
        except httpx.RequestError as exc:
            logger.warning("cached canonical fetch failed for %s: %s", card["id"], exc)

    # 2. Direct slug construction
    constructed = _canonical_url_for_card(card)
    if not constructed:
        return None
    try:
        resp = client.get(constructed)
    except httpx.RequestError as exc:
        logger.warning("direct fetch failed for %s: %s", card["id"], exc)
        return None
    if resp.status_code != 200:
        return None
    # 3. PC redirects to /search-products when the slug doesn't exist.
    if "/search-products" in str(resp.url):
        return None
    return resp


def _parse_product_page(html: str) -> dict[str, Any]:
    """Extract image URL, product-id, canonical URL, and chart_data history."""
    out: dict[str, Any] = {
        "image_url": None,
        "pc_product_id": None,
        "pc_canonical_url": None,
        "history": {},  # date -> {raw_price, psa_7_price, ...}
    }

    # Canonical URL
    soup = BeautifulSoup(html, "html.parser")
    canonical = soup.find("link", rel="canonical")
    if canonical and canonical.get("href"):
        out["pc_canonical_url"] = canonical["href"]

    # Product ID
    pid_m = PRODUCT_ID_RE.search(html)
    if pid_m:
        out["pc_product_id"] = pid_m.group(1)

    # Image (high-res 1600.jpg)
    img_m = IMAGE_RE.search(html)
    if img_m:
        out["image_url"] = img_m.group(0)

    # chart_data
    chart_m = CHART_DATA_RE.search(html)
    if chart_m:
        try:
            chart = json.loads(chart_m.group(1))
        except json.JSONDecodeError as exc:
            logger.warning("chart_data parse failed: %s", exc)
            chart = {}
        history: dict[str, dict[str, Optional[float]]] = {}
        for series, points in chart.items():
            column = SERIES_TO_COLUMN.get(series)
            if column is None:
                continue
            for point in points:
                if not isinstance(point, list) or len(point) < 2:
                    continue
                ts_ms, cents = point[0], point[1]
                if not isinstance(ts_ms, (int, float)) or not isinstance(
                    cents, (int, float)
                ):
                    continue
                # PC stores 0 as "no data" — keep them as None so the
                # transformer doesn't treat them as a real price drop.
                price = (cents / 100.0) if cents and cents > 0 else None
                date = dt.datetime.fromtimestamp(ts_ms / 1000.0).date().isoformat()
                history.setdefault(date, {})[column] = price
        out["history"] = history

    return out


# ----------------------------------------------------------------------
# DB writes
# ----------------------------------------------------------------------


def _update_card_metadata(
    db, card_id: str, parsed: dict[str, Any], force_image: bool
) -> None:
    """Update cards.image_url + cards.pc_canonical_url for one card."""
    sets: list[str] = []
    args: list[Any] = []
    if parsed.get("pc_canonical_url"):
        sets.append("pc_canonical_url = ?")
        args.append(parsed["pc_canonical_url"])
    if parsed.get("image_url"):
        if force_image:
            sets.append("image_url = ?")
            args.append(parsed["image_url"])
        else:
            sets.append(
                "image_url = COALESCE(NULLIF(image_url, ''), ?)"
            )
            args.append(parsed["image_url"])
    if not sets:
        return
    args.append(card_id)
    db.execute(f"UPDATE cards SET {', '.join(sets)} WHERE id = ?", args)


def _write_history(db, card_id: str, history: dict[str, dict[str, Any]]) -> int:
    """Write chart-data history rows. Returns number of rows written."""
    written = 0
    for date, prices in history.items():
        raw = prices.get("raw_price")
        psa_10 = prices.get("psa_10_price")
        vs_raw = None
        vs_raw_pct = None
        if raw and psa_10:
            vs_raw = psa_10 - raw
            if raw:
                vs_raw_pct = (vs_raw / raw) * 100.0
        db.execute(
            """
            INSERT OR REPLACE INTO price_history (
                card_id, date,
                raw_price, psa_7_price, psa_8_price, psa_9_price, psa_10_price,
                psa_10_vs_raw, psa_10_vs_raw_pct,
                sales_volume, interpolated, interpolation_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, NULL)
            """,
            (
                card_id,
                date,
                raw,
                prices.get("psa_7_price"),
                prices.get("psa_8_price"),
                prices.get("psa_9_price"),
                prices.get("psa_10_price"),
                vs_raw,
                vs_raw_pct,
            ),
        )
        written += 1
    return written


# ----------------------------------------------------------------------
# Loaders
# ----------------------------------------------------------------------


def _load_cards(
    *,
    blank_images: bool,
    resume: bool,
    limit: Optional[int],
    card_id: Optional[str],
    shard: Optional[tuple[int, int]] = None,
    set_code: Optional[str] = None,
) -> list[dict]:
    where = ["c.sealed_product = 'N'"]
    params: list[Any] = []
    if card_id:
        where = ["c.id = ?"]
        params = [card_id]
    else:
        if set_code:
            where.append("c.set_code = ?")
            params.append(set_code)
        if blank_images:
            where.append("(c.image_url IS NULL OR c.image_url = '')")
        if resume:
            where.append("(c.pc_canonical_url IS NULL OR c.pc_canonical_url = '')")
        if shard:
            n, m = shard
            # Hash by rowid mod m so the same card always lands on the same worker.
            where.append(f"(c.rowid % {m}) = {n}")
    sql = (
        "SELECT c.id, c.product_name, c.set_code, c.card_number, "
        "       c.image_url, c.pc_canonical_url, s.set_name "
        "FROM cards c LEFT JOIN sets s ON s.set_code = c.set_code "
        "WHERE " + " AND ".join(where) +
        " ORDER BY c.set_code, c.card_number"
    )
    with get_db() as db:
        rows = db.execute(sql, params).fetchall()
    cards = [dict(r) for r in rows]
    if limit:
        cards = cards[:limit]
    return cards


# ----------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------


def run(
    *,
    blank_images: bool = False,
    resume: bool = False,
    limit: Optional[int] = None,
    card_id: Optional[str] = None,
    force_image: bool = False,
    dry_run: bool = False,
    shard: Optional[tuple[int, int]] = None,
    set_code: Optional[str] = None,
) -> dict[str, int]:
    cards = _load_cards(
        blank_images=blank_images,
        resume=resume,
        limit=limit,
        card_id=card_id,
        shard=shard,
        set_code=set_code,
    )
    shard_label = f" shard={shard[0]}/{shard[1]}" if shard else ""
    logger.info(
        "bootstrap_pc starting%s: cards=%d blank_images=%s resume=%s force_image=%s dry_run=%s",
        shard_label, len(cards), blank_images, resume, force_image, dry_run,
    )

    client = PCClient()
    processed = 0
    no_match = 0
    errors = 0
    history_rows = 0
    images_filled = 0
    started = time.time()

    try:
        for idx, card in enumerate(cards, start=1):
            cid = card["id"]
            try:
                resp = _fetch_product_page(client, card)
                if resp is None:
                    no_match += 1
                    logger.warning("[%d/%d] %s no_match (%s)", idx, len(cards), cid, card.get("product_name"))
                    continue
                parsed = _parse_product_page(resp.text)

                if not parsed.get("pc_canonical_url"):
                    no_match += 1
                    logger.warning("[%d/%d] %s no_canonical_url", idx, len(cards), cid)
                    continue

                if dry_run:
                    logger.info(
                        "[%d/%d] %s DRY image=%s history_dates=%d canonical=%s",
                        idx, len(cards), cid,
                        bool(parsed.get("image_url")),
                        len(parsed.get("history") or {}),
                        parsed.get("pc_canonical_url"),
                    )
                    processed += 1
                    continue

                with get_db() as db:
                    _update_card_metadata(db, cid, parsed, force_image=force_image)
                    rows_written = _write_history(db, cid, parsed.get("history") or {})

                history_rows += rows_written
                if parsed.get("image_url") and (force_image or not card.get("image_url")):
                    images_filled += 1
                processed += 1

                if idx % 10 == 0 or idx == len(cards):
                    elapsed = time.time() - started
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta = (len(cards) - idx) / rate if rate > 0 else 0
                    logger.info(
                        "[%d/%d] %s ok rows=%d image=%s | rate=%.2f/s eta=%.1fmin",
                        idx, len(cards), cid, rows_written,
                        bool(parsed.get("image_url")),
                        rate, eta / 60.0,
                    )
                else:
                    logger.info(
                        "[%d/%d] %s ok rows=%d image=%s",
                        idx, len(cards), cid, rows_written,
                        bool(parsed.get("image_url")),
                    )
            except Exception as exc:  # noqa: BLE001
                errors += 1
                logger.error("[%d/%d] %s error: %s", idx, len(cards), cid, exc)
                continue
    finally:
        client.close()

    summary = {
        "cards_total": len(cards),
        "processed": processed,
        "no_match": no_match,
        "errors": errors,
        "history_rows_written": history_rows,
        "images_filled": images_filled,
        "elapsed_seconds": int(time.time() - started),
    }
    logger.info("bootstrap_pc done: %s", summary)
    return summary


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="bootstrap_pc_history_and_images",
        description="One-shot PriceCharting bootstrap: image + full chart_data history per card.",
    )
    p.add_argument("--blank-images", action="store_true",
                   help="Only process cards that currently have no image_url.")
    p.add_argument("--resume", action="store_true",
                   help="Skip cards that already have a pc_canonical_url stored.")
    p.add_argument("--limit", type=int, default=None,
                   help="Limit to N cards (for smoke tests).")
    p.add_argument("--card-id", type=str, default=None,
                   help="Process a single card_id (overrides other selectors).")
    p.add_argument("--force-image", action="store_true",
                   help="Overwrite cards.image_url even if already set.")
    p.add_argument("--dry-run", action="store_true",
                   help="Parse pages but do not write to the DB.")
    p.add_argument("--shard", type=str, default=None,
                   help="Shard selector 'N/M' — process only cards where rowid % M == N. "
                        "Use to run multiple workers in parallel without overlap.")
    p.add_argument("--set-code", type=str, default=None,
                   help="Only process cards in this set_code (e.g. 'PROMO').")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Debug logging.")
    return p


def _parse_shard(s: Optional[str]) -> Optional[tuple[int, int]]:
    if not s:
        return None
    try:
        n_str, m_str = s.split("/", 1)
        n, m = int(n_str), int(m_str)
        if m <= 0 or n < 0 or n >= m:
            raise ValueError
        return (n, m)
    except (ValueError, AttributeError):
        raise SystemExit(f"--shard must look like '0/4', got: {s!r}")


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    _configure_logging(verbose=args.verbose)
    shard = _parse_shard(args.shard)
    summary = run(
        blank_images=args.blank_images,
        resume=args.resume,
        limit=args.limit,
        card_id=args.card_id,
        force_image=args.force_image,
        dry_run=args.dry_run,
        shard=shard,
        set_code=args.set_code,
    )
    print(json.dumps(summary, indent=2))
    if summary["errors"] > summary["processed"] // 4 and summary["processed"] > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
