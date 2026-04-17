"""Lightweight alerting for pipeline failures.

Writes structured JSON lines to data/logs/alerts.jsonl and, if the
ALERT_WEBHOOK_URL environment variable is set, POSTs a small JSON
payload to that URL. Designed to have zero third-party dependencies
(stdlib only) so cron invocations don't break due to missing packages.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("pipeline.alerting")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ALERT_LOG = PROJECT_ROOT / "data" / "logs" / "alerts.jsonl"


def _now_iso() -> str:
    return dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _ensure_parent() -> None:
    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)


def alert(
    severity: str,
    source: str,
    message: str,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """Record an alert locally and optionally POST it to a webhook.

    severity: 'info' | 'warn' | 'error' | 'fatal'
    source:   short string identifying the emitter (e.g. 'daily_pipeline')
    message:  human readable summary
    context:  optional dict of extra metadata (run_id, stage, error, etc.)
    """
    payload: Dict[str, Any] = {
        "ts": _now_iso(),
        "severity": severity,
        "source": source,
        "message": message,
        "context": context or {},
    }

    _ensure_parent()
    try:
        with ALERT_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to write alert to %s: %s", ALERT_LOG, exc)

    webhook = os.environ.get("ALERT_WEBHOOK_URL")
    if not webhook:
        return

    try:
        data = json.dumps(payload, default=str).encode("utf-8")
        req = urllib.request.Request(
            webhook,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except urllib.error.URLError as exc:
        logger.warning("Alert webhook POST failed: %s", exc)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Alert webhook unexpected error: %s", exc)
