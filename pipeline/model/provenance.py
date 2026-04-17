"""Provenance hashing for model projections.

Every projection row should carry:
  * model_version       -- which trained artifact produced it
  * training_cutoff     -- the last date of data used to train
  * feature_hash        -- stable hash of the ordered feature list

Together these let us answer "exactly which model + feature schema + data
window produced this number?" months later when debugging a bad call.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


def feature_hash(feature_columns: Iterable[str]) -> str:
    """Stable short hash of the ordered feature list.

    Order matters — swapping two columns produces a different model even
    if the names overlap — so we hash the tuple, not a set.
    """
    payload = json.dumps(list(feature_columns), sort_keys=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class Provenance:
    model_version: str
    training_cutoff: str  # ISO date
    feature_hash: str

    def as_row(self) -> tuple[str, str, str]:
        return (self.model_version, self.training_cutoff, self.feature_hash)


def load_training_cutoff(version: str, models_dir: Path) -> Optional[str]:
    """Read the training cutoff sidecar file, if present.

    Convention: data/models/cutoff_<version>.txt contains an ISO date.
    Returns None if absent — caller decides whether to fail or fall back.
    """
    path = models_dir / f"cutoff_{version}.txt"
    if not path.exists():
        return None
    return path.read_text().strip()


def write_training_cutoff(version: str, cutoff_iso: str, models_dir: Path) -> None:
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / f"cutoff_{version}.txt").write_text(cutoff_iso)
