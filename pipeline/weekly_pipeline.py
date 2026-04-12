"""Weekly pipeline orchestrator.

Extends DailyPipeline by first running the PSA Pop Report scraper
(which walks all ~20 set pages), then runs the full daily sequence.

CLI:
    python3 -m pipeline.weekly_pipeline
    python3 -m pipeline.weekly_pipeline --date 2026-04-12
    python3 -m pipeline.weekly_pipeline --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional

from pipeline.daily_pipeline import (
    DailyPipeline,
    PSA_POP_SPEC,
    _try_import_scraper,
    LOG_DIR,
    _configure_logging,
)

logger = logging.getLogger("pipeline.weekly")


class WeeklyPipeline(DailyPipeline):
    """Weekly pipeline: PSA Pop scrape, then the full daily sequence."""

    def _stage_train_model(self) -> None:
        """Train the prediction model on latest data."""
        if self.dry_run:
            logger.info("[DRY RUN] would train prediction model")
            return
        try:
            from pipeline.model.train import train_model
        except ImportError as exc:
            logger.warning("Model module not available: %s", exc)
            self.errors.append(f"model:train: module unavailable ({exc})")
            return
        from db.connection import get_db
        with get_db() as db:
            result = train_model(db)
            logger.info("Model training result: %s", result)

    def _stage_psa_pop(self) -> None:
        """Run the PSA Pop Report scraper (all set pages)."""
        if "psa_pop" in self.skip:
            logger.info("Skipping PSA Pop scraper (explicit --skip)")
            return

        if self.dry_run:
            logger.info("[DRY RUN] would run PSA Pop scraper (~%d min across 20 sets)",
                        PSA_POP_SPEC["est_minutes"])
            return

        cls = _try_import_scraper(PSA_POP_SPEC)
        if cls is None:
            self.errors.append("psa_pop: module unavailable")
            return

        logger.info("Running scraper: psa_pop")
        instance = cls()
        for method_name in ("run", "scrape", "collect", "execute"):
            method = getattr(instance, method_name, None)
            if callable(method):
                from pipeline.daily_pipeline import _accepts_arg
                result = method(self.date) if _accepts_arg(method) else method()
                self._accumulate_result(result)
                return
        raise RuntimeError("PSA Pop scraper has no run/scrape/collect/execute method")

    def run(self) -> int:  # type: ignore[override]
        log_file = LOG_DIR / f"weekly_{self.date}.log"
        _configure_logging(log_file)

        stage_label = f"weekly:{self.stage}"
        self._start_run(stage_label)

        import time
        run_start = time.time()
        print(f"=== Pokemon Analytics Weekly Pipeline ===")
        print(f"    date:    {self.date}")
        print(f"    stage:   {self.stage}")
        print(f"    skip:    {self.skip or '(none)'}")
        print(f"    dry_run: {self.dry_run}")
        print(f"    log:     {log_file}")

        try:
            # 1. PSA Pop (only when stage is all or scrape)
            if self.stage in (self.STAGE_ALL, self.STAGE_SCRAPE):
                self._run_stage("scrape:psa_pop", self._stage_psa_pop)

            # 2. Daily stages
            if self.stage in (self.STAGE_ALL, self.STAGE_SCRAPE):
                self._stage_scrape()
            if self.stage in (self.STAGE_ALL, self.STAGE_TRANSFORM):
                self._stage_transform()
            if self.stage in (self.STAGE_ALL, self.STAGE_COMPUTE):
                self._stage_compute()

            # 3. Model training + projection (weekly retraining)
            if self.stage in (self.STAGE_ALL,):
                self._run_stage("model:train", self._stage_train_model)
                self._stage_predict()

            total_secs = time.time() - run_start
            print(f"\n=== Weekly pipeline finished in {total_secs:.1f}s ===")
            print(f"    errors: {len(self.errors)}")
            for err in self.errors:
                print(f"    - {err}")

            if self.errors:
                self._finish_run("done_with_errors")
                return 1
            self._finish_run("done")
            return 0

        except Exception as exc:  # noqa: BLE001
            logger.exception("Fatal weekly pipeline error")
            self.errors.append(f"fatal: {exc}")
            self._finish_run("failed", f"fatal={exc}")
            return 2


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="weekly_pipeline",
        description="Run the Pokemon Analytics weekly pipeline (PSA Pop + daily).",
    )
    p.add_argument("--date", default=None,
                   help="Target date YYYY-MM-DD (default: today)")
    p.add_argument("--stage",
                   choices=["all", "scrape", "transform", "compute"],
                   default="all",
                   help="Which stage group to run (default: all)")
    p.add_argument("--skip", action="append", default=[],
                   help="Scraper name to skip (repeatable). "
                        "Options: psa_pop, pricecharting, onethirty_point, tcgplayer")
    p.add_argument("--dry-run", action="store_true",
                   help="Log what would happen; don't hit network or write to DB")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    pipe = WeeklyPipeline(
        date=args.date,
        stage=args.stage,
        skip=args.skip,
        dry_run=args.dry_run,
    )
    return pipe.run()


if __name__ == "__main__":
    sys.exit(main())
