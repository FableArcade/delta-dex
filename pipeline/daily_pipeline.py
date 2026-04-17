"""Daily pipeline orchestrator for Pokemon Analytics.

Runs scrapers -> transformers -> computes in sequence, logging each stage
to the pipeline_runs table.  Resilient to missing scraper modules (they are
being built by other agents).

CLI:
    python3 -m pipeline.daily_pipeline
    python3 -m pipeline.daily_pipeline --date 2026-04-09
    python3 -m pipeline.daily_pipeline --stage scrape
    python3 -m pipeline.daily_pipeline --stage compute
    python3 -m pipeline.daily_pipeline --skip pricecharting
    python3 -m pipeline.daily_pipeline --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib
import json
import logging
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from db.connection import get_db
from pipeline.alerting import alert as emit_alert

# ------------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("pipeline.daily")


def _configure_logging(log_file: Optional[Path] = None, level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s | %(name)-22s | %(levelname)-5s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Stream handler (stdout)
    if not any(isinstance(h, logging.StreamHandler) and getattr(h, "_daily_stream", False)
               for h in root.handlers):
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        sh._daily_stream = True  # type: ignore[attr-defined]
        root.addHandler(sh)

    # File handler
    if log_file is not None:
        fh = logging.FileHandler(str(log_file))
        fh.setFormatter(fmt)
        root.addHandler(fh)


# ------------------------------------------------------------------
# Scraper registry -- wrapped imports so missing modules don't crash
# ------------------------------------------------------------------

SCRAPER_SPECS: List[Dict[str, str]] = [
    {
        "name": "pricecharting",
        "module": "pipeline.scrapers.pricecharting_scraper",
        "class": "PriceChartingScraper",
        "est_minutes": 300,
    },
    {
        "name": "onethirty_point",
        "module": "pipeline.scrapers.onethirty_point_scraper",
        "class": "OneThirtyPointScraper",
        "est_minutes": 25,
    },
    {
        "name": "tcgplayer",
        "module": "pipeline.scrapers.tcgplayer_scraper",
        "class": "TCGPlayerScraper",
        "est_minutes": 15,
    },
]

PSA_POP_SPEC: Dict[str, str] = {
    "name": "psa_pop",
    "module": "pipeline.scrapers.psa_pop_scraper",
    "class": "PSAPopScraper",
    "est_minutes": 30,
}


def _try_import_scraper(spec: Dict[str, Any]) -> Optional[type]:
    """Attempt to import a scraper class. Returns None if unavailable."""
    try:
        mod = importlib.import_module(spec["module"])
    except ImportError as exc:
        logger.warning("Scraper module %s not found (%s) -- skipping", spec["module"], exc)
        return None
    except Exception as exc:
        logger.warning("Scraper module %s failed to import (%s) -- skipping",
                       spec["module"], exc)
        return None

    cls = getattr(mod, spec["class"], None)
    if cls is None:
        logger.warning("Scraper class %s not found in %s -- skipping",
                       spec["class"], spec["module"])
        return None
    return cls


# ------------------------------------------------------------------
# DailyPipeline
# ------------------------------------------------------------------

class DailyPipeline:
    """Orchestrates the daily data pipeline."""

    STAGE_ALL = "all"
    STAGE_SCRAPE = "scrape"
    STAGE_TRANSFORM = "transform"
    STAGE_COMPUTE = "compute"
    STAGE_PREDICT = "predict"

    def __init__(
        self,
        date: Optional[str] = None,
        stage: str = "all",
        skip: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> None:
        self.date: str = date or dt.date.today().isoformat()
        self.stage: str = stage
        self.skip: List[str] = [s.lower() for s in (skip or [])]
        self.dry_run: bool = dry_run
        self.run_id: Optional[int] = None
        self.errors: List[str] = []
        self.stage_failures: List[str] = []  # stages that hit an exception
        self.stage_timings: Dict[str, float] = {}
        self.cards_processed: int = 0
        # Per-source scrape completion: {"pricecharting": {"expected":N, "processed":M, "pct":X}, ...}
        self.scraper_completion: Dict[str, Dict[str, Any]] = {}

    # -------------------- run bookkeeping --------------------

    def _start_run(self, stage_label: str) -> None:
        if self.dry_run:
            logger.info("[DRY RUN] would insert pipeline_runs row for stage=%s", stage_label)
            self.run_id = -1
            return
        with get_db() as db:
            cur = db.execute(
                "INSERT INTO pipeline_runs (started_at, status, stage, notes) "
                "VALUES (?, 'running', ?, ?)",
                (
                    dt.datetime.utcnow().isoformat(timespec="seconds"),
                    stage_label,
                    f"date={self.date}",
                ),
            )
            self.run_id = cur.lastrowid
        logger.info("pipeline_runs row created: id=%s stage=%s date=%s",
                    self.run_id, stage_label, self.date)

    def _update_stage(self, sub_stage: str) -> None:
        if self.dry_run or self.run_id in (None, -1):
            logger.info("[stage] %s", sub_stage)
            return
        with get_db() as db:
            db.execute(
                "UPDATE pipeline_runs SET stage = ? WHERE id = ?",
                (sub_stage, self.run_id),
            )
        logger.info("[stage] %s", sub_stage)

    def _finish_run(self, status: str, notes_suffix: str = "") -> None:
        summary = self._build_summary(notes_suffix)
        completion_json = json.dumps(self.scraper_completion) if self.scraper_completion else None
        if self.dry_run or self.run_id in (None, -1):
            logger.info("[DRY RUN] would finish pipeline_runs: status=%s notes=%s completion=%s",
                        status, summary, completion_json)
            return
        # Use scraper_completion_json column if it exists (see scripts/migrate_ops_columns.py).
        with get_db() as db:
            cols = {r["name"] for r in db.execute("PRAGMA table_info(pipeline_runs)").fetchall()}
            if "scraper_completion_json" in cols:
                db.execute(
                    """UPDATE pipeline_runs
                          SET finished_at = ?,
                              status = ?,
                              cards_processed = ?,
                              errors = ?,
                              notes = ?,
                              scraper_completion_json = ?
                        WHERE id = ?""",
                    (
                        dt.datetime.utcnow().isoformat(timespec="seconds"),
                        status,
                        self.cards_processed,
                        len(self.errors),
                        summary,
                        completion_json,
                        self.run_id,
                    ),
                )
            else:
                db.execute(
                    """UPDATE pipeline_runs
                          SET finished_at = ?,
                              status = ?,
                              cards_processed = ?,
                              errors = ?,
                              notes = ?
                        WHERE id = ?""",
                    (
                        dt.datetime.utcnow().isoformat(timespec="seconds"),
                        status,
                        self.cards_processed,
                        len(self.errors),
                        summary,
                        self.run_id,
                    ),
                )
        logger.info("pipeline_runs updated: id=%s status=%s", self.run_id, status)

    def _build_summary(self, suffix: str = "") -> str:
        parts = [f"date={self.date}"]
        for stg, secs in self.stage_timings.items():
            parts.append(f"{stg}={secs:.1f}s")
        if self.errors:
            parts.append(f"errors={len(self.errors)}")
        if suffix:
            parts.append(suffix)
        return " | ".join(parts)

    # -------------------- stage runner --------------------

    def _run_stage(self, name: str, func: Callable[[], Any]) -> bool:
        """Run a single stage callable. Returns True on success, False on failure.

        Records timing and captures full traceback on failure. Individual stage
        failures do NOT re-raise (so later stages can still run), but they ARE
        tracked in self.stage_failures — if any are present at the end of the
        run, the overall run is marked FAILED (not DONE).
        """
        self._update_stage(name)
        print(f"\n>>> STAGE: {name}")
        start = time.time()
        try:
            func()
            elapsed = time.time() - start
            self.stage_timings[name] = elapsed
            print(f"    OK {name} ({elapsed:.1f}s)")
            return True
        except Exception as exc:  # noqa: BLE001  -- resilience
            elapsed = time.time() - start
            self.stage_timings[name] = elapsed
            tb = traceback.format_exc()
            msg = f"{name}: {exc.__class__.__name__}: {exc}"
            self.errors.append(msg)
            self.stage_failures.append(name)
            logger.error("Stage %s failed: %s", name, exc)
            logger.error("Traceback:\n%s", tb)
            print(f"    FAIL {name} ({elapsed:.1f}s): {exc}")
            try:
                emit_alert(
                    severity="error",
                    source="daily_pipeline",
                    message=f"Stage {name} failed: {exc.__class__.__name__}: {exc}",
                    context={
                        "run_id": self.run_id,
                        "date": self.date,
                        "stage": name,
                        "traceback": tb,
                    },
                )
            except Exception:  # noqa: BLE001 -- alerting must never break pipeline
                logger.exception("alerting failed (non-fatal)")
            return False

    # -------------------- scrapers --------------------

    def _run_scraper(self, spec: Dict[str, Any]) -> None:
        name = spec["name"]
        if name in self.skip:
            logger.info("Skipping scraper %s (explicit --skip)", name)
            self.scraper_completion[name] = {
                "expected": None, "processed": 0, "pct": None, "status": "skipped",
            }
            return

        if self.dry_run:
            logger.info("[DRY RUN] would run scraper %s (~%d min)",
                        name, spec["est_minutes"])
            return

        cls = _try_import_scraper(spec)
        if cls is None:
            # Module unavailable — record as a real error so the run fails loudly.
            self.errors.append(f"{name}: module unavailable")
            self.stage_failures.append(f"scrape:{name}")
            self.scraper_completion[name] = {
                "expected": None, "processed": 0, "pct": 0.0, "status": "module_missing",
            }
            emit_alert(
                severity="error",
                source="daily_pipeline",
                message=f"Scraper module unavailable: {name}",
                context={"run_id": self.run_id, "date": self.date, "scraper": name},
            )
            return

        logger.info("Running scraper: %s", name)
        instance = cls()

        # Try common entry points in priority order.
        # `scrape_all_cards` is the convention used by PriceCharting / 130point
        # (per-card scrapers). `collect` is used by TCGPlayer (per-set). The
        # earlier names are kept as fallbacks for any future scraper.
        for method_name in ("run", "scrape_all_cards", "collect", "scrape", "execute"):
            method = getattr(instance, method_name, None)
            if callable(method):
                result = method(self.date) if _accepts_arg(method) else method()
                self._accumulate_result(name, result)
                return
        raise RuntimeError(
            f"Scraper {name} has no run/scrape/collect/execute method"
        )

    def _accumulate_result(self, source: str, result: Any) -> None:
        """Record per-source completion stats + accumulate totals.

        Scrapers are encouraged to return a dict like:
            {"processed": 915, "expected": 8535}
        If only 'processed' is present, we record that (pct=None).
        """
        processed = 0
        expected: Optional[int] = None
        if isinstance(result, dict):
            processed = int(result.get("processed", 0) or 0)
            exp_val = result.get("expected")
            if exp_val is not None:
                try:
                    expected = int(exp_val)
                except (TypeError, ValueError):
                    expected = None

        self.cards_processed += processed
        pct: Optional[float] = None
        if expected and expected > 0:
            pct = round(100.0 * processed / expected, 2)

        self.scraper_completion[source] = {
            "expected": expected,
            "processed": processed,
            "pct": pct,
            "status": "ok",
        }

        # Alert when completion is visibly broken (matches "915/8535" symptom).
        if pct is not None and pct < 50.0:
            emit_alert(
                severity="warn",
                source="daily_pipeline",
                message=f"Scraper {source} low completion: {processed}/{expected} ({pct}%)",
                context={"run_id": self.run_id, "date": self.date, "scraper": source},
            )

    def _stage_scrape(self) -> None:
        """Stage 1-3: run scrapers concurrently across independent domains.

        Each scraper hits a different host (pricecharting / 130point / tcgplayer),
        so per-domain rate limits are unaffected. WAL-mode SQLite + fresh
        connections per get_db() call handle concurrent writers.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        jobs = [
            ("scrape:pricecharting", SCRAPER_SPECS[0]),
            ("scrape:onethirty_point", SCRAPER_SPECS[1]),
            ("scrape:tcgplayer", SCRAPER_SPECS[2]),
        ]

        with ThreadPoolExecutor(max_workers=len(jobs), thread_name_prefix="scrape") as pool:
            futures = {
                pool.submit(self._run_stage, stage_name, lambda s=spec: self._run_scraper(s)): stage_name
                for stage_name, spec in jobs
            }
            for fut in as_completed(futures):
                fut.result()

    # -------------------- transformers --------------------

    def _stage_transform(self) -> None:
        """Stage 4: run transformers (interpolation only)."""
        if self.dry_run:
            self._run_stage("transform:interpolation",
                            lambda: logger.info("[DRY RUN] would interpolate price_history"))
            return

        card_ids = _all_card_ids()
        logger.info("Running transformers across %d cards", len(card_ids))

        self._run_stage(
            "transform:interpolation",
            lambda: _apply_card_transformer(
                "pipeline.transformers.interpolation",
                "interpolate_price_history",
                card_ids,
            ),
        )

    # -------------------- computes --------------------

    def _stage_compute(self) -> None:
        """Stage 5: run computes (ev, pack_cost, market_pressure, leaderboard)."""
        if self.dry_run:
            for step in ("ev_calculator", "pack_cost", "market_pressure", "leaderboard"):
                self._run_stage(
                    f"compute:{step}",
                    lambda s=step: logger.info("[DRY RUN] would run compute.%s", s),
                )
            return

        set_codes = _all_set_codes()
        card_ids = _all_card_ids()

        self._run_stage(
            "compute:ev_calculator",
            lambda: _apply_set_compute(
                "pipeline.compute.ev_calculator",
                "compute_ev_for_set",
                set_codes,
                self.date,
            ),
        )
        self._run_stage(
            "compute:pack_cost",
            lambda: _apply_set_compute(
                "pipeline.compute.pack_cost",
                "compute_pack_cost",
                set_codes,
                self.date,
            ),
        )
        self._run_stage(
            "compute:market_pressure",
            lambda: _apply_card_transformer(
                "pipeline.compute.market_pressure",
                "compute_market_pressure",
                card_ids,
            ),
        )
        self._run_stage(
            "compute:leaderboard",
            lambda: _apply_global_compute(
                "pipeline.compute.leaderboard",
                "compute_leaderboard",
                self.date,
            ),
        )

    # -------------------- prediction --------------------

    def _stage_predict(self) -> None:
        """Stage 6: generate model projections for all active cards."""
        if self.dry_run:
            self._run_stage("predict:projections",
                            lambda: logger.info("[DRY RUN] would generate model projections"))
            return

        def _run_predict():
            try:
                from pipeline.model.predict import generate_projections
            except ImportError as exc:
                logger.warning("Model module not available: %s", exc)
                self.errors.append(f"predict: model module unavailable ({exc})")
                return
            with get_db() as db:
                result = generate_projections(db)
                logger.info("Projections: %s", result)

        self._run_stage("predict:projections", _run_predict)

        # After projections land, lock them into paper_trades (with cohort
        # tags) and evaluate any trades that have reached T+90.
        def _run_paper_trade():
            try:
                from pipeline.model import paper_trade
            except ImportError as exc:
                logger.warning("paper_trade module not available: %s", exc)
                self.errors.append(f"paper_trade: module unavailable ({exc})")
                return
            with get_db() as db:
                result = paper_trade.run_daily(db)
                logger.info("paper_trade: %s", result)

        self._run_stage("predict:paper_trade", _run_paper_trade)

    # -------------------- main run --------------------

    def run(self) -> int:
        """Execute the pipeline. Returns an exit code (0 ok, 1 partial, 2 fatal)."""
        log_file = LOG_DIR / f"daily_{self.date}.log"
        _configure_logging(log_file)

        stage_label = f"daily:{self.stage}"
        self._start_run(stage_label)
        run_start = time.time()

        print(f"=== Pokemon Analytics Daily Pipeline ===")
        print(f"    date:    {self.date}")
        print(f"    stage:   {self.stage}")
        print(f"    skip:    {self.skip or '(none)'}")
        print(f"    dry_run: {self.dry_run}")
        print(f"    log:     {log_file}")

        try:
            if self.stage in (self.STAGE_ALL, self.STAGE_SCRAPE):
                self._stage_scrape()

            if self.stage in (self.STAGE_ALL, self.STAGE_TRANSFORM):
                self._stage_transform()

            if self.stage in (self.STAGE_ALL, self.STAGE_COMPUTE):
                self._stage_compute()

            if self.stage in (self.STAGE_ALL, self.STAGE_PREDICT):
                self._stage_predict()

            total_secs = time.time() - run_start
            print(f"\n=== Pipeline finished in {total_secs:.1f}s ===")
            print(f"    errors:         {len(self.errors)}")
            print(f"    stage failures: {len(self.stage_failures)}")
            for err in self.errors:
                print(f"    - {err}")
            if self.scraper_completion:
                print(f"    scraper completion:")
                for src, stats in self.scraper_completion.items():
                    print(f"      - {src}: {stats}")

            # Honest status: any stage failure => FAILED, not "done".
            if self.stage_failures:
                self._finish_run("failed", f"failed_stages={','.join(self.stage_failures)}")
                emit_alert(
                    severity="error",
                    source="daily_pipeline",
                    message=f"Pipeline run {self.run_id} finished FAILED with "
                            f"{len(self.stage_failures)} failed stage(s)",
                    context={
                        "run_id": self.run_id,
                        "date": self.date,
                        "failed_stages": self.stage_failures,
                        "errors": self.errors,
                        "scraper_completion": self.scraper_completion,
                    },
                )
                return 1
            if self.errors:
                # Soft errors (warnings captured without stage failure).
                self._finish_run("done_with_errors")
                return 1
            self._finish_run("done")
            return 0

        except Exception as exc:  # noqa: BLE001
            logger.exception("Fatal pipeline error")
            self.errors.append(f"fatal: {exc}")
            self._finish_run("failed", f"fatal={exc}")
            emit_alert(
                severity="fatal",
                source="daily_pipeline",
                message=f"Fatal pipeline error: {exc.__class__.__name__}: {exc}",
                context={
                    "run_id": self.run_id,
                    "date": self.date,
                    "traceback": traceback.format_exc(),
                },
            )
            return 2


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _accepts_arg(func: Callable[..., Any]) -> bool:
    """Return True if func accepts at least one positional argument."""
    import inspect
    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError):
        return True  # assume yes
    params = [
        p for p in sig.parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    return len(params) >= 1


def _all_card_ids() -> List[str]:
    with get_db() as db:
        rows = db.execute("SELECT id FROM cards").fetchall()
    return [r["id"] for r in rows]


def _all_set_codes() -> List[str]:
    with get_db() as db:
        rows = db.execute("SELECT set_code FROM sets").fetchall()
    return [r["set_code"] for r in rows]


def _apply_card_transformer(module_path: str, func_name: str, card_ids: List[str]) -> None:
    """Import a transformer and run it once per card_id inside a single db connection."""
    mod = importlib.import_module(module_path)
    func = getattr(mod, func_name)

    ok = 0
    failed = 0
    with get_db() as db:
        for cid in card_ids:
            try:
                func(db, cid)
                ok += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                logger.warning("%s(%s) failed: %s", func_name, cid, exc)
    logger.info("%s: %d ok, %d failed", func_name, ok, failed)


def _apply_set_compute(module_path: str, func_name: str,
                        set_codes: List[str], date: str) -> None:
    mod = importlib.import_module(module_path)
    func = getattr(mod, func_name)
    ok = 0
    failed = 0
    with get_db() as db:
        for code in set_codes:
            try:
                func(db, code, date)
                ok += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                logger.warning("%s(%s) failed: %s", func_name, code, exc)
    logger.info("%s: %d ok, %d failed", func_name, ok, failed)


def _apply_global_compute(module_path: str, func_name: str, date: str) -> None:
    mod = importlib.import_module(module_path)
    func = getattr(mod, func_name)
    with get_db() as db:
        func(db, date)
    logger.info("%s: completed", func_name)


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="daily_pipeline",
        description="Run the Pokemon Analytics daily pipeline.",
    )
    p.add_argument("--date", default=None,
                   help="Target date YYYY-MM-DD (default: today)")
    p.add_argument("--stage",
                   choices=["all", "scrape", "transform", "compute", "predict"],
                   default="all",
                   help="Which stage group to run (default: all)")
    p.add_argument("--skip", action="append", default=[],
                   help="Scraper name to skip (repeatable). "
                        "Options: pricecharting, onethirty_point, tcgplayer")
    p.add_argument("--dry-run", action="store_true",
                   help="Log what would happen; don't hit network or write to DB")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    pipe = DailyPipeline(
        date=args.date,
        stage=args.stage,
        skip=args.skip,
        dry_run=args.dry_run,
    )
    return pipe.run()


if __name__ == "__main__":
    sys.exit(main())
