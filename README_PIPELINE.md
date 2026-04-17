# Pokemon Analytics :: Pipeline

This document describes the daily and weekly data pipelines that collect, transform, and compute the analytics that power the Pokemon Analytics site.

## What the pipeline does

```
[scrapers] -> [transformers] -> [computes] -> [database]
```

### Stage 1-3: Scrapers

| Scraper            | Source                    | Purpose                          | Est. Duration |
| ------------------ | ------------------------- | -------------------------------- | ------------- |
| `pricecharting`    | pricecharting.com         | Primary daily raw/graded prices  | ~5 hours      |
| `onethirty_point`  | 130point.com              | eBay sold listings (graded)      | ~25 min       |
| `tcgplayer`        | tcgplayer.com             | NM market prices (async)         | ~15 min       |
| `psa_pop` (weekly) | psacard.com (20 set pgs)  | PSA population reports           | ~30 min       |

Scrapers are resilient: if a scraper module is not installed yet (another agent may still be building it), the orchestrator logs a warning and continues.

### Stage 4: Transformers

Run once per card on the live database connection:

- `transformers.interpolation` -- linear-fill small gaps in `price_history`
- `transformers.ebay_derived` -- outlier-adjusted + 7-day EMA smoothing over eBay ended-auction averages
- `transformers.composite_price` -- blend PriceCharting + eBay + JustTCG into `composite_history`

### Stage 5: Computes

Run per set or globally:

- `compute.ev_calculator` -- Expected Value per pack by rarity (per set)
- `compute.pack_cost` -- derive avg pack cost from sealed product prices (per set)
- `compute.market_pressure` -- demand/supply pressure + saturation (per card)
- `compute.leaderboard` -- rank all sets across 4 dimensions (global)

All stage transitions are recorded in the `pipeline_runs` table.

---

## How to run it manually

From the project root (wherever you cloned this repo — the scripts resolve their own location, no hardcoded paths):

```bash
# Full daily run for today
python3 -m pipeline.daily_pipeline

# Run for a specific backfill date
python3 -m pipeline.daily_pipeline --date 2026-04-09

# Only run scrapers
python3 -m pipeline.daily_pipeline --stage scrape

# Only run the compute stage (e.g. after fixing a transformer bug)
python3 -m pipeline.daily_pipeline --stage compute

# Only run the transform stage
python3 -m pipeline.daily_pipeline --stage transform

# Skip a specific scraper (repeatable)
python3 -m pipeline.daily_pipeline --skip pricecharting
python3 -m pipeline.daily_pipeline --skip pricecharting --skip tcgplayer

# Dry run -- log what would happen, don't touch the database
python3 -m pipeline.daily_pipeline --dry-run
```

### Weekly

```bash
# Full weekly: PSA pop + daily sequence
python3 -m pipeline.weekly_pipeline

# Backfill a specific week
python3 -m pipeline.weekly_pipeline --date 2026-04-12
```

### Via the wrapper (what cron invokes)

```bash
./scripts/run_pipeline.sh daily
./scripts/run_pipeline.sh weekly
./scripts/run_pipeline.sh daily --stage compute
```

The wrapper activates `.venv/` if present, tees stdout/stderr to a timestamped log, and prints a failure banner on non-zero exit.

---

## How to install cron

```bash
./scripts/install_cron.sh            # install
./scripts/install_cron.sh --dry-run  # preview the entries that would be installed
```

Idempotent — safe to re-run. The script resolves the project dir from its own location, so there are no hardcoded paths. The installed entries look like:

```
0 4 * * * <PROJECT>/scripts/run_daily.sh                 >> data/logs/cron_daily.log  2>&1   # pokemon-analytics:daily
0 2 * * 0 <PROJECT>/scripts/run_pipeline.sh weekly       >> data/logs/cron_weekly.log 2>&1   # pokemon-analytics:weekly
```

- Daily  : every day at 04:00 local time (via `scripts/run_daily.sh`)
- Weekly : Sunday at 02:00 local time (via `scripts/run_pipeline.sh weekly`)

To inspect:

```bash
crontab -l | grep pokemon-analytics
```

To remove:

```bash
crontab -l | grep -v "pokemon-analytics" | crontab -
```

---

## Where logs are written

All logs land under `data/logs/`:

| File                           | Written by                       |
| ------------------------------ | -------------------------------- |
| `daily_YYYY-MM-DD.log`         | `DailyPipeline` (Python logging) |
| `weekly_YYYY-MM-DD.log`        | `WeeklyPipeline`                 |
| `run_daily_YYYY-MM-DD.log`     | `run_pipeline.sh daily`          |
| `run_weekly_YYYY-MM-DD.log`    | `run_pipeline.sh weekly`         |
| `cron_daily.log`               | cron stdout/stderr redirect      |
| `cron_weekly.log`              | cron stdout/stderr redirect      |

---

## How to monitor the `pipeline_runs` table

Every run inserts a row on start and updates it on finish:

```sql
SELECT id, started_at, finished_at, status, stage, cards_processed, errors, notes
  FROM pipeline_runs
 ORDER BY id DESC
 LIMIT 20;
```

Status values:

- `running`            -- in progress
- `done`               -- completed cleanly, zero stage failures
- `done_with_errors`   -- no stage raised an exception but soft warnings exist
- `failed`             -- one or more stages raised an exception, or a fatal error occurred

`failed` is now the honest default whenever any stage exception is caught. Previously the orchestrator would catch and mark `done` even if a stage silently blew up; that bug is fixed — every stage exception is logged with full traceback, written to `data/logs/alerts.jsonl`, and forces the run into `failed`.

### Per-source scrape completion

Each run also records a JSON blob in `pipeline_runs.scraper_completion_json` with the shape:

```json
{
  "pricecharting":     {"expected": 8535, "processed": 915, "pct": 10.72, "status": "ok"},
  "onethirty_point":   {"expected": 8535, "processed": 8520, "pct": 99.82, "status": "ok"},
  "tcgplayer":         {"expected": null, "processed": 0,    "pct": null, "status": "skipped"}
}
```

Any source with `pct < 50` auto-fires a warn alert. To enable the column, run once:

```bash
python3 scripts/migrate_ops_columns.py
```

This is an idempotent `ALTER TABLE ADD COLUMN` — safe to re-run.

### Alerting

Failures are written to `data/logs/alerts.jsonl` (one JSON object per line) and, when `ALERT_WEBHOOK_URL` is set in the environment, POSTed to that URL. There is no hard dependency on any external service.

```bash
# Quick tail of recent alerts:
tail -n 20 data/logs/alerts.jsonl | jq .
```

The `stage` column is updated live as the run progresses through sub-stages (e.g. `scrape:pricecharting`, `transform:composite_price`, `compute:leaderboard`), so you can see where a long-running job is at by polling this table.

Quick health check query:

```sql
SELECT date(started_at) AS day,
       stage,
       status,
       cards_processed,
       errors
  FROM pipeline_runs
 WHERE started_at >= date('now', '-14 days')
 ORDER BY started_at DESC;
```

---

## How to backfill historical data

1. **Pick a target date** -- say the scrapers missed `2026-04-05`.
2. **Run the daily pipeline with `--date`**:
    ```bash
    python3 -m pipeline.daily_pipeline --date 2026-04-05
    ```
3. **For a range**, loop in bash:
    ```bash
    for d in $(python3 -c "
    import datetime
    for i in range(7):
        print((datetime.date(2026,4,1) + datetime.timedelta(days=i)).isoformat())
    "); do
        python3 -m pipeline.daily_pipeline --date "$d"
    done
    ```
4. **Skip expensive scrapers during a compute-only backfill**:
    ```bash
    python3 -m pipeline.daily_pipeline --date 2026-04-05 --stage compute
    ```
    This re-runs the transformers + computes against whatever is already in the database, which is fast (seconds to a minute).
5. **Verify with pipeline_runs**:
    ```sql
    SELECT * FROM pipeline_runs
     WHERE notes LIKE '%date=2026-04-05%'
     ORDER BY id DESC;
    ```

---

## Troubleshooting

- **"Scraper module ... not found -- skipping"**
  One of the scraper modules has not been built yet. The orchestrator logs a warning and continues. Install / implement the missing scraper and re-run with `--stage scrape` to catch up.
- **Stage failed but pipeline kept going**
  By design, each stage is wrapped in try/except so a single failure does not take down the whole run. Check `pipeline_runs.notes` and the daily log file for details.
- **Cron job not firing**
  Check `crontab -l`, make sure `scripts/run_pipeline.sh` is executable (`chmod +x`), and confirm `data/logs/cron_daily.log` is writable.
