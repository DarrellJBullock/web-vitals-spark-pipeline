# Architecture

## Overview

This is a batch PySpark pipeline that ingests four raw data sources, normalizes
them into a common shape, joins/aggregates them into curated analytics tables,
detects release-over-release performance regressions, and exports
dashboard-ready CSV + a markdown summary report.

Every stage is a plain PySpark job invoked as `python -m src.jobs.<job>`. Jobs
are intentionally decoupled: each one reads its inputs from a stable layer on
disk (raw or processed Parquet) rather than depending on another job having
just run in the same process. That mirrors how a real orchestrator (Airflow,
Dagster, a scheduled Databricks job) would wire this up -- each task is
independently retryable and re-runnable.

## Data flow

```
                      ┌─────────────────────┐
                      │   data/raw/          │
                      │  lighthouse/*.json   │  (one JSON array per day)
                      │  crux/*.csv          │  (daily field-metric export)
                      │  page_load_logs/*.jsonl │ (one JSON object per line/event)
                      │  devices/devices.json│  (static dimension table)
                      └─────────┬───────────┘
                                │
      ┌─────────────────────────┼─────────────────────────┐
      │                         │                          │
      ▼                         ▼                          ▼
ingest_lighthouse.py     ingest_crux.py            ingest_page_logs.py
(stage 1, 5-8)           (stage 2, 5-8)             (stages 3-8)
      │                         │                          │
      ▼                         ▼                          ▼
data/processed/           data/processed/            data/processed/
  lighthouse/                crux/                    page_logs/  + devices/
(route/device        (route/device standardized,   (route/device standardized,
standardized,           quality-checked)              joined to devices for
production-only,                                       device_class/network_profile,
quality-checked)                                        quality-checked)
      │                                                       │
      └───────────────────────┬───────────────────────────────┘
                               ▼
                build_core_web_vitals_tables.py  (stages 9-10)
                               │
        ┌──────────────┬───────┴────────┬────────────────────┐
        ▼              ▼                ▼                    ▼
 core_web_vitals_  route_performance_ device_        before_after_
   daily              rankings        breakdowns      comparisons
        │              │                │                    │
        └──────────────┴────────────────┴────────────────────┘
                               │
                    detect_regressions.py
                               │
                       regression_events
                               │
                    generate_reports.py
                               │
              ┌────────────────┼─────────────────┬───────────────────┐
              ▼                ▼                 ▼                   ▼
   route_rankings.csv  regression_events.csv  before_after_    device_breakdown.csv
                                                comparison.csv
                               │
                    web_vitals_summary.md
```

## Pipeline stages

| # | Stage | Where |
|---|-------|-------|
| 1 | Read raw Lighthouse JSON reports | `src/jobs/ingest_lighthouse.py` |
| 2 | Read raw CrUX-like CSV field metrics | `src/jobs/ingest_crux.py` |
| 3 | Read raw page load logs (JSONL) | `src/jobs/ingest_page_logs.py` |
| 4 | Read raw device/network metadata | `src/jobs/ingest_page_logs.py` |
| 5 | Standardize route names | `src/transforms/normalization.py` |
| 6 | Standardize device type (mobile/desktop/tablet) | `src/transforms/normalization.py` |
| 7 | Normalize Core Web Vitals fields (types, timestamps) | `src/transforms/{lighthouse,crux,page_log}_transforms.py` |
| 8 | Apply data quality checks | `src/quality/data_quality_checks.py` |
| 9 | Build curated tables | `src/jobs/build_core_web_vitals_tables.py`, `src/transforms/vitals_transforms.py` |
| 10 | Create analytics outputs (regressions + reports) | `src/jobs/detect_regressions.py`, `src/jobs/generate_reports.py` |

## Key design decisions

- **Page-load logs are the "field data" source of truth for curated tables.**
  The CrUX-like dataset is ingested and quality-checked (it's a realistic
  secondary field-metrics source real teams also have), but it has no
  `release_version` column -- by definition CrUX is origin-level aggregate
  data with no concept of your internal release cadence. Since
  `core_web_vitals_daily` needs to slice by release, it's built from the
  synthetic page-load logs instead, which stand in for RUM (Real User
  Monitoring) with release attribution.
- **Lighthouse (lab) and field data are blended in `route_performance_rankings`.**
  `performance_score` comes from Lighthouse's own 0-100 lab score; the p75
  LCP/CLS/INP columns come from field data. This mirrors how frontend teams
  actually triage: lab scores catch regressions before they ship, field data
  confirms real-world impact.
- **Releases are ordered by first-seen date, not by parsing version strings.**
  `regression_transforms.build_release_aggregates/build_release_pairs` ranks
  releases by `min(event_date)` per route/device. This works regardless of
  whether you version with semver, dates, or git SHAs.
- **A regression event requires an actual change, not just being chronically bad.**
  Each rule fires on relative worsening (e.g. LCP p75 up >20%) OR *newly*
  crossing the absolute "poor" threshold (was acceptable, is now poor). A page
  that's been slow for three releases straight and hasn't gotten worse is a
  standing risk (visible via `risk_level` in rankings/device breakdowns), not
  a fresh regression event every time the job runs.
- **Every job pins `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to `sys.executable`**
  (`src/utils/spark.py`) so Spark's worker subprocesses always match the
  driver's interpreter, regardless of what `python3` resolves to on `PATH`.
- **Spark session timezone is pinned to UTC.** All raw timestamps are
  generated/ingested in UTC; without pinning `spark.sql.session.timeZone`,
  `to_date()` would extract calendar dates using the host machine's local
  timezone and silently shift some events across a day boundary.
- **Curated tables are Parquet; only the final reports are CSV.** This keeps
  the curated layer efficient and schema-enforced for downstream dashboard
  tools, while `generate_reports.py` flattens the specific cuts a
  frontend/analytics stakeholder wants into single, portfolio-readable CSV
  files (`write_single_csv` coalesces Spark's multi-part output into one file).

## Idempotency & re-runs

Every job writes with `.mode("overwrite")`, so re-running any stage (or the
whole `make run`) is safe and produces the same output given the same input.
There's no incremental/append logic in this version -- see the roadmap in the
README for what a streaming or incremental-batch version would add.
