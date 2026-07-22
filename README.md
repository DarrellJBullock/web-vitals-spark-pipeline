# Core Web Vitals Data Pipeline

**web-vitals-spark-pipeline** -- a production-style PySpark data engineering
pipeline that analyzes frontend performance at scale.

## Portfolio angle

Built a PySpark pipeline to analyze frontend performance at scale: ingesting
Lighthouse reports, CrUX-like field metrics, browser page-load logs, and
device metadata, then modeling them into curated Spark SQL analytics tables
that detect route-level regressions, trends, and mobile-vs-desktop gaps.

## Problem being solved

A frontend engineering team ships weekly and has no systematic way to answer:
*Did the last release make anything slower? Which routes are worst on mobile?
Is this regression new, or has this page always been slow?* Lighthouse runs
and RUM/CrUX data pile up in disconnected dashboards with no route-level,
release-aware trend analysis. This pipeline turns that raw exhaust into
curated tables built for exactly those questions -- trend analysis,
before/after release comparison, automatic regression detection with severity
and recommended actions, and mobile/desktop/device-class breakdowns.

## Why Core Web Vitals matter

LCP, CLS, and INP are Google's user-experience metrics and a confirmed
ranking signal; TTFB and FCP gate how fast they can even start improving.
Regressions in these metrics translate directly to bounce rate, conversion
rate, and SEO ranking -- but they're invisible without a systematic pipeline
tracking them **per route, per device, per release**, not just as a single
site-wide average that hides where the actual problem is.

## Tech stack

Python * PySpark * Spark SQL * Parquet * CSV/JSON * pytest * Docker (optional)
* Makefile (optional) * Jupyter (optional) * FastAPI (optional, stretch goal)

This is a **PySpark-first** project: every transform is a Spark DataFrame /
Spark SQL operation (window functions, `groupBy` aggregations,
`percentile_approx`, joins). There is no pandas anywhere in the pipeline
itself -- pandas only shows up, optionally, inside the exploratory notebook.

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full diagram and
design-decision writeup. Short version:

```
raw/ (JSON, CSV, JSONL) --> ingest_* jobs --> processed/ (Parquet)
    --> build_core_web_vitals_tables.py --> curated/ (Parquet)
    --> detect_regressions.py --> curated/regression_events
    --> generate_reports.py --> reports/ (CSV + markdown)
```

## Pipeline stages

1. Read raw Lighthouse JSON reports
2. Read raw CrUX-like CSV field metrics
3. Read raw synthetic page-load logs
4. Read raw device/network metadata
5. Standardize route names
6. Standardize device type (mobile / desktop / tablet)
7. Normalize Core Web Vitals fields
8. Apply data quality checks
9. Build curated analytics tables
10. Create analytics outputs (regressions + reports)

## Data model

Full column-by-column schemas for every raw and curated table are in
[`docs/data_model.md`](docs/data_model.md). The five curated tables:

| Table | Grain |
|---|---|
| `core_web_vitals_daily` | date x route x device_type x release_version |
| `route_performance_rankings` | date x route x device_type, ranked within date+device |
| `before_after_comparisons` | route x device_type x consecutive release pair |
| `regression_events` | route x device_type x metric x release transition (only breaches) |
| `device_breakdowns` | date x route x device_type x device_class x connection_type |

## How regression detection works

For each route/device, releases are paired consecutively (ordered by
first-seen date, not by parsing version strings). A metric is flagged as
regressed when **either**:

- it worsens by more than its percent threshold vs. the prior release, **or**
- it *newly* crosses its absolute "poor" threshold (was acceptable before,
  is poor now -- a page that's been chronically bad and unchanged is a
  standing risk, not a fresh regression).

| Metric | Regression rule |
|---|---|
| LCP | p75 increases >20% vs. baseline, or newly exceeds 2500ms |
| CLS | p75 increases by more than 0.05, or newly exceeds 0.1 |
| INP | p75 increases >20% vs. baseline, or newly exceeds 200ms |
| TTFB | p75 increases >25% vs. baseline, or newly exceeds 800ms |

Each flagged event gets a **severity** (low/medium/high/critical, from how
far past the trigger it is) and a static **probable cause + recommended
action** per metric (e.g. CLS -> "check late-loading images/ads without
reserved space"). See `src/transforms/regression_transforms.py`.

## How before-and-after comparisons work

Same consecutive-release pairing as regression detection, but every pair is
reported (not just breaches), with an `improvement_status` of `regressed`
(same rule as above), `improved` (LCP or INP improved >=10%, or CLS improved
by >=0.02, with no regression), or `stable`.

## How mobile vs. desktop breakdown works

`core_web_vitals_daily` and `route_performance_rankings` are both grained by
`device_type`, so every trend/ranking query can slice mobile vs. desktop vs.
tablet directly. `device_breakdowns` goes one level deeper: it joins
page-load events to the device dimension table (`device_class`:
low-end/mid-range/high-end) and `connection_type`, so you can see, e.g.,
whether a route's mobile problem is really a *low-end-device-on-3G* problem.

## How to run locally

Requires Python 3.11+ and a JVM (PySpark needs Java 8/11/17). If you don't
have Java installed:

```bash
# macOS
brew install openjdk@17
export JAVA_HOME=$(brew --prefix openjdk@17)
```

Then:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python -m scripts.generate_sample_data   # writes realistic sample data to data/raw/
python -m src.jobs.ingest_lighthouse
python -m src.jobs.ingest_crux
python -m src.jobs.ingest_page_logs
python -m src.jobs.build_core_web_vitals_tables
python -m src.jobs.detect_regressions
python -m src.jobs.generate_reports
```

Or with the Makefile:

```bash
make install
make seed
make run
```

No Java on your machine? Run it in Docker instead (bundles a JVM):

```bash
docker compose run --rm pipeline make run
```

### Config

All paths and thresholds are environment-driven (see `.env.example`) -- copy
it to `.env` to override anything (e.g. a different `DATA_ROOT`, or looser
regression thresholds). Nothing in the codebase hardcodes an absolute path.

## How to run tests

```bash
pytest
# or
make test
```

21 tests across `tests/`, covering: Lighthouse report parsing, route
normalization, device-type normalization, regression detection + severity
classification, before/after status classification, health-score math,
risk-level classification, and every data quality check
(`test_lighthouse_transforms.py`, `test_regression_detection.py`,
`test_vitals_transforms.py`, `test_data_quality_checks.py`).

## Sample output

### `reports/web_vitals_summary.md` (excerpt)

```markdown
## Biggest Regressions

| Route | Device | Metric | Release | Severity | Baseline | Current | Delta % |
| --- | --- | --- | --- | --- | --- | --- | --- |
| /checkout | mobile | CLS | v2.4.0 | critical | 0.0651 | 0.1808 | 177.73% |
| /account | tablet | TTFB | v2.4.0 | critical | 510.0 | 965.0 | 89.22% |
| /search | mobile | INP | v2.5.0 | critical | 259.0 | 417.0 | 61.0% |

## Mobile vs. Desktop vs. Tablet Findings

| Device Type | Avg Health Score | Avg p75 LCP (ms) | % Good LCP |
| --- | --- | --- | --- |
| desktop | 66.6 | 1782.0 | 98.3% |
| tablet | 66.4 | 2095.0 | 92.7% |
| mobile | 63.2 | 2988.0 | 49.2% |
```

The full report (best/worst routes, biggest improvements, recommended
actions) is generated fresh every run at `reports/web_vitals_summary.md`.
Screenshots of the CSV outputs opened in a spreadsheet/BI tool go here for
a portfolio site: `docs/screenshots/` (add your own after running the demo).

## Sample data & the regression story

`scripts/generate_sample_data.py` seeds 21 days of data across 6 routes, 3
device types, and 3 releases (`v2.3.0` -> `v2.4.0` -> `v2.5.0`), with two
deliberate regressions and one fix baked in so detection has real signal:

- **`/checkout` LCP + CLS regress in `v2.4.0`** (an oversized hero carousel
  ships), then **recover past the original baseline in `v2.5.0`** (lazy
  loading fixes it) -- a clean "introduced then fixed" story.
- **`/account` TTFB regresses in `v2.4.0`** (a slow backend dependency),
  recovers in `v2.5.0`.
- **`/search` mobile INP regresses in `v2.5.0`** (an unthrottled type-ahead)
  and is still live at the end of the window -- an active, unresolved
  regression.

## Future roadmap

- Incremental/append ingestion instead of full-overwrite batch (watermarking,
  merge-on-write to curated tables).
- A FastAPI endpoint (`GET /summary`, `/rankings`, `/regressions`) serving the
  curated Parquet tables directly -- stubbed out as a stretch goal, not
  blocking the core pipeline.
- A small Next.js dashboard consuming that API for live charts instead of
  static CSV/markdown.
- Real Lighthouse CI / CrUX BigQuery export integration in place of the
  synthetic generators.
- Alerting (Slack/email) wired to `regression_events` for `critical` severity.

## Resume bullet

> Built a PySpark data pipeline to process Lighthouse reports, CrUX-like
> metrics, page load logs, and device data, producing route-level Core Web
> Vitals trends, regression detection, and mobile vs desktop performance
> analytics.

## Project structure

```
web-vitals-spark-pipeline/
  README.md
  requirements.txt
  Makefile
  docker-compose.yml / Dockerfile
  data/{raw,processed,curated}/
  reports/
  scripts/generate_sample_data.py
  src/
    config/settings.py
    jobs/            # the 6 runnable pipeline stages
    transforms/       # pure, unit-testable Spark DataFrame transforms
    quality/data_quality_checks.py
    utils/{spark,paths,logging}.py
  tests/
  notebooks/web_vitals_analysis.ipynb
  docs/{architecture,data_model,demo_script}.md
```
