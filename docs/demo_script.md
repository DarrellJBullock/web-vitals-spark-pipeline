# Demo Script

A walkthrough for showing this project live (interview, portfolio review, or
just convincing yourself it works after a fresh clone).

## 0. Setup (~2 min)

```bash
python3 -m venv .venv && source .venv/bin/activate
make install
```

You need a JVM for PySpark. If `java -version` fails, either install one
(`brew install openjdk@17` on macOS, then `export JAVA_HOME=$(brew --prefix openjdk@17)`)
or run everything through Docker instead (`docker compose run --rm pipeline make run`).

## 1. Generate sample data (~5 sec)

```bash
make seed
```

Narrate: *"This generates 21 days of realistic Lighthouse reports, CrUX-like
field metrics, page-load logs, and device metadata for a small e-commerce
site across 6 routes, 3 device types, and 3 releases. Two regressions and one
fix are deliberately seeded so the detection logic has real signal to find."*

```bash
ls data/raw/lighthouse | head -3
cat data/raw/devices/devices.json | python -m json.tool | head -20
```

## 2. Run the pipeline (~30-60 sec)

```bash
make run
```

This runs, in order: `ingest_lighthouse`, `ingest_crux`, `ingest_page_logs`,
`build_core_web_vitals_tables`, `detect_regressions`, `generate_reports`.
Watch the log lines -- each job reports how many rows it read, how many
quality checks dropped, and how many it wrote.

Point out: `[lighthouse] quality checks passed on all 359 rows` after
`Read 378 raw Lighthouse records` -- 19 non-production (staging/dev) reports
were filtered by the normalization stage, and that's log-visible, not silent.

## 3. Show the regression detection story (~2 min)

```bash
column -s, -t reports/regression_events.csv | less -S
```

Narrate the seeded story:
- `/checkout` LCP + CLS regress hard in `v2.4.0` (critical) -- an oversized
  hero carousel shipped.
- `/account` TTFB regresses in `v2.4.0` on every device (critical) -- a slow
  backend dependency.
- `/search` mobile INP regresses in `v2.5.0` (critical) -- an unthrottled
  type-ahead, still live at the end of the window.

Then open `reports/before_after_comparison.csv` and filter to `/checkout` to
show `v2.4.0 -> v2.5.0` flip to `improved` -- the carousel got fixed.

## 4. Show the summary report (~2 min)

```bash
cat reports/web_vitals_summary.md
```

This is the artifact a frontend team would actually read: best/worst routes,
biggest regressions, mobile-vs-desktop health gap (mobile consistently scores
lower -- worth calling out explicitly, since it's realistic and matches what
most teams see in production).

## 5. Show the tests (~1 min)

```bash
make test
```

21 tests covering: route/device-type normalization, Lighthouse parsing,
regression detection + severity, before/after classification, health-score
math, risk-level classification, and every data quality check. Point out
that the transform functions take/return plain Spark DataFrames -- no
job-level side effects -- which is what makes them unit-testable at all.

## 6. Optional: open the notebook

```bash
jupyter notebook notebooks/web_vitals_analysis.ipynb
```

Ad-hoc exploration on top of the curated Parquet tables -- the kind of
follow-up analysis a data engineer would do after the batch jobs land.

## Talking points if asked "why PySpark for this data volume?"

Be upfront: the sample dataset (a few thousand rows) doesn't *need* Spark.
The point of this project is to demonstrate the engineering pattern --
DataFrame/Spark SQL transforms, partitioned Parquet, window functions for
ranking, quality gates, idempotent batch jobs -- at a scale where the code
would not need to change if `data/raw/` had 500M rows instead of 8K. That's
the difference between a notebook script and a pipeline.
