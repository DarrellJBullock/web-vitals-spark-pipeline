# Data Model

## Raw layer (`data/raw/`)

### `lighthouse/lighthouse_report_YYYY-MM-DD.json`

One JSON array per day of Lighthouse CI-style lab runs (one run per
route x device_type). Batched-per-day is a deliberate simplification of real
Lighthouse CI exports (which are usually one file per run) so the sample data
is easy to browse in a repo.

| Column | Type | Notes |
|---|---|---|
| report_id | string | |
| run_timestamp | ISO8601 string (UTC) | |
| url | string | |
| route | string | raw, pre-normalization |
| device_type | string | raw, pre-normalization |
| environment | string | `production` / `staging` / `development` -- only `production` survives normalization |
| git_commit | string | |
| release_version | string | |
| lcp_ms, cls, inp_ms, fcp_ms, ttfb_ms, speed_index_ms, total_blocking_time_ms | double | |
| performance_score, accessibility_score, best_practices_score, seo_score | double | Lighthouse's own 0-100 scores |
| request_count, js_transfer_kb, css_transfer_kb, image_transfer_kb, total_transfer_kb | double | |

### `crux/crux_metrics.csv`

Daily, already-aggregated field metrics (CrUX-like: no per-event grain, no
release attribution -- matching how real CrUX data is published).

| Column | Type |
|---|---|
| date | date string |
| origin, route, device_type, connection_type | string |
| p75_lcp_ms, p75_cls, p75_inp_ms | double |
| good/needs_improvement/poor _lcp/cls/inp_ rate | double (each triplet sums to ~1.0) |
| sample_count | int |

### `page_load_logs/page_load_logs_YYYY-MM-DD.jsonl`

One JSON object per line, one line per synthetic browser page-load event.
This is the pipeline's stand-in for RUM (Real User Monitoring) -- the only
source with both individual-event grain and `release_version`, which is why
`core_web_vitals_daily` and regression detection are built from it.

| Column | Type | Notes |
|---|---|---|
| event_id, session_id | string | |
| timestamp | ISO8601 string (UTC) | |
| route, device_type | string | raw, pre-normalization |
| device_id | string | **extra field**, not in the original ask -- see note below |
| browser, os, country, connection_type | string | |
| lcp_ms, cls, inp_ms, ttfb_ms, fcp_ms, page_weight_kb | double | |
| js_error_count | int | |
| api_latency_ms | double | |
| cache_status | string | hit / miss / stale |
| release_version | string | |

> **Design note on `device_id`:** the spec's page-load-log schema has no
> device identifier, but `device_breakdowns` needs to slice by `device_class`,
> which only exists on the device dimension table. A real RUM pipeline
> captures *something* identifying the client's hardware tier (UA-CH,
> a device-detection service, etc.), so `device_id` was added here as the
> realistic join key -- documented rather than silently bolted on.

### `devices/devices.json`

Static dimension table, 9 rows (3 device classes x 3 device types).

| Column | Type |
|---|---|
| device_id | string |
| device_class | string (low-end / mid-range / high-end) |
| device_name | string |
| viewport_width, viewport_height | int |
| cpu_tier | string |
| memory_gb | int |
| network_profile | string |

## Processed layer (`data/processed/`)

Same shape as raw, after: route/device_type standardization, timestamp
casting, production-only filtering (Lighthouse), device enrichment (page
logs), and quality-check filtering. Partitioned by date where applicable.
Parquet.

## Curated layer (`data/curated/`)

### 1. `core_web_vitals_daily`
Grain: one row per (date, route, device_type, release_version).

| Column | Type |
|---|---|
| date | date |
| route, device_type, release_version | string |
| avg_lcp_ms, p75_lcp_ms, avg_cls, p75_cls, avg_inp_ms, p75_inp_ms, avg_ttfb_ms | double |
| sample_count | long |
| good_lcp_rate, good_cls_rate, good_inp_rate | double (share of events under the "good" threshold) |
| overall_health_score | double (0-100, see below) |

### 2. `route_performance_rankings`
Grain: one row per (date, route, device_type).

| Column | Type |
|---|---|
| date, route, device_type | |
| p75_lcp_ms, p75_cls, p75_inp_ms | double (field data) |
| performance_score | double (Lighthouse lab score, daily avg) |
| rank_overall, rank_lcp, rank_cls, rank_inp | int (`dense_rank`, 1 = best, within date+device_type) |
| risk_level | string (low / medium / high / critical) |

### 3. `before_after_comparisons`
Grain: one row per (route, device_type, before_release -> after_release) for
every pair of consecutive releases (ordered by first-seen date).

| Column | Type |
|---|---|
| route, device_type, before_release, after_release | string |
| before/after_p75_lcp_ms, lcp_delta_ms, lcp_delta_percent | double |
| before/after_p75_cls, cls_delta | double |
| before/after_p75_inp_ms, inp_delta_ms | double |
| improvement_status | string (improved / regressed / stable) |

### 4. `regression_events`
Grain: one row per (route, device_type, metric, release transition) that
breaches its regression rule -- see the README for the exact rules.

| Column | Type |
|---|---|
| detected_at | date (the after-release's first-seen date) |
| route, device_type, metric | string |
| baseline_value, current_value, delta, delta_percent | double |
| release_version | string (the "after" release) |
| severity | string (low / medium / high / critical) |
| probable_cause, recommended_action | string (static per-metric guidance) |

### 5. `device_breakdowns`
Grain: one row per (date, route, device_type, device_class, connection_type).

| Column | Type |
|---|---|
| date, route, device_type, device_class, connection_type | string/date |
| p75_lcp_ms, p75_cls, p75_inp_ms | double |
| sample_count | long |
| risk_level | string |

## Health score formula

`overall_health_score` (0-100) is a weighted blend of per-metric scores, each
scored 100 at/under its "good" threshold and linearly decaying to 0 at 2x the
threshold, discounted by a sample-count confidence factor:

```
score(metric, good_threshold) = clamp(0, 1, 2 - value/good_threshold) * 100

weighted = score(lcp, 2500) * 0.35
         + score(cls, 0.1)  * 0.20
         + score(inp, 200)  * 0.30
         + score(ttfb, 800) * 0.15

confidence = min(1, sample_count / 30)

overall_health_score = round(weighted * confidence, 1)
```

## Risk level classification

Used in `route_performance_rankings` and `device_breakdowns`. "Poor" here
means more than 1.6x the good threshold (2500/0.1/200 for LCP/CLS/INP):

- **critical** -- 2 or more metrics are poor
- **high** -- exactly 1 metric is poor
- **medium** -- no metric is poor, but at least 1 is past its good threshold
- **low** -- every metric is within its good threshold
