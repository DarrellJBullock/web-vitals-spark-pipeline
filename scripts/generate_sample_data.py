"""Generates realistic, internally-consistent sample data for the pipeline.

Simulates a small e-commerce site over a 21-day window that ships three
releases. Two deliberate regressions are seeded (a checkout LCP/CLS
regression from an oversized hero carousel, and a search INP regression
from an unthrottled type-ahead) so that detect_regressions.py and the
before/after comparison job have real signal to find. This is pure-Python
(no PySpark, no pandas) so it can run without a JVM.

Run: python -m scripts.generate_sample_data
"""
from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from src.config.settings import PATHS
from src.utils.logging import get_logger
from src.utils.paths import ensure_dir

log = get_logger("generate_sample_data")

RNG_SEED = 42
START_DATE = date(2026, 6, 1)
NUM_DAYS = 21

ORIGIN = "https://example-shop.com"

ROUTES = ["/", "/product", "/search", "/checkout", "/blog", "/account"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
CONNECTION_TYPES = ["wifi", "4G", "3G"]

RELEASES = [
    {"version": "v2.3.0", "commit": "a1b2c3d", "start_day": 0, "end_day": 6},
    {"version": "v2.4.0", "commit": "d4e5f6a", "start_day": 7, "end_day": 13},
    {"version": "v2.5.0", "commit": "f7a8b9c", "start_day": 14, "end_day": 20},
]

# Baseline "healthy" metrics per route (desktop-normalized; device multipliers below).
BASE_METRICS = {
    "/": dict(lcp=1800, cls=0.03, inp=140, ttfb=450, fcp=1100, si=1900, tbt=120,
              page_weight=850, js_kb=280, css_kb=60, image_kb=350, requests=45),
    "/product": dict(lcp=2100, cls=0.05, inp=160, ttfb=500, fcp=1300, si=2200, tbt=160,
                      page_weight=1200, js_kb=320, css_kb=70, image_kb=650, requests=60),
    "/search": dict(lcp=1950, cls=0.04, inp=170, ttfb=480, fcp=1200, si=2000, tbt=180,
                     page_weight=900, js_kb=350, css_kb=55, image_kb=300, requests=50),
    "/checkout": dict(lcp=1900, cls=0.04, inp=150, ttfb=500, fcp=1150, si=1950, tbt=140,
                       page_weight=800, js_kb=300, css_kb=65, image_kb=250, requests=48),
    "/blog": dict(lcp=1700, cls=0.02, inp=120, ttfb=400, fcp=1000, si=1750, tbt=90,
                   page_weight=700, js_kb=200, css_kb=50, image_kb=380, requests=38),
    "/account": dict(lcp=1850, cls=0.03, inp=145, ttfb=480, fcp=1120, si=1900, tbt=110,
                      page_weight=750, js_kb=260, css_kb=55, image_kb=200, requests=40),
}

# Mobile is slowest, desktop fastest, tablet in between -- mirrors real-world CWV data.
DEVICE_MULTIPLIERS = {
    "desktop": dict(lcp=0.82, cls=0.90, inp=0.85, ttfb=0.90, fcp=0.85, si=0.80, tbt=0.75),
    "tablet":  dict(lcp=1.00, cls=1.00, inp=1.00, ttfb=1.00, fcp=1.00, si=1.00, tbt=1.00),
    "mobile":  dict(lcp=1.25, cls=1.30, inp=1.20, ttfb=1.10, fcp=1.20, si=1.30, tbt=1.40),
}

# Deliberate regressions/optimizations layered on top of the baseline, keyed by
# (route, release, device_type) -> {metric: multiplier}.
METRIC_OVERRIDES = {
    # Checkout regression in v2.4.0: an oversized hero carousel ships, hurting
    # LCP and CLS across every device. v2.5.0 removes it and lazy-loads images,
    # ending up *better* than the original v2.3.0 baseline.
    ("/checkout", "v2.4.0", "desktop"): dict(lcp=1.42, cls=3.20),
    ("/checkout", "v2.4.0", "mobile"): dict(lcp=1.48, cls=2.70),
    ("/checkout", "v2.4.0", "tablet"): dict(lcp=1.43, cls=2.60),
    ("/checkout", "v2.5.0", "desktop"): dict(lcp=0.80, cls=0.70),
    ("/checkout", "v2.5.0", "mobile"): dict(lcp=0.80, cls=0.65),
    ("/checkout", "v2.5.0", "tablet"): dict(lcp=0.80, cls=0.68),
    # Search INP regression in v2.5.0: an unthrottled type-ahead ships and is
    # still live at the end of the window (mobile only).
    ("/search", "v2.5.0", "mobile"): dict(inp=1.55),
    # Account TTFB regression in v2.4.0 from a slow backend dependency, fixed
    # by v2.5.0.
    ("/account", "v2.4.0", "desktop"): dict(ttfb=1.85),
    ("/account", "v2.4.0", "mobile"): dict(ttfb=1.75),
    ("/account", "v2.4.0", "tablet"): dict(ttfb=1.80),
    ("/account", "v2.5.0", "desktop"): dict(ttfb=0.95),
    ("/account", "v2.5.0", "mobile"): dict(ttfb=0.95),
    ("/account", "v2.5.0", "tablet"): dict(ttfb=0.95),
}

# Content-weight overrides (device-independent): the checkout carousel adds
# image payload in v2.4.0, then lazy-loading trims it below baseline in v2.5.0.
CONTENT_OVERRIDES = {
    ("/checkout", "v2.4.0"): dict(image_kb=1.60, page_weight=1.30),
    ("/checkout", "v2.5.0"): dict(image_kb=0.90, page_weight=0.92),
}

DEVICE_CATALOG = [
    dict(device_id="m-low-1", device_class="low-end", device_name="Galaxy A13",
         viewport_width=360, viewport_height=800, cpu_tier="low", memory_gb=3,
         network_profile="3G", device_type="mobile", weight=0.40, perf_mult=1.30),
    dict(device_id="m-mid-1", device_class="mid-range", device_name="Pixel 7a",
         viewport_width=393, viewport_height=852, cpu_tier="mid", memory_gb=6,
         network_profile="4G", device_type="mobile", weight=0.40, perf_mult=1.00),
    dict(device_id="m-high-1", device_class="high-end", device_name="iPhone 15 Pro",
         viewport_width=393, viewport_height=852, cpu_tier="high", memory_gb=8,
         network_profile="5G", device_type="mobile", weight=0.20, perf_mult=0.80),
    dict(device_id="d-low-1", device_class="low-end", device_name="Dell Inspiron 15 (2019)",
         viewport_width=1366, viewport_height=768, cpu_tier="low", memory_gb=4,
         network_profile="wifi", device_type="desktop", weight=0.30, perf_mult=1.25),
    dict(device_id="d-mid-1", device_class="mid-range", device_name="Dell XPS 13",
         viewport_width=1920, viewport_height=1080, cpu_tier="mid", memory_gb=16,
         network_profile="wifi", device_type="desktop", weight=0.45, perf_mult=1.00),
    dict(device_id="d-high-1", device_class="high-end", device_name="MacBook Pro 14 (M2 Pro)",
         viewport_width=1920, viewport_height=1080, cpu_tier="high", memory_gb=32,
         network_profile="wifi", device_type="desktop", weight=0.25, perf_mult=0.78),
    dict(device_id="t-low-1", device_class="low-end", device_name="Amazon Fire HD 10",
         viewport_width=800, viewport_height=1280, cpu_tier="low", memory_gb=3,
         network_profile="wifi", device_type="tablet", weight=0.30, perf_mult=1.20),
    dict(device_id="t-mid-1", device_class="mid-range", device_name="iPad (10th gen)",
         viewport_width=820, viewport_height=1180, cpu_tier="mid", memory_gb=4,
         network_profile="wifi", device_type="tablet", weight=0.45, perf_mult=1.00),
    dict(device_id="t-high-1", device_class="high-end", device_name="iPad Pro 12.9 (M2)",
         viewport_width=1024, viewport_height=1366, cpu_tier="high", memory_gb=8,
         network_profile="5G", device_type="tablet", weight=0.25, perf_mult=0.82),
]

BROWSERS_BY_DEVICE = {
    "mobile": [("Chrome Mobile", "Android"), ("Safari", "iOS"), ("Samsung Internet", "Android")],
    "desktop": [("Chrome", "Windows"), ("Chrome", "macOS"), ("Firefox", "Windows"), ("Edge", "Windows"), ("Safari", "macOS")],
    "tablet": [("Safari", "iPadOS"), ("Chrome", "Android")],
}
COUNTRIES = ["US", "GB", "DE", "IN", "BR", "JP", "CA"]
CACHE_STATUSES = ["hit", "miss", "stale"]


def daterange(start: date, days: int):
    for i in range(days):
        yield start + timedelta(days=i)


def release_for_day(day_index: int) -> dict:
    for release in RELEASES:
        if release["start_day"] <= day_index <= release["end_day"]:
            return release
    return RELEASES[-1]


def noisy(value: float, pct: float, rng: random.Random) -> float:
    return value * (1 + rng.uniform(-pct, pct))


def compute_metrics(route: str, device_type: str, release: str, rng: random.Random) -> dict:
    base = BASE_METRICS[route]
    device_mult = DEVICE_MULTIPLIERS[device_type]
    overrides = METRIC_OVERRIDES.get((route, release, device_type), {})
    content_overrides = CONTENT_OVERRIDES.get((route, release), {})

    perf_metrics = ["lcp", "cls", "inp", "ttfb", "fcp", "si", "tbt"]
    out = {}
    for metric in perf_metrics:
        value = base[metric] * device_mult[metric] * overrides.get(metric, 1.0)
        out[metric] = round(noisy(value, 0.06, rng), 4)

    for metric in ["page_weight", "js_kb", "css_kb", "image_kb", "requests"]:
        value = base[metric] * content_overrides.get(metric, 1.0)
        out[metric] = round(noisy(value, 0.08, rng))

    return out


def rate_split(p75_value: float, good_threshold: float, poor_threshold: float, rng: random.Random):
    """Approximates good/needs-improvement/poor distribution shares from a p75 value."""
    if p75_value <= good_threshold:
        good = rng.uniform(0.78, 0.92)
    elif p75_value <= poor_threshold:
        good = rng.uniform(0.45, 0.65)
    else:
        good = rng.uniform(0.15, 0.35)

    if p75_value <= good_threshold:
        poor = rng.uniform(0.01, 0.05)
    elif p75_value <= poor_threshold:
        poor = rng.uniform(0.10, 0.20)
    else:
        poor = rng.uniform(0.35, 0.55)

    needs = max(0.0, 1.0 - good - poor)
    total = good + needs + poor
    return round(good / total, 4), round(needs / total, 4), round(poor / total, 4)


def performance_score(metrics: dict) -> float:
    """Lighthouse-style weighted score (0-100), heavier weight on LCP/TBT/CLS."""
    lcp_score = max(0.0, 1 - metrics["lcp"] / 6000)
    cls_score = max(0.0, 1 - metrics["cls"] / 0.5)
    tbt_score = max(0.0, 1 - metrics["tbt"] / 900)
    fcp_score = max(0.0, 1 - metrics["fcp"] / 4000)
    si_score = max(0.0, 1 - metrics["si"] / 5000)
    weighted = (
        lcp_score * 0.25 + cls_score * 0.15 + tbt_score * 0.30
        + fcp_score * 0.10 + si_score * 0.20
    )
    return round(max(0.0, min(1.0, weighted)) * 100, 1)


def generate_lighthouse(rng: random.Random):
    out_dir = ensure_dir(PATHS.raw_lighthouse)
    total = 0
    for day_index, day in enumerate(daterange(START_DATE, NUM_DAYS)):
        release = release_for_day(day_index)
        reports = []
        for route in ROUTES:
            for device_type in DEVICE_TYPES:
                metrics = compute_metrics(route, device_type, release["version"], rng)
                environment = "production" if rng.random() > 0.04 else rng.choice(["staging", "development"])
                run_ts = datetime(day.year, day.month, day.day, rng.randint(1, 6), rng.randint(0, 59), tzinfo=timezone.utc)
                route_slug = route.strip("/").replace("/", "-") or "home"
                report = {
                    "report_id": f"lh-{day.isoformat()}-{route_slug}-{device_type}",
                    "run_timestamp": run_ts.isoformat(),
                    "url": f"{ORIGIN}{route}",
                    "route": route,
                    "device_type": device_type,
                    "environment": environment,
                    "git_commit": release["commit"] if environment == "production" else rng.choice(["c0ffee1", "dead10c"]),
                    "release_version": release["version"],
                    "lcp_ms": metrics["lcp"],
                    "cls": metrics["cls"],
                    "inp_ms": metrics["inp"],
                    "fcp_ms": metrics["fcp"],
                    "ttfb_ms": metrics["ttfb"],
                    "speed_index_ms": metrics["si"],
                    "total_blocking_time_ms": metrics["tbt"],
                    "performance_score": performance_score(metrics),
                    "accessibility_score": round(noisy(96, 0.02, rng), 1),
                    "best_practices_score": round(noisy(93, 0.03, rng), 1),
                    "seo_score": round(noisy(97, 0.01, rng), 1),
                    "request_count": metrics["requests"],
                    "js_transfer_kb": metrics["js_kb"],
                    "css_transfer_kb": metrics["css_kb"],
                    "image_transfer_kb": metrics["image_kb"],
                    "total_transfer_kb": metrics["page_weight"],
                }
                reports.append(report)
        out_path = out_dir / f"lighthouse_report_{day.isoformat()}.json"
        out_path.write_text(json.dumps(reports, indent=2))
        total += len(reports)
    log.info(f"Wrote {total} Lighthouse reports across {NUM_DAYS} daily files to {out_dir}")


def generate_crux(rng: random.Random):
    out_dir = ensure_dir(PATHS.raw_crux)
    out_path = out_dir / "crux_metrics.csv"
    fieldnames = [
        "date", "origin", "route", "device_type", "connection_type",
        "p75_lcp_ms", "p75_cls", "p75_inp_ms",
        "good_lcp_rate", "needs_improvement_lcp_rate", "poor_lcp_rate",
        "good_cls_rate", "needs_improvement_cls_rate", "poor_cls_rate",
        "good_inp_rate", "needs_improvement_inp_rate", "poor_inp_rate",
        "sample_count",
    ]
    connection_weights = {
        "desktop": [("wifi", 0.90), ("4G", 0.10)],
        "mobile": [("4G", 0.55), ("wifi", 0.30), ("3G", 0.15)],
        "tablet": [("wifi", 0.70), ("4G", 0.30)],
    }
    rows = []
    for day_index, day in enumerate(daterange(START_DATE, NUM_DAYS)):
        release = release_for_day(day_index)
        for route in ROUTES:
            for device_type in DEVICE_TYPES:
                metrics = compute_metrics(route, device_type, release["version"], rng)
                for connection_type, share in connection_weights[device_type]:
                    conn_penalty = {"wifi": 1.0, "4G": 1.05, "3G": 1.25}[connection_type]
                    p75_lcp = round(metrics["lcp"] * conn_penalty)
                    p75_cls = round(metrics["cls"] * (1.0 if connection_type == "wifi" else 1.02), 4)
                    p75_inp = round(metrics["inp"] * conn_penalty)
                    good_lcp, ni_lcp, poor_lcp = rate_split(p75_lcp, 2500, 4000, rng)
                    good_cls, ni_cls, poor_cls = rate_split(p75_cls, 0.1, 0.25, rng)
                    good_inp, ni_inp, poor_inp = rate_split(p75_inp, 200, 500, rng)
                    sample_count = max(50, round(rng.uniform(500, 5000) * share))
                    rows.append({
                        "date": day.isoformat(),
                        "origin": ORIGIN,
                        "route": route,
                        "device_type": device_type,
                        "connection_type": connection_type,
                        "p75_lcp_ms": p75_lcp,
                        "p75_cls": p75_cls,
                        "p75_inp_ms": p75_inp,
                        "good_lcp_rate": good_lcp,
                        "needs_improvement_lcp_rate": ni_lcp,
                        "poor_lcp_rate": poor_lcp,
                        "good_cls_rate": good_cls,
                        "needs_improvement_cls_rate": ni_cls,
                        "poor_cls_rate": poor_cls,
                        "good_inp_rate": good_inp,
                        "needs_improvement_inp_rate": ni_inp,
                        "poor_inp_rate": poor_inp,
                        "sample_count": sample_count,
                    })
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"Wrote {len(rows)} CrUX-like rows to {out_path}")


def pick_device(device_type: str, rng: random.Random) -> dict:
    candidates = [d for d in DEVICE_CATALOG if d["device_type"] == device_type]
    weights = [d["weight"] for d in candidates]
    return rng.choices(candidates, weights=weights, k=1)[0]


def generate_page_load_logs(rng: random.Random):
    out_dir = ensure_dir(PATHS.raw_page_load_logs)
    events_per_slice = 20
    total = 0
    for day_index, day in enumerate(daterange(START_DATE, NUM_DAYS)):
        release = release_for_day(day_index)
        out_path = out_dir / f"page_load_logs_{day.isoformat()}.jsonl"
        with out_path.open("w") as f:
            for route in ROUTES:
                for device_type in DEVICE_TYPES:
                    metrics = compute_metrics(route, device_type, release["version"], rng)
                    for _ in range(events_per_slice):
                        device = pick_device(device_type, rng)
                        browser, os_name = rng.choice(BROWSERS_BY_DEVICE[device_type])
                        event_ts = datetime(
                            day.year, day.month, day.day,
                            rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59),
                            tzinfo=timezone.utc,
                        )
                        perf_mult = device["perf_mult"]
                        js_errors = 0
                        if rng.random() < 0.05:
                            js_errors = rng.randint(1, 3)
                        # Checkout regression window also drives a small uptick in JS errors
                        # (carousel script throwing on slow devices).
                        if route == "/checkout" and release["version"] == "v2.4.0" and rng.random() < 0.15:
                            js_errors += rng.randint(1, 2)
                        event = {
                            "event_id": f"evt-{day.isoformat()}-{route.strip('/').replace('/', '-') or 'home'}-{device_type}-{_}",
                            "session_id": f"sess-{rng.randint(100000, 999999)}",
                            "timestamp": event_ts.isoformat(),
                            "route": route,
                            "device_type": device_type,
                            "device_id": device["device_id"],
                            "browser": browser,
                            "os": os_name,
                            "country": rng.choice(COUNTRIES),
                            "connection_type": device["network_profile"] if device["network_profile"] != "5G" else rng.choice(["wifi", "4G"]),
                            "lcp_ms": round(noisy(metrics["lcp"] * perf_mult, 0.08, rng)),
                            "cls": round(noisy(metrics["cls"] * perf_mult, 0.10, rng), 4),
                            "inp_ms": round(noisy(metrics["inp"] * perf_mult, 0.08, rng)),
                            "ttfb_ms": round(noisy(metrics["ttfb"] * perf_mult, 0.08, rng)),
                            "fcp_ms": round(noisy(metrics["fcp"] * perf_mult, 0.08, rng)),
                            "page_weight_kb": round(noisy(metrics["page_weight"], 0.08, rng)),
                            "js_error_count": js_errors,
                            "api_latency_ms": round(noisy(metrics["ttfb"] * 0.6, 0.15, rng)),
                            "cache_status": rng.choice(CACHE_STATUSES),
                            "release_version": release["version"],
                        }
                        f.write(json.dumps(event) + "\n")
                        total += 1
        _ = out_path
    log.info(f"Wrote {total} page load log events across {NUM_DAYS} daily files to {out_dir}")


def generate_devices():
    out_dir = ensure_dir(PATHS.raw_devices)
    out_path = out_dir / "devices.json"
    devices = [
        {
            "device_id": d["device_id"],
            "device_class": d["device_class"],
            "device_name": d["device_name"],
            "viewport_width": d["viewport_width"],
            "viewport_height": d["viewport_height"],
            "cpu_tier": d["cpu_tier"],
            "memory_gb": d["memory_gb"],
            "network_profile": d["network_profile"],
        }
        for d in DEVICE_CATALOG
    ]
    out_path.write_text(json.dumps(devices, indent=2))
    log.info(f"Wrote {len(devices)} device records to {out_path}")


def main():
    rng = random.Random(RNG_SEED)
    log.info(f"Generating sample data for {NUM_DAYS} days starting {START_DATE.isoformat()}")
    generate_lighthouse(rng)
    generate_crux(rng)
    generate_page_load_logs(rng)
    generate_devices()
    log.info("Sample data generation complete.")


if __name__ == "__main__":
    main()
