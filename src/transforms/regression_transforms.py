"""Release-over-release comparison and regression detection (curated tables
3 and 4: before_after_comparisons and regression_events).

Releases are ordered generically by first-seen date rather than by parsing
version strings, so this works regardless of the versioning scheme in use.

Each metric's regression rule fires on either leg: (a) a relative worsening
past its percent/delta threshold, or (b) *newly* crossing its absolute "poor"
threshold (was acceptable before, is poor now). A page that was already poor
and stayed poor with no real change is not re-flagged every release -- that's
chronic risk, which route_performance_rankings/device_breakdowns already
surface via risk_level, not a new regression event.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config.settings import REGRESSION_THRESHOLDS as R

METRIC_RULES = {
    "LCP": dict(pct_threshold=R.lcp_regression_pct, poor_threshold=R.lcp_poor_threshold_ms, mode="pct"),
    "CLS": dict(delta_threshold=R.cls_regression_delta, poor_threshold=R.cls_poor_threshold, mode="delta"),
    "INP": dict(pct_threshold=R.inp_regression_pct, poor_threshold=R.inp_poor_threshold_ms, mode="pct"),
    "TTFB": dict(pct_threshold=R.ttfb_regression_pct, poor_threshold=R.ttfb_poor_threshold_ms, mode="pct"),
}

PROBABLE_CAUSES = {
    "LCP": (
        "Largest content element is taking longer to render (often hero images, "
        "web fonts, or render-blocking resources).",
        "Audit and compress hero/above-the-fold images, preload critical fonts, "
        "and defer non-critical JS/CSS.",
    ),
    "CLS": (
        "Visible layout shift increased, typically from late-loading images/ads/"
        "embeds without reserved space, or web fonts causing reflow.",
        "Set explicit width/height (or aspect-ratio) on images and embeds, reserve "
        "space for ads, and use font-display: swap with size-matched fallbacks.",
    ),
    "INP": (
        "Interactions are taking longer to respond, usually from long JavaScript "
        "tasks blocking the main thread.",
        "Break up long tasks, debounce/throttle expensive event handlers, and move "
        "non-urgent work off the main thread.",
    ),
    "TTFB": (
        "Server/API response time increased, pointing to backend, database, or "
        "CDN/cache issues.",
        "Profile backend response times, check for slow DB queries or cache "
        "misses, and verify CDN edge cache hit rates.",
    ),
}


def build_release_aggregates(page_logs_df: DataFrame) -> DataFrame:
    """Per route/device/release p75 metrics plus the release's first-seen date,
    used both to order releases and as the join key for before/after pairing."""
    return page_logs_df.groupBy("route", "device_type", "release_version").agg(
        F.min("event_date").alias("release_start_date"),
        F.round(F.percentile_approx("lcp_ms", 0.75), 1).alias("p75_lcp_ms"),
        F.round(F.percentile_approx("cls", 0.75), 4).alias("p75_cls"),
        F.round(F.percentile_approx("inp_ms", 0.75), 1).alias("p75_inp_ms"),
        F.round(F.percentile_approx("ttfb_ms", 0.75), 1).alias("p75_ttfb_ms"),
        F.count(F.lit(1)).alias("sample_count"),
    )


def build_release_pairs(release_aggregates_df: DataFrame) -> DataFrame:
    """Self-joins consecutive releases (by first-seen date) per route/device."""
    window = Window.partitionBy("route", "device_type").orderBy("release_start_date")
    ranked = release_aggregates_df.withColumn("release_rank", F.dense_rank().over(window))

    before = ranked.select(
        "route", "device_type", "release_rank",
        F.col("release_version").alias("before_release"),
        F.col("p75_lcp_ms").alias("before_p75_lcp_ms"),
        F.col("p75_cls").alias("before_p75_cls"),
        F.col("p75_inp_ms").alias("before_p75_inp_ms"),
        F.col("p75_ttfb_ms").alias("before_p75_ttfb_ms"),
    )
    after = ranked.select(
        "route", "device_type", "release_rank",
        F.col("release_version").alias("after_release"),
        F.col("release_start_date").alias("after_release_start_date"),
        F.col("p75_lcp_ms").alias("after_p75_lcp_ms"),
        F.col("p75_cls").alias("after_p75_cls"),
        F.col("p75_inp_ms").alias("after_p75_inp_ms"),
        F.col("p75_ttfb_ms").alias("after_p75_ttfb_ms"),
    ).withColumn("release_rank", F.col("release_rank") - 1)

    pairs = before.join(after, on=["route", "device_type", "release_rank"], how="inner")

    pairs = (
        pairs.withColumn("lcp_delta_ms", F.round(F.col("after_p75_lcp_ms") - F.col("before_p75_lcp_ms"), 1))
        .withColumn("lcp_delta_percent", F.round(F.col("lcp_delta_ms") / F.col("before_p75_lcp_ms") * 100, 2))
        .withColumn("cls_delta", F.round(F.col("after_p75_cls") - F.col("before_p75_cls"), 4))
        .withColumn("inp_delta_ms", F.round(F.col("after_p75_inp_ms") - F.col("before_p75_inp_ms"), 1))
        .withColumn("inp_delta_percent", F.round(F.col("inp_delta_ms") / F.col("before_p75_inp_ms") * 100, 2))
        .withColumn("ttfb_delta_ms", F.round(F.col("after_p75_ttfb_ms") - F.col("before_p75_ttfb_ms"), 1))
        .withColumn("ttfb_delta_percent", F.round(F.col("ttfb_delta_ms") / F.col("before_p75_ttfb_ms") * 100, 2))
    )
    return pairs


def _newly_crosses(before_col: str, after_col: str, poor_threshold: float) -> F.Column:
    """True when a route/device newly breaches the poor threshold this release
    (was under it before, is over it now) -- as opposed to a page that's been
    chronically bad and simply hasn't changed, which isn't a "regression"."""
    return (F.col(after_col) > poor_threshold) & (F.col(before_col) <= poor_threshold)


def _is_regressed(pairs: DataFrame) -> F.Column:
    lcp_rule = METRIC_RULES["LCP"]
    inp_rule = METRIC_RULES["INP"]
    cls_rule = METRIC_RULES["CLS"]
    ttfb_rule = METRIC_RULES["TTFB"]
    return (
        (F.col("lcp_delta_percent") > lcp_rule["pct_threshold"] * 100)
        | _newly_crosses("before_p75_lcp_ms", "after_p75_lcp_ms", lcp_rule["poor_threshold"])
        | (F.col("cls_delta") > cls_rule["delta_threshold"])
        | _newly_crosses("before_p75_cls", "after_p75_cls", cls_rule["poor_threshold"])
        | (F.col("inp_delta_percent") > inp_rule["pct_threshold"] * 100)
        | _newly_crosses("before_p75_inp_ms", "after_p75_inp_ms", inp_rule["poor_threshold"])
        | (F.col("ttfb_delta_percent") > ttfb_rule["pct_threshold"] * 100)
        | _newly_crosses("before_p75_ttfb_ms", "after_p75_ttfb_ms", ttfb_rule["poor_threshold"])
    )


def build_before_after_comparisons(pairs_df: DataFrame) -> DataFrame:
    """Curated table 3: before_after_comparisons."""
    improved = (
        (F.col("lcp_delta_percent") <= -10)
        | (F.col("inp_delta_percent") <= -10)
        | (F.col("cls_delta") <= -0.02)
    )
    with_status = pairs_df.withColumn(
        "improvement_status",
        F.when(_is_regressed(pairs_df), F.lit("regressed"))
        .when(improved, F.lit("improved"))
        .otherwise(F.lit("stable")),
    )
    return with_status.select(
        "route", "device_type", "before_release", "after_release",
        "before_p75_lcp_ms", "after_p75_lcp_ms", "lcp_delta_ms", "lcp_delta_percent",
        "before_p75_cls", "after_p75_cls", "cls_delta",
        "before_p75_inp_ms", "after_p75_inp_ms", "inp_delta_ms", "improvement_status",
    )


def _severity_expr(excess_ratio: F.Column) -> F.Column:
    return (
        F.when(excess_ratio >= 2.0, F.lit("critical"))
        .when(excess_ratio >= 1.5, F.lit("high"))
        .when(excess_ratio >= 1.2, F.lit("medium"))
        .otherwise(F.lit("low"))
    )


def _metric_events(pairs_df: DataFrame, metric: str) -> DataFrame:
    rule = METRIC_RULES[metric]
    col_map = {
        "LCP": ("before_p75_lcp_ms", "after_p75_lcp_ms", "lcp_delta_ms", "lcp_delta_percent"),
        "CLS": ("before_p75_cls", "after_p75_cls", "cls_delta", None),
        "INP": ("before_p75_inp_ms", "after_p75_inp_ms", "inp_delta_ms", "inp_delta_percent"),
        "TTFB": ("before_p75_ttfb_ms", "after_p75_ttfb_ms", "ttfb_delta_ms", "ttfb_delta_percent"),
    }
    before_col, after_col, delta_col, pct_col = col_map[metric]

    newly_crossed = _newly_crosses(before_col, after_col, rule["poor_threshold"])

    if rule["mode"] == "pct":
        flagged = (F.col(pct_col) > rule["pct_threshold"] * 100) | newly_crossed
        excess_ratio = F.greatest(
            F.col(pct_col) / F.lit(rule["pct_threshold"] * 100),
            F.col(after_col) / F.lit(float(rule["poor_threshold"])),
        )
        delta_percent_col = F.col(pct_col)
    else:  # delta mode (CLS)
        flagged = (F.col(delta_col) > rule["delta_threshold"]) | newly_crossed
        excess_ratio = F.greatest(
            F.col(delta_col) / F.lit(rule["delta_threshold"]),
            F.col(after_col) / F.lit(rule["poor_threshold"]),
        )
        delta_percent_col = F.round(F.col(delta_col) / F.col(before_col) * 100, 2)

    cause, action = PROBABLE_CAUSES[metric]
    events = pairs_df.select(
        F.col("after_release_start_date").alias("detected_at"),
        "route",
        "device_type",
        F.lit(metric).alias("metric"),
        F.col(before_col).alias("baseline_value"),
        F.col(after_col).alias("current_value"),
        F.col(delta_col).alias("delta"),
        delta_percent_col.alias("delta_percent"),
        F.col("after_release").alias("release_version"),
        _severity_expr(excess_ratio).alias("severity"),
        F.lit(cause).alias("probable_cause"),
        F.lit(action).alias("recommended_action"),
    ).where(flagged)
    return events


def detect_regressions(pairs_df: DataFrame) -> DataFrame:
    """Curated table 4: regression_events, one row per (route, device, metric,
    release-transition) that breaches its regression rule."""
    frames = [_metric_events(pairs_df, metric) for metric in METRIC_RULES]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result
