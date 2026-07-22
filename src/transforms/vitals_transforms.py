"""Core Web Vitals normalization (stage 7) and curated analytics table builds
(stages 9-10): core_web_vitals_daily, route_performance_rankings, and
device_breakdowns. Before/after comparisons and regression detection live in
regression_transforms.py since both need release-over-release pairing.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config.settings import VITALS_THRESHOLDS

_T = VITALS_THRESHOLDS
# "Clearly bad" stretch factor used to separate "needs improvement" from "poor"
# when no explicit poor threshold is given (health-score/risk classification
# only; regression detection uses the exact rules in regression_transforms.py).
_POOR_STRETCH = 1.6


def _metric_good_expr(col: str, threshold: float) -> F.Column:
    return (F.col(col) <= threshold).cast("int")


def _metric_score_expr(col: str, threshold: float) -> F.Column:
    """0-100 per-metric score: 100 at/under the good threshold, linearly
    decaying to 0 at 2x the threshold."""
    ratio = F.col(col) / F.lit(threshold)
    score = F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.lit(2.0) - ratio))
    return score * F.lit(100.0)


def add_health_score(
    df: DataFrame,
    lcp_col: str = "avg_lcp_ms",
    cls_col: str = "avg_cls",
    inp_col: str = "avg_inp_ms",
    ttfb_col: str = "avg_ttfb_ms",
    sample_count_col: str = "sample_count",
    min_confident_samples: int = 30,
) -> DataFrame:
    """Adds overall_health_score (0-100): a weighted blend of LCP/CLS/INP/TTFB
    health, discounted when sample_count is too small to be reliable."""
    weighted = (
        _metric_score_expr(lcp_col, _T.lcp_good_ms) * 0.35
        + _metric_score_expr(cls_col, _T.cls_good) * 0.20
        + _metric_score_expr(inp_col, _T.inp_good_ms) * 0.30
        + _metric_score_expr(ttfb_col, _T.ttfb_good_ms) * 0.15
    )
    confidence = F.least(F.lit(1.0), F.col(sample_count_col) / F.lit(float(min_confident_samples)))
    return df.withColumn("overall_health_score", F.round(weighted * confidence, 1))


def add_risk_level(
    df: DataFrame,
    lcp_col: str,
    cls_col: str,
    inp_col: str,
) -> DataFrame:
    """Adds risk_level (low/medium/high/critical) from how many metrics are
    in "poor" (> 1.6x good threshold) vs. "needs improvement" territory."""
    poor_count = (
        (F.col(lcp_col) > _T.lcp_good_ms * _POOR_STRETCH).cast("int")
        + (F.col(cls_col) > _T.cls_good * _POOR_STRETCH).cast("int")
        + (F.col(inp_col) > _T.inp_good_ms * _POOR_STRETCH).cast("int")
    )
    needs_improvement_count = (
        (F.col(lcp_col) > _T.lcp_good_ms).cast("int")
        + (F.col(cls_col) > _T.cls_good).cast("int")
        + (F.col(inp_col) > _T.inp_good_ms).cast("int")
    )
    return df.withColumn(
        "risk_level",
        F.when(poor_count >= 2, F.lit("critical"))
        .when(poor_count == 1, F.lit("high"))
        .when(needs_improvement_count >= 1, F.lit("medium"))
        .otherwise(F.lit("low")),
    )


def build_core_web_vitals_daily(page_logs_df: DataFrame) -> DataFrame:
    """Curated table 1: daily CWV rollups per route/device/release, aggregated
    from page-load events (our RUM-equivalent source)."""
    grouped = page_logs_df.groupBy(
        F.col("event_date").alias("date"), "route", "device_type", "release_version"
    ).agg(
        F.round(F.avg("lcp_ms"), 1).alias("avg_lcp_ms"),
        F.round(F.percentile_approx("lcp_ms", 0.75), 1).alias("p75_lcp_ms"),
        F.round(F.avg("cls"), 4).alias("avg_cls"),
        F.round(F.percentile_approx("cls", 0.75), 4).alias("p75_cls"),
        F.round(F.avg("inp_ms"), 1).alias("avg_inp_ms"),
        F.round(F.percentile_approx("inp_ms", 0.75), 1).alias("p75_inp_ms"),
        F.round(F.avg("ttfb_ms"), 1).alias("avg_ttfb_ms"),
        F.count(F.lit(1)).alias("sample_count"),
        F.round(F.avg(_metric_good_expr("lcp_ms", _T.lcp_good_ms).cast("double")), 4).alias("good_lcp_rate"),
        F.round(F.avg(_metric_good_expr("cls", _T.cls_good).cast("double")), 4).alias("good_cls_rate"),
        F.round(F.avg(_metric_good_expr("inp_ms", _T.inp_good_ms).cast("double")), 4).alias("good_inp_rate"),
    )
    return add_health_score(grouped)


def build_route_performance_rankings(daily_df: DataFrame, lighthouse_df: DataFrame) -> DataFrame:
    """Curated table 2: ranks routes within each date+device_type by field p75
    metrics, blended with the Lighthouse lab performance_score."""
    lab_scores = lighthouse_df.groupBy(
        F.col("report_date").alias("date"), "route", "device_type"
    ).agg(F.round(F.avg("performance_score"), 1).alias("performance_score"))

    joined = daily_df.join(lab_scores, on=["date", "route", "device_type"], how="left")

    partition = Window.partitionBy("date", "device_type")
    ranked = (
        joined
        .withColumn("rank_overall", F.dense_rank().over(partition.orderBy(F.col("performance_score").desc_nulls_last())))
        .withColumn("rank_lcp", F.dense_rank().over(partition.orderBy(F.col("p75_lcp_ms").asc())))
        .withColumn("rank_cls", F.dense_rank().over(partition.orderBy(F.col("p75_cls").asc())))
        .withColumn("rank_inp", F.dense_rank().over(partition.orderBy(F.col("p75_inp_ms").asc())))
    )
    ranked = add_risk_level(ranked, "p75_lcp_ms", "p75_cls", "p75_inp_ms")
    return ranked.select(
        "date", "route", "device_type", "p75_lcp_ms", "p75_cls", "p75_inp_ms",
        "performance_score", "rank_overall", "rank_lcp", "rank_cls", "rank_inp", "risk_level",
    )


def build_device_breakdowns(enriched_page_logs_df: DataFrame) -> DataFrame:
    """Curated table 5: p75 metrics sliced by device_class + connection_type."""
    grouped = enriched_page_logs_df.groupBy(
        F.col("event_date").alias("date"), "route", "device_type", "device_class", "connection_type"
    ).agg(
        F.round(F.percentile_approx("lcp_ms", 0.75), 1).alias("p75_lcp_ms"),
        F.round(F.percentile_approx("cls", 0.75), 4).alias("p75_cls"),
        F.round(F.percentile_approx("inp_ms", 0.75), 1).alias("p75_inp_ms"),
        F.count(F.lit(1)).alias("sample_count"),
    )
    return add_risk_level(grouped, "p75_lcp_ms", "p75_cls", "p75_inp_ms")
