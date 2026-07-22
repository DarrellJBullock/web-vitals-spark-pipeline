"""Generates dashboard-ready CSV exports and a markdown summary report from
the curated Parquet tables.

Run: python -m src.jobs.generate_reports
"""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.settings import PATHS
from src.utils.logging import get_logger
from src.utils.paths import as_posix, ensure_dir, write_single_csv
from src.utils.spark import get_spark

log = get_logger("generate_reports")


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return "_No data available for this period._\n"
    header_line = "| " + " | ".join(headers) + " |"
    sep_line = "| " + " | ".join("---" for _ in headers) + " |"
    body_lines = ["| " + " | ".join(str(v) for v in row) + " |" for row in rows]
    return "\n".join([header_line, sep_line, *body_lines]) + "\n"


def _overall_health_section(daily_df: DataFrame) -> str:
    overall = daily_df.agg(
        F.round(F.avg("overall_health_score"), 1).alias("avg_score"),
        F.sum("sample_count").alias("total_samples"),
        F.countDistinct("route").alias("routes"),
        F.min("date").alias("start_date"),
        F.max("date").alias("end_date"),
    ).collect()[0]

    latest_date = daily_df.agg(F.max("date")).collect()[0][0]
    by_device = (
        daily_df.where(F.col("date") == latest_date)
        .groupBy("device_type")
        .agg(F.round(F.avg("overall_health_score"), 1).alias("avg_score"))
        .orderBy(F.col("avg_score").desc())
        .collect()
    )
    device_rows = [[r["device_type"], r["avg_score"]] for r in by_device]

    lines = [
        f"- **Period covered:** {overall['start_date']} to {overall['end_date']}",
        f"- **Routes tracked:** {overall['routes']}",
        f"- **Total field samples analyzed:** {overall['total_samples']:,}",
        f"- **Average overall health score (0-100):** {overall['avg_score']}",
        "",
        f"**Health score by device type (as of {latest_date}):**",
        "",
        _md_table(["Device Type", "Avg Health Score"], device_rows),
    ]
    return "\n".join(lines)


def _best_worst_routes_section(daily_df: DataFrame) -> tuple[str, str]:
    by_route = (
        daily_df.groupBy("route")
        .agg(
            F.round(F.avg("overall_health_score"), 1).alias("avg_health_score"),
            F.round(F.avg("p75_lcp_ms"), 0).alias("avg_p75_lcp_ms"),
            F.round(F.avg("p75_cls"), 3).alias("avg_p75_cls"),
            F.round(F.avg("p75_inp_ms"), 0).alias("avg_p75_inp_ms"),
        )
        .collect()
    )
    ranked = sorted(by_route, key=lambda r: r["avg_health_score"], reverse=True)

    def fmt(rows):
        return [[r["route"], r["avg_health_score"], r["avg_p75_lcp_ms"], r["avg_p75_cls"], r["avg_p75_inp_ms"]] for r in rows]

    headers = ["Route", "Avg Health Score", "Avg p75 LCP (ms)", "Avg p75 CLS", "Avg p75 INP (ms)"]
    best_md = _md_table(headers, fmt(ranked[:5]))
    worst_md = _md_table(headers, fmt(list(reversed(ranked))[:5]))
    return best_md, worst_md


def _biggest_improvements_section(before_after_df: DataFrame) -> str:
    rows = (
        before_after_df.where(F.col("improvement_status") == "improved")
        .orderBy(F.col("lcp_delta_percent").asc())
        .limit(5)
        .collect()
    )
    table_rows = [
        [
            r["route"], r["device_type"], f"{r['before_release']} -> {r['after_release']}",
            f"{r['before_p75_lcp_ms']} -> {r['after_p75_lcp_ms']}", f"{r['lcp_delta_percent']}%",
            f"{r['before_p75_cls']} -> {r['after_p75_cls']}",
        ]
        for r in rows
    ]
    headers = ["Route", "Device", "Release", "p75 LCP (ms)", "LCP Delta", "p75 CLS"]
    return _md_table(headers, table_rows)


def _biggest_regressions_section(regression_df: DataFrame) -> str:
    severity_order = F.when(F.col("severity") == "critical", 4) \
        .when(F.col("severity") == "high", 3) \
        .when(F.col("severity") == "medium", 2) \
        .otherwise(1)
    rows = (
        regression_df.withColumn("_sev_rank", severity_order)
        .orderBy(F.col("_sev_rank").desc(), F.col("delta_percent").desc())
        .limit(5)
        .collect()
    )
    table_rows = [
        [
            r["route"], r["device_type"], r["metric"], r["release_version"],
            r["severity"], r["baseline_value"], r["current_value"], f"{r['delta_percent']}%",
        ]
        for r in rows
    ]
    headers = ["Route", "Device", "Metric", "Release", "Severity", "Baseline", "Current", "Delta %"]
    return _md_table(headers, table_rows)


def _mobile_vs_desktop_section(daily_df: DataFrame) -> str:
    rows = (
        daily_df.groupBy("device_type")
        .agg(
            F.round(F.avg("overall_health_score"), 1).alias("avg_health_score"),
            F.round(F.avg("p75_lcp_ms"), 0).alias("avg_p75_lcp_ms"),
            F.round(F.avg("p75_cls"), 3).alias("avg_p75_cls"),
            F.round(F.avg("p75_inp_ms"), 0).alias("avg_p75_inp_ms"),
            F.round(F.avg("good_lcp_rate") * 100, 1).alias("good_lcp_pct"),
        )
        .orderBy(F.col("avg_health_score").desc())
        .collect()
    )
    table_rows = [
        [r["device_type"], r["avg_health_score"], r["avg_p75_lcp_ms"], r["avg_p75_cls"], r["avg_p75_inp_ms"], f"{r['good_lcp_pct']}%"]
        for r in rows
    ]
    headers = ["Device Type", "Avg Health Score", "Avg p75 LCP (ms)", "Avg p75 CLS", "Avg p75 INP (ms)", "% Good LCP"]
    return _md_table(headers, table_rows)


def _recommended_actions_section(regression_df: DataFrame) -> str:
    rows = (
        regression_df.select("metric", "probable_cause", "recommended_action")
        .distinct()
        .orderBy("metric")
        .collect()
    )
    if not rows:
        return "_No active regressions -- no immediate action required._\n"
    lines = []
    for r in rows:
        lines.append(f"- **{r['metric']}** -- {r['probable_cause']}\n  - _Action:_ {r['recommended_action']}")
    return "\n".join(lines) + "\n"


def build_summary_markdown(daily_df: DataFrame, before_after_df: DataFrame, regression_df: DataFrame) -> str:
    best_md, worst_md = _best_worst_routes_section(daily_df)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""# Core Web Vitals Summary Report

_Generated {generated_at}_

## Overall Health Summary

{_overall_health_section(daily_df)}

## Best Performing Routes

{best_md}

## Worst Performing Routes

{worst_md}

## Biggest Improvements (Before vs. After Release)

{_biggest_improvements_section(before_after_df)}

## Biggest Regressions

{_biggest_regressions_section(regression_df)}

## Mobile vs. Desktop vs. Tablet Findings

{_mobile_vs_desktop_section(daily_df)}

## Recommended Frontend Actions

{_recommended_actions_section(regression_df)}
"""


def run() -> None:
    spark = get_spark("generate-reports")
    try:
        daily_df = spark.read.parquet(as_posix(PATHS.curated / "core_web_vitals_daily"))
        rankings_df = spark.read.parquet(as_posix(PATHS.curated / "route_performance_rankings"))
        before_after_df = spark.read.parquet(as_posix(PATHS.curated / "before_after_comparisons"))
        regression_df = spark.read.parquet(as_posix(PATHS.curated / "regression_events"))
        device_breakdown_df = spark.read.parquet(as_posix(PATHS.curated / "device_breakdowns"))

        reports_dir = ensure_dir(PATHS.reports)

        write_single_csv(rankings_df.orderBy("date", "device_type", "rank_overall"), reports_dir / "route_rankings.csv")
        log.info("Wrote reports/route_rankings.csv")

        write_single_csv(regression_df.orderBy(F.col("detected_at").desc()), reports_dir / "regression_events.csv")
        log.info("Wrote reports/regression_events.csv")

        write_single_csv(before_after_df.orderBy("route", "device_type"), reports_dir / "before_after_comparison.csv")
        log.info("Wrote reports/before_after_comparison.csv")

        write_single_csv(device_breakdown_df.orderBy("date", "route", "device_type"), reports_dir / "device_breakdown.csv")
        log.info("Wrote reports/device_breakdown.csv")

        summary_md = build_summary_markdown(daily_df, before_after_df, regression_df)
        summary_path = reports_dir / "web_vitals_summary.md"
        summary_path.write_text(summary_md)
        log.info("Wrote reports/web_vitals_summary.md")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
