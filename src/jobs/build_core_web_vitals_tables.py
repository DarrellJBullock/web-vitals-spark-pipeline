"""Stages 9-10: build the curated analytics tables from the processed layer.

Produces core_web_vitals_daily, route_performance_rankings,
device_breakdowns, and before_after_comparisons as Parquet under
data/curated/. Regression events are built separately by detect_regressions.py.

Run: python -m src.jobs.build_core_web_vitals_tables
"""
from __future__ import annotations

from src.config.settings import PATHS
from src.transforms.regression_transforms import (
    build_before_after_comparisons,
    build_release_aggregates,
    build_release_pairs,
)
from src.transforms.vitals_transforms import (
    build_core_web_vitals_daily,
    build_device_breakdowns,
    build_route_performance_rankings,
)
from src.utils.logging import get_logger
from src.utils.paths import as_posix, ensure_dir
from src.utils.spark import get_spark

log = get_logger("build_core_web_vitals_tables")


def run() -> None:
    spark = get_spark("build-core-web-vitals-tables")
    try:
        page_logs_df = spark.read.parquet(as_posix(PATHS.processed / "page_logs")).cache()
        lighthouse_df = spark.read.parquet(as_posix(PATHS.processed / "lighthouse"))

        log.info("Building core_web_vitals_daily")
        daily_df = build_core_web_vitals_daily(page_logs_df).cache()
        daily_out = ensure_dir(PATHS.curated / "core_web_vitals_daily")
        daily_df.write.mode("overwrite").partitionBy("date").parquet(as_posix(daily_out))
        log.info(f"Wrote {daily_df.count()} rows to {daily_out}")

        log.info("Building route_performance_rankings")
        rankings_df = build_route_performance_rankings(daily_df, lighthouse_df)
        rankings_out = ensure_dir(PATHS.curated / "route_performance_rankings")
        rankings_df.write.mode("overwrite").partitionBy("date").parquet(as_posix(rankings_out))
        log.info(f"Wrote {rankings_df.count()} rows to {rankings_out}")

        log.info("Building device_breakdowns")
        breakdowns_df = build_device_breakdowns(page_logs_df)
        breakdowns_out = ensure_dir(PATHS.curated / "device_breakdowns")
        breakdowns_df.write.mode("overwrite").partitionBy("date").parquet(as_posix(breakdowns_out))
        log.info(f"Wrote {breakdowns_df.count()} rows to {breakdowns_out}")

        log.info("Building before_after_comparisons")
        release_aggregates_df = build_release_aggregates(page_logs_df)
        release_pairs_df = build_release_pairs(release_aggregates_df)
        before_after_df = build_before_after_comparisons(release_pairs_df)
        before_after_out = ensure_dir(PATHS.curated / "before_after_comparisons")
        before_after_df.write.mode("overwrite").parquet(as_posix(before_after_out))
        log.info(f"Wrote {before_after_df.count()} rows to {before_after_out}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
