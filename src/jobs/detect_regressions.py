"""Detects release-over-release performance regressions and writes the
regression_events curated table.

Reads independently from the processed layer (rather than depending on
build_core_web_vitals_tables having just run) so it can be re-run on its own,
same as any other batch job in this pipeline.

Run: python -m src.jobs.detect_regressions
"""
from __future__ import annotations

from src.config.settings import PATHS
from src.transforms.regression_transforms import (
    build_release_aggregates,
    build_release_pairs,
    detect_regressions,
)
from src.utils.logging import get_logger
from src.utils.paths import as_posix, ensure_dir
from src.utils.spark import get_spark

log = get_logger("detect_regressions")


def run() -> None:
    spark = get_spark("detect-regressions")
    try:
        page_logs_df = spark.read.parquet(as_posix(PATHS.processed / "page_logs"))

        release_aggregates_df = build_release_aggregates(page_logs_df)
        release_pairs_df = build_release_pairs(release_aggregates_df)
        events_df = detect_regressions(release_pairs_df)

        out_dir = ensure_dir(PATHS.curated / "regression_events")
        events_df.write.mode("overwrite").parquet(as_posix(out_dir))

        count = events_df.count()
        if count:
            by_severity = events_df.groupBy("severity").count().collect()
            summary = ", ".join(f"{row['severity']}={row['count']}" for row in by_severity)
            log.warning(f"Detected {count} regression events ({summary})")
        else:
            log.info("No regressions detected")
        log.info(f"Wrote regression_events to {out_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
