"""Stage 1: ingest raw Lighthouse reports, normalize, quality-check, and land
them in the processed layer as Parquet.

Run: python -m src.jobs.ingest_lighthouse
"""
from __future__ import annotations

from src.config.settings import PATHS
from src.quality.data_quality_checks import run_quality_checks
from src.transforms.lighthouse_transforms import (
    REQUIRED_COLUMNS,
    normalize_lighthouse,
    read_raw_lighthouse,
)
from src.utils.logging import get_logger
from src.utils.paths import as_posix, ensure_dir
from src.utils.spark import get_spark

log = get_logger("ingest_lighthouse")

NON_NEGATIVE_COLUMNS = [
    "lcp_ms", "cls", "inp_ms", "ttfb_ms", "fcp_ms", "speed_index_ms",
    "total_blocking_time_ms", "request_count",
]


def run() -> None:
    spark = get_spark("ingest-lighthouse")
    try:
        log.info(f"Reading raw Lighthouse reports from {PATHS.raw_lighthouse}")
        raw_df = read_raw_lighthouse(spark, PATHS.raw_lighthouse)
        raw_count = raw_df.count()
        log.info(f"Read {raw_count} raw Lighthouse records")

        normalized_df = normalize_lighthouse(raw_df)
        clean_df = run_quality_checks(
            normalized_df,
            table_name="lighthouse",
            required_columns=REQUIRED_COLUMNS,
            non_negative_columns=NON_NEGATIVE_COLUMNS,
        )

        out_dir = ensure_dir(PATHS.processed / "lighthouse")
        clean_df.write.mode("overwrite").partitionBy("report_date").parquet(as_posix(out_dir))
        log.info(f"Wrote {clean_df.count()} normalized Lighthouse records to {out_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
