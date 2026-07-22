"""Stage 2: ingest raw CrUX-like field metrics, normalize, quality-check, and
land them in the processed layer as Parquet.

Run: python -m src.jobs.ingest_crux
"""
from __future__ import annotations

from src.config.settings import PATHS
from src.quality.data_quality_checks import run_quality_checks
from src.transforms.crux_transforms import REQUIRED_COLUMNS, normalize_crux, read_raw_crux
from src.utils.logging import get_logger
from src.utils.paths import as_posix, ensure_dir
from src.utils.spark import get_spark

log = get_logger("ingest_crux")

NON_NEGATIVE_COLUMNS = ["p75_lcp_ms", "p75_cls", "p75_inp_ms", "sample_count"]


def run() -> None:
    spark = get_spark("ingest-crux")
    try:
        log.info(f"Reading raw CrUX-like metrics from {PATHS.raw_crux}")
        raw_df = read_raw_crux(spark, PATHS.raw_crux)
        raw_count = raw_df.count()
        log.info(f"Read {raw_count} raw CrUX-like records")

        normalized_df = normalize_crux(raw_df)
        clean_df = run_quality_checks(
            normalized_df,
            table_name="crux",
            required_columns=REQUIRED_COLUMNS,
            non_negative_columns=NON_NEGATIVE_COLUMNS,
            check_sample_count=True,
        )

        out_dir = ensure_dir(PATHS.processed / "crux")
        clean_df.write.mode("overwrite").partitionBy("date").parquet(as_posix(out_dir))
        log.info(f"Wrote {clean_df.count()} normalized CrUX-like records to {out_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
