"""Stages 3-4: ingest synthetic page-load logs and device/network metadata,
normalize, enrich logs with device metadata, quality-check, and land both in
the processed layer as Parquet.

Run: python -m src.jobs.ingest_page_logs
"""
from __future__ import annotations

from src.config.settings import PATHS
from src.quality.data_quality_checks import run_quality_checks
from src.transforms.device_transforms import (
    REQUIRED_COLUMNS as DEVICE_REQUIRED_COLUMNS,
    normalize_devices,
    read_raw_devices,
)
from src.transforms.page_log_transforms import (
    REQUIRED_COLUMNS as PAGE_LOG_REQUIRED_COLUMNS,
    enrich_with_devices,
    normalize_page_logs,
    read_raw_page_logs,
)
from src.utils.logging import get_logger
from src.utils.paths import as_posix, ensure_dir
from src.utils.spark import get_spark

log = get_logger("ingest_page_logs")

NON_NEGATIVE_COLUMNS = ["lcp_ms", "cls", "inp_ms", "ttfb_ms", "fcp_ms", "page_weight_kb", "js_error_count"]


def run() -> None:
    spark = get_spark("ingest-page-logs")
    try:
        log.info(f"Reading raw device metadata from {PATHS.raw_devices}")
        raw_devices_df = read_raw_devices(spark, PATHS.raw_devices)
        devices_df = run_quality_checks(
            normalize_devices(raw_devices_df),
            table_name="devices",
            required_columns=DEVICE_REQUIRED_COLUMNS,
            check_route=False,
            check_device_type=False,
        )
        devices_out = ensure_dir(PATHS.processed / "devices")
        devices_df.write.mode("overwrite").parquet(as_posix(devices_out))
        log.info(f"Wrote {devices_df.count()} normalized device records to {devices_out}")

        log.info(f"Reading raw page load logs from {PATHS.raw_page_load_logs}")
        raw_logs_df = read_raw_page_logs(spark, PATHS.raw_page_load_logs)
        raw_count = raw_logs_df.count()
        log.info(f"Read {raw_count} raw page load log events")

        normalized_df = normalize_page_logs(raw_logs_df)
        enriched_df = enrich_with_devices(normalized_df, devices_df)
        clean_df = run_quality_checks(
            enriched_df,
            table_name="page_load_logs",
            required_columns=PAGE_LOG_REQUIRED_COLUMNS,
            non_negative_columns=NON_NEGATIVE_COLUMNS,
        )

        out_dir = ensure_dir(PATHS.processed / "page_logs")
        clean_df.write.mode("overwrite").partitionBy("event_date").parquet(as_posix(out_dir))
        log.info(f"Wrote {clean_df.count()} normalized page load log events to {out_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
