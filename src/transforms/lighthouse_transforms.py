"""Transforms for raw Lighthouse lab-metric reports (pipeline stages 5-7)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.transforms.normalization import standardize_device_type, standardize_route
from src.utils.paths import as_posix

RAW_SCHEMA = StructType([
    StructField("report_id", StringType(), False),
    StructField("run_timestamp", StringType(), False),
    StructField("url", StringType(), True),
    StructField("route", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("environment", StringType(), True),
    StructField("git_commit", StringType(), True),
    StructField("release_version", StringType(), True),
    StructField("lcp_ms", DoubleType(), True),
    StructField("cls", DoubleType(), True),
    StructField("inp_ms", DoubleType(), True),
    StructField("fcp_ms", DoubleType(), True),
    StructField("ttfb_ms", DoubleType(), True),
    StructField("speed_index_ms", DoubleType(), True),
    StructField("total_blocking_time_ms", DoubleType(), True),
    StructField("performance_score", DoubleType(), True),
    StructField("accessibility_score", DoubleType(), True),
    StructField("best_practices_score", DoubleType(), True),
    StructField("seo_score", DoubleType(), True),
    StructField("request_count", DoubleType(), True),
    StructField("js_transfer_kb", DoubleType(), True),
    StructField("css_transfer_kb", DoubleType(), True),
    StructField("image_transfer_kb", DoubleType(), True),
    StructField("total_transfer_kb", DoubleType(), True),
])

REQUIRED_COLUMNS = [
    "report_id", "run_timestamp", "route", "device_type", "release_version",
    "lcp_ms", "cls", "inp_ms", "ttfb_ms",
]


def read_raw_lighthouse(spark: SparkSession, raw_dir) -> DataFrame:
    """Reads one JSON-array file per day (a synthetic batch export of
    per-route/per-device Lighthouse CI runs)."""
    return (
        spark.read.option("multiLine", True)
        .schema(RAW_SCHEMA)
        .json(as_posix(raw_dir))
    )


def normalize_lighthouse(df: DataFrame) -> DataFrame:
    """Stages 5-7: standardize route/device_type, cast timestamp, keep only
    production runs so lab metrics reflect what real users would experience."""
    df = standardize_route(df, "route")
    df = standardize_device_type(df, "device_type")
    df = df.withColumn("run_timestamp", F.to_timestamp("run_timestamp"))
    df = df.withColumn("report_date", F.to_date("run_timestamp"))
    df = df.filter(F.col("environment") == "production")
    return df
