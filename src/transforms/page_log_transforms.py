"""Transforms for synthetic browser page-load logs (pipeline stages 5-7)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.transforms.normalization import standardize_device_type, standardize_route
from src.utils.paths import as_posix

RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("timestamp", StringType(), False),
    StructField("route", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("device_id", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("os", StringType(), True),
    StructField("country", StringType(), True),
    StructField("connection_type", StringType(), True),
    StructField("lcp_ms", DoubleType(), True),
    StructField("cls", DoubleType(), True),
    StructField("inp_ms", DoubleType(), True),
    StructField("ttfb_ms", DoubleType(), True),
    StructField("fcp_ms", DoubleType(), True),
    StructField("page_weight_kb", DoubleType(), True),
    StructField("js_error_count", IntegerType(), True),
    StructField("api_latency_ms", DoubleType(), True),
    StructField("cache_status", StringType(), True),
    StructField("release_version", StringType(), True),
])

REQUIRED_COLUMNS = [
    "event_id", "timestamp", "route", "device_type", "lcp_ms", "cls", "inp_ms",
    "ttfb_ms", "release_version",
]


def read_raw_page_logs(spark: SparkSession, raw_dir) -> DataFrame:
    return spark.read.schema(RAW_SCHEMA).json(as_posix(raw_dir))


def normalize_page_logs(df: DataFrame) -> DataFrame:
    df = standardize_route(df, "route")
    df = standardize_device_type(df, "device_type")
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    df = df.withColumn("event_date", F.to_date("timestamp"))
    return df


def enrich_with_devices(df: DataFrame, devices_df: DataFrame) -> DataFrame:
    """Joins in device_class/network_profile so downstream jobs can build the
    device_breakdowns curated table without re-deriving device tiering."""
    devices_slim = devices_df.select(
        F.col("device_id"),
        F.col("device_class"),
        F.col("network_profile"),
    )
    return df.join(devices_slim, on="device_id", how="left")
