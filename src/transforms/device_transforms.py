"""Transforms for the device/network metadata dimension table."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.utils.paths import as_posix

RAW_SCHEMA = StructType([
    StructField("device_id", StringType(), False),
    StructField("device_class", StringType(), True),
    StructField("device_name", StringType(), True),
    StructField("viewport_width", IntegerType(), True),
    StructField("viewport_height", IntegerType(), True),
    StructField("cpu_tier", StringType(), True),
    StructField("memory_gb", IntegerType(), True),
    StructField("network_profile", StringType(), True),
])

REQUIRED_COLUMNS = ["device_id", "device_class"]


def read_raw_devices(spark: SparkSession, raw_dir) -> DataFrame:
    return (
        spark.read.option("multiLine", True)
        .schema(RAW_SCHEMA)
        .json(as_posix(raw_dir))
    )


def normalize_devices(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["device_id"])
