"""Transforms for raw CrUX-like field metrics (pipeline stages 5-7)."""
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
    StructField("date", StringType(), False),
    StructField("origin", StringType(), True),
    StructField("route", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("connection_type", StringType(), True),
    StructField("p75_lcp_ms", DoubleType(), True),
    StructField("p75_cls", DoubleType(), True),
    StructField("p75_inp_ms", DoubleType(), True),
    StructField("good_lcp_rate", DoubleType(), True),
    StructField("needs_improvement_lcp_rate", DoubleType(), True),
    StructField("poor_lcp_rate", DoubleType(), True),
    StructField("good_cls_rate", DoubleType(), True),
    StructField("needs_improvement_cls_rate", DoubleType(), True),
    StructField("poor_cls_rate", DoubleType(), True),
    StructField("good_inp_rate", DoubleType(), True),
    StructField("needs_improvement_inp_rate", DoubleType(), True),
    StructField("poor_inp_rate", DoubleType(), True),
    StructField("sample_count", IntegerType(), True),
])

REQUIRED_COLUMNS = [
    "date", "route", "device_type", "p75_lcp_ms", "p75_cls", "p75_inp_ms", "sample_count",
]


def read_raw_crux(spark: SparkSession, raw_dir) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(RAW_SCHEMA)
        .csv(as_posix(raw_dir))
    )


def normalize_crux(df: DataFrame) -> DataFrame:
    df = standardize_route(df, "route")
    df = standardize_device_type(df, "device_type")
    df = df.withColumn("date", F.to_date("date"))
    return df
