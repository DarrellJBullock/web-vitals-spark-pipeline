"""SparkSession factory used by every job so config stays in one place."""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from src.config.settings import SPARK_SETTINGS

# Pin worker processes to the exact same interpreter as the driver. Without
# this, Spark falls back to whatever `python3` resolves to on PATH, which
# breaks (PYTHON_VERSION_MISMATCH) the moment that differs from the venv --
# e.g. a newer system Python whose cloudpickle version PySpark doesn't support.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark(app_name: str | None = None) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name or SPARK_SETTINGS.app_name)
        .master(SPARK_SETTINGS.master)
        .config("spark.sql.shuffle.partitions", SPARK_SETTINGS.shuffle_partitions)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(SPARK_SETTINGS.log_level)
    return spark
