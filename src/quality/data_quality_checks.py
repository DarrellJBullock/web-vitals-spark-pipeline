"""Data quality checks applied at pipeline stage 8, before curated tables are
built. Individual checks are pure functions (easy to unit test in isolation);
run_quality_checks() is the orchestrator jobs call.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.settings import VALID_DEVICE_TYPES
from src.utils.logging import get_logger

log = get_logger("data_quality_checks")


def missing_required_columns(df: DataFrame, required: list[str]) -> list[str]:
    """Returns any columns in `required` that are absent from the DataFrame."""
    present = set(df.columns)
    return [c for c in required if c not in present]


def count_negative(df: DataFrame, column: str) -> int:
    """Counts rows where `column` is non-null and negative."""
    return df.where(F.col(column).isNotNull() & (F.col(column) < 0)).count()


def filter_non_negative(df: DataFrame, columns: list[str]) -> DataFrame:
    """Drops rows where any of `columns` is negative (nulls are left alone --
    that's a separate completeness concern)."""
    condition = None
    for column in columns:
        clause = F.col(column).isNull() | (F.col(column) >= 0)
        condition = clause if condition is None else (condition & clause)
    return df.where(condition) if condition is not None else df


def filter_route_not_null(df: DataFrame, column: str = "route") -> DataFrame:
    return df.where(F.col(column).isNotNull())


def filter_valid_device_type(df: DataFrame, column: str = "device_type") -> DataFrame:
    return df.where(F.col(column).isin(*VALID_DEVICE_TYPES))


def filter_sample_count_positive(df: DataFrame, column: str = "sample_count") -> DataFrame:
    return df.where(F.col(column) > 0)


def run_quality_checks(
    df: DataFrame,
    table_name: str,
    required_columns: list[str],
    non_negative_columns: list[str] | None = None,
    check_route: bool = True,
    check_device_type: bool = True,
    check_sample_count: bool = False,
) -> DataFrame:
    """Runs the full stage-8 quality gate for a raw/curated DataFrame, logging
    what it dropped, and returns the cleaned DataFrame."""
    missing = missing_required_columns(df, required_columns)
    if missing:
        raise ValueError(f"[{table_name}] missing required columns: {missing}")

    clean = df
    before = clean.count()

    if non_negative_columns:
        clean = filter_non_negative(clean, non_negative_columns)
    if check_route and "route" in clean.columns:
        clean = filter_route_not_null(clean)
    if check_device_type and "device_type" in clean.columns:
        clean = filter_valid_device_type(clean)
    if check_sample_count and "sample_count" in clean.columns:
        clean = filter_sample_count_positive(clean)

    after = clean.count()
    dropped = before - after
    if dropped:
        log.warning(f"[{table_name}] quality checks dropped {dropped}/{before} rows")
    else:
        log.info(f"[{table_name}] quality checks passed on all {before} rows")
    return clean
