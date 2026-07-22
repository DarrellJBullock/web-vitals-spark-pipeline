"""Shared route/device-type standardization used by every ingest transform
(pipeline stages 5 and 6). Kept separate from vitals_transforms.py, which
handles stage 7 (normalizing the metric fields themselves).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.config.settings import VALID_DEVICE_TYPES

# Maps loose/legacy spellings seen across data sources onto the canonical
# route taxonomy used everywhere downstream.
ROUTE_ALIASES = {
    "/home": "/",
    "/index": "/",
    "/homepage": "/",
    "/products": "/product",
    "/pdp": "/product",
    "/cart": "/checkout",
    "/basket": "/checkout",
    "/signin": "/account",
    "/login": "/account",
    "/profile": "/account",
}

DEVICE_TYPE_ALIASES = {
    "phone": "mobile",
    "smartphone": "mobile",
    "handset": "mobile",
    "pc": "desktop",
    "laptop": "desktop",
    "ipad": "tablet",
}


def _clean_route_expr(col: Column) -> Column:
    # Strip query string / fragment, lowercase, drop a trailing slash (but keep
    # a bare "/"), and ensure a single leading slash.
    stripped = F.regexp_extract(col, r"^([^?#]*)", 1)
    lowered = F.lower(F.trim(stripped))
    with_leading_slash = F.when(lowered.startswith("/"), lowered).otherwise(F.concat(F.lit("/"), lowered))
    no_trailing_slash = F.when(
        (F.length(with_leading_slash) > 1) & with_leading_slash.endswith("/"),
        with_leading_slash.substr(F.lit(1), F.length(with_leading_slash) - 1),
    ).otherwise(with_leading_slash)
    return no_trailing_slash


def standardize_route(df: DataFrame, column: str = "route") -> DataFrame:
    """Normalizes a route column to a consistent, lowercase, alias-resolved form."""
    cleaned = df.withColumn(
        "_clean_route",
        _clean_route_expr(F.col(column)),
    )
    alias_map = F.create_map([F.lit(x) for pair in ROUTE_ALIASES.items() for x in pair])
    resolved = cleaned.withColumn(
        column,
        F.coalesce(alias_map[F.col("_clean_route")], F.col("_clean_route")),
    ).drop("_clean_route")
    return resolved


def standardize_device_type(df: DataFrame, column: str = "device_type") -> DataFrame:
    """Normalizes device_type to one of mobile/desktop/tablet."""
    cleaned = df.withColumn(column, F.lower(F.trim(F.col(column))))
    alias_map = F.create_map([F.lit(x) for pair in DEVICE_TYPE_ALIASES.items() for x in pair])
    resolved = cleaned.withColumn(
        column,
        F.coalesce(alias_map[F.col(column)], F.col(column)),
    )
    return resolved.withColumn(
        column,
        F.when(F.col(column).isin(*VALID_DEVICE_TYPES), F.col(column)).otherwise(F.lit("unknown")),
    )
