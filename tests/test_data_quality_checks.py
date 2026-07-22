import pytest

from src.quality.data_quality_checks import (
    count_negative,
    filter_non_negative,
    filter_route_not_null,
    filter_sample_count_positive,
    filter_valid_device_type,
    missing_required_columns,
    run_quality_checks,
)


def test_missing_required_columns_detects_gaps(spark):
    df = spark.createDataFrame([(1, "a")], ["route", "device_type"])
    missing = missing_required_columns(df, ["route", "device_type", "lcp_ms"])
    assert missing == ["lcp_ms"]


def test_missing_required_columns_empty_when_satisfied(spark):
    df = spark.createDataFrame([(1, "a")], ["route", "device_type"])
    assert missing_required_columns(df, ["route", "device_type"]) == []


def test_count_negative_counts_only_negative_non_null_values(spark):
    df = spark.createDataFrame([(1.0,), (-1.0,), (None,), (-5.0,)], ["lcp_ms"])
    assert count_negative(df, "lcp_ms") == 2


def test_filter_non_negative_drops_negative_rows(spark):
    df = spark.createDataFrame([(1.0, 0.02), (-100.0, 0.02), (100.0, -0.01)], ["lcp_ms", "cls"])
    result = filter_non_negative(df, ["lcp_ms", "cls"]).collect()
    assert len(result) == 1
    assert result[0].lcp_ms == 1.0


def test_filter_route_not_null_drops_nulls(spark):
    df = spark.createDataFrame([("/",), (None,), ("/product",)], ["route"])
    result = [r.route for r in filter_route_not_null(df).collect()]
    assert result == ["/", "/product"]


def test_filter_valid_device_type_keeps_only_known_values(spark):
    df = spark.createDataFrame([("mobile",), ("desktop",), ("tablet",), ("unknown",), ("smartwatch",)], ["device_type"])
    result = {r.device_type for r in filter_valid_device_type(df).collect()}
    assert result == {"mobile", "desktop", "tablet"}


def test_filter_sample_count_positive_drops_zero_and_negative(spark):
    df = spark.createDataFrame([(10,), (0,), (-3,)], ["sample_count"])
    result = [r.sample_count for r in filter_sample_count_positive(df).collect()]
    assert result == [10]


def test_run_quality_checks_raises_on_missing_required_column(spark):
    df = spark.createDataFrame([("/", "mobile")], ["route", "device_type"])
    with pytest.raises(ValueError):
        run_quality_checks(df, "test_table", required_columns=["route", "lcp_ms"])


def test_run_quality_checks_filters_bad_rows(spark):
    df = spark.createDataFrame(
        [("/", "mobile", 1800.0), (None, "mobile", 1800.0), ("/product", "smartwatch", 1800.0), ("/checkout", "desktop", -5.0)],
        ["route", "device_type", "lcp_ms"],
    )
    clean = run_quality_checks(
        df, "test_table", required_columns=["route", "device_type", "lcp_ms"], non_negative_columns=["lcp_ms"],
    )
    result = clean.collect()
    assert len(result) == 1
    assert result[0].route == "/"
