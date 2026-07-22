from datetime import datetime

from src.transforms.lighthouse_transforms import normalize_lighthouse
from src.transforms.normalization import standardize_device_type, standardize_route


def test_standardize_route_resolves_aliases_and_trims(spark):
    cases = [
        ("/home", "/"),
        ("/Products?ref=nav", "/product"),
        ("/checkout/", "/checkout"),
        ("/BLOG", "/blog"),
        ("/pdp", "/product"),
    ]
    df = spark.createDataFrame([(raw,) for raw, _ in cases], ["route"])
    result = [row.route for row in standardize_route(df).collect()]
    assert result == [expected for _, expected in cases]


def test_standardize_route_keeps_root_slash(spark):
    df = spark.createDataFrame([("/",)], ["route"])
    result = standardize_route(df).collect()[0].route
    assert result == "/"


def test_standardize_device_type_resolves_aliases(spark):
    df = spark.createDataFrame(
        [("phone",), ("PC",), ("iPad",), ("desktop",), ("smartwatch",)],
        ["device_type"],
    )
    rows = {row.device_type for row in standardize_device_type(df).collect()}
    assert rows == {"mobile", "desktop", "tablet", "unknown"}


def test_normalize_lighthouse_filters_non_production_and_casts_types(spark):
    df = spark.createDataFrame(
        [
            ("lh-1", "2026-06-01T05:00:00Z", "/Home", "phone", "production", "v1.0.0", 1800.0, 0.03, 140.0, 400.0),
            ("lh-2", "2026-06-01T05:05:00Z", "/home", "phone", "staging", "v1.0.0", 1800.0, 0.03, 140.0, 400.0),
        ],
        ["report_id", "run_timestamp", "route", "device_type", "environment", "release_version",
         "lcp_ms", "cls", "inp_ms", "ttfb_ms"],
    )
    result = normalize_lighthouse(df).collect()
    assert len(result) == 1
    row = result[0]
    assert row.route == "/"
    assert row.device_type == "mobile"
    assert isinstance(row.run_timestamp, datetime)
    assert row.report_date is not None
