from src.transforms.regression_transforms import (
    build_before_after_comparisons,
    build_release_aggregates,
    build_release_pairs,
    detect_regressions,
)

PAGE_LOG_COLUMNS = ["route", "device_type", "release_version", "event_date", "lcp_ms", "cls", "inp_ms", "ttfb_ms"]


def _page_log_rows(spark, rows):
    return spark.createDataFrame(rows, PAGE_LOG_COLUMNS)


def test_release_pairs_link_consecutive_releases_by_first_seen_date(spark):
    rows = [
        ("/checkout", "desktop", "v1.0.0", "2026-06-01", 1900.0, 0.04, 150.0, 500.0),
        ("/checkout", "desktop", "v1.1.0", "2026-06-08", 2700.0, 0.13, 150.0, 500.0),
        ("/checkout", "desktop", "v1.2.0", "2026-06-15", 1600.0, 0.03, 150.0, 500.0),
    ]
    df = _page_log_rows(spark, rows)
    aggregates = build_release_aggregates(df)
    pairs = build_release_pairs(aggregates).collect()

    versions = {(r.before_release, r.after_release) for r in pairs}
    assert versions == {("v1.0.0", "v1.1.0"), ("v1.1.0", "v1.2.0")}


def test_detect_regressions_flags_lcp_and_cls_over_threshold(spark):
    rows = [
        ("/checkout", "desktop", "v1.0.0", "2026-06-01", 1900.0, 0.04, 150.0, 500.0),
        ("/checkout", "desktop", "v1.1.0", "2026-06-08", 2700.0, 0.13, 150.0, 500.0),
    ]
    df = _page_log_rows(spark, rows)
    pairs = build_release_pairs(build_release_aggregates(df))
    events = detect_regressions(pairs).collect()

    flagged_metrics = {e.metric for e in events}
    assert "LCP" in flagged_metrics
    assert "CLS" in flagged_metrics
    assert "INP" not in flagged_metrics
    assert "TTFB" not in flagged_metrics

    lcp_event = next(e for e in events if e.metric == "LCP")
    assert lcp_event.severity in {"medium", "high", "critical"}
    assert lcp_event.baseline_value == 1900.0
    assert lcp_event.current_value == 2700.0


def test_detect_regressions_does_not_flag_small_changes(spark):
    rows = [
        ("/", "desktop", "v1.0.0", "2026-06-01", 1800.0, 0.03, 140.0, 450.0),
        ("/", "desktop", "v1.1.0", "2026-06-08", 1850.0, 0.031, 145.0, 460.0),
    ]
    df = _page_log_rows(spark, rows)
    pairs = build_release_pairs(build_release_aggregates(df))
    events = detect_regressions(pairs).collect()
    assert events == []


def test_before_after_comparison_status_classification(spark):
    rows = [
        # regressed on checkout
        ("/checkout", "desktop", "v1.0.0", "2026-06-01", 1900.0, 0.04, 150.0, 500.0),
        ("/checkout", "desktop", "v1.1.0", "2026-06-08", 2700.0, 0.13, 150.0, 500.0),
        # improved on checkout (next release fixes it)
        ("/checkout", "desktop", "v1.2.0", "2026-06-15", 1500.0, 0.02, 140.0, 480.0),
        # stable on home
        ("/", "desktop", "v1.0.0", "2026-06-01", 1800.0, 0.03, 140.0, 450.0),
        ("/", "desktop", "v1.1.0", "2026-06-08", 1820.0, 0.031, 142.0, 455.0),
    ]
    df = _page_log_rows(spark, rows)
    pairs = build_release_pairs(build_release_aggregates(df))
    comparisons = {
        (r.route, r.before_release, r.after_release): r.improvement_status
        for r in build_before_after_comparisons(pairs).collect()
    }

    assert comparisons[("/checkout", "v1.0.0", "v1.1.0")] == "regressed"
    assert comparisons[("/checkout", "v1.1.0", "v1.2.0")] == "improved"
    assert comparisons[("/", "v1.0.0", "v1.1.0")] == "stable"
