from src.transforms.vitals_transforms import add_health_score, add_risk_level

DAILY_COLUMNS = ["avg_lcp_ms", "avg_cls", "avg_inp_ms", "avg_ttfb_ms", "sample_count"]


def test_health_score_is_high_for_all_good_metrics_with_enough_samples(spark):
    df = spark.createDataFrame([(1800.0, 0.03, 140.0, 400.0, 100)], DAILY_COLUMNS)
    row = add_health_score(df).collect()[0]
    assert row.overall_health_score >= 90.0


def test_health_score_drops_for_poor_metrics(spark):
    df = spark.createDataFrame([(5000.0, 0.25, 400.0, 1200.0, 100)], DAILY_COLUMNS)
    row = add_health_score(df).collect()[0]
    assert row.overall_health_score < 30.0


def test_health_score_is_discounted_for_low_sample_count(spark):
    df = spark.createDataFrame(
        [(1800.0, 0.03, 140.0, 400.0, 100), (1800.0, 0.03, 140.0, 400.0, 3)],
        DAILY_COLUMNS,
    )
    rows = {row.sample_count: row.overall_health_score for row in add_health_score(df).collect()}
    assert rows[3] < rows[100]


def test_risk_level_classification(spark):
    df = spark.createDataFrame(
        [
            (1800.0, 0.03, 140.0),   # all good -> low
            (2600.0, 0.03, 140.0),   # one needs-improvement -> medium
            (5000.0, 0.03, 140.0),   # one poor -> high
            (5000.0, 0.25, 400.0),   # multiple poor -> critical
        ],
        ["p75_lcp_ms", "p75_cls", "p75_inp_ms"],
    )
    result = [r.risk_level for r in add_risk_level(df, "p75_lcp_ms", "p75_cls", "p75_inp_ms").collect()]
    assert result == ["low", "medium", "high", "critical"]
