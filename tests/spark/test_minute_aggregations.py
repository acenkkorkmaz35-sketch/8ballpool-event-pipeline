"""Tests for the per-minute aggregations on small fixed inputs (batch DataFrames)."""

import pytest
from spark_events import T0, init, match, purchase

from pipeline.spark.aggregations import matches_by_country, minute_summary, revenue_by_country
from pipeline.spark.parsing import parse_clean_events, to_participants

M1 = T0 + 60


def ts(spark, epoch: int):
    """Epoch seconds as a naive datetime in the session time zone (UTC)."""
    return spark.sql(f"SELECT timestamp_seconds({epoch}) AS t").collect()[0].t


def test_minute_summary(spark, make_events):
    events = parse_clean_events(
        make_events(
            [
                purchase("A", 1.99, t=T0 + 5),
                purchase("A", 3.00, t=T0 + 40),
                init("B", t=T0 + 10),
                match("C", "D", "TR", "GB", t=T0 + 20),
                init("A", t=M1 + 10),
            ]
        )
    )
    rows = {r.window_start: r for r in minute_summary(to_participants(events)).collect()}

    first, second = rows[ts(spark, T0)], rows[ts(spark, M1)]
    assert first.window_end == ts(spark, M1)
    assert first.purchases_count == 2
    assert first.revenue_sum == pytest.approx(4.99)
    assert first.distinct_users == 4
    assert second.purchases_count == 0
    assert second.revenue_sum == 0.0  # 0.0, not null, for a window without purchases
    assert second.distinct_users == 1


def test_minute_summary_columns(make_events):
    summary = minute_summary(to_participants(parse_clean_events(make_events([init("A")]))))
    assert summary.columns == ["window_start", "window_end", "purchases_count", "revenue_sum", "distinct_users"]
    assert dict(summary.dtypes)["window_start"] == "timestamp"


def test_revenue_by_country(spark, make_events):
    events = parse_clean_events(
        make_events(
            [
                purchase("A", 1.99, country="TR", t=T0 + 5),
                purchase("B", 3.00, country="TR", t=T0 + 30),
                purchase("C", 5.00, country="GB", t=T0 + 31),
                purchase("A", 2.00, country="TR", t=M1 + 1),
                init("D", t=T0 + 2),  # not a purchase: ignored
            ]
        )
    )
    result = revenue_by_country(to_participants(events))
    assert result.columns == ["window_start", "country", "revenue_sum"]
    got = {(r.window_start, r.country): r.revenue_sum for r in result.collect()}
    assert got.keys() == {(ts(spark, T0), "TR"), (ts(spark, T0), "GB"), (ts(spark, M1), "TR")}
    assert got[(ts(spark, T0), "TR")] == pytest.approx(4.99)
    assert got[(ts(spark, T0), "GB")] == pytest.approx(5.00)
    assert got[(ts(spark, M1), "TR")] == pytest.approx(2.00)


def test_matches_by_country_counts_once_per_distinct_country(spark, make_events):
    events = parse_clean_events(
        make_events(
            [
                match("a", "b", "TR", "GB", t=T0 + 10),  # TR +1, GB +1
                match("c", "d", "TR", "TR", t=T0 + 20),  # TR +1 (once, not twice)
            ]
        )
    )
    result = matches_by_country(events)
    assert result.columns == ["window_start", "country", "matches_count"]
    got = {(r.window_start, r.country): r.matches_count for r in result.collect()}
    assert got == {(ts(spark, T0), "TR"): 2, (ts(spark, T0), "GB"): 1}


def test_matches_by_country_ignores_non_matches_and_maps_null_to_unknown(make_events):
    broken = match("a", "b", "TR", "GB", t=T0 + 10)
    del broken["user-b-country"]
    events = parse_clean_events(make_events([broken, init("x", t=T0 + 3)]))
    got = {r.country: r.matches_count for r in matches_by_country(events).collect()}
    assert got == {"TR": 1, "UNKNOWN": 1}
