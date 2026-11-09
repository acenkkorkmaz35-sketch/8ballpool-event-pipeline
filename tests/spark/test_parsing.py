"""Tests for parsing clean-events JSON into flat columns and participant rows."""

import pytest
from spark_events import T0, init, match, purchase

from pipeline.spark.parsing import parse_clean_events, to_participants


def test_parse_init(make_events):
    row = parse_clean_events(make_events([init("u1")])).collect()[0]
    assert row.event_type == "init"
    assert row.user_id == "u1"
    assert (row.country, row.platform) == ("TR", "ANDROID")
    assert row.purchase_value is None
    assert row.user_a is None and row.user_a_country is None and row.user_b_platform is None


def test_parse_event_time_is_timestamp_from_epoch_seconds(make_events):
    df = parse_clean_events(make_events([init("u1", t=T0 + 5)]))
    assert dict(df.dtypes)["event_time"] == "timestamp"
    text = df.selectExpr("date_format(event_time, 'yyyy-MM-dd HH:mm:ss') AS s").collect()[0].s
    assert text == "2023-11-14 22:14:05"


def test_parse_purchase_value_is_double(make_events):
    df = parse_clean_events(make_events([purchase("u1", 3)]))  # JSON integer 3
    assert dict(df.dtypes)["purchase_value"] == "double"
    assert df.collect()[0].purchase_value == pytest.approx(3.0)


def test_parse_match_with_both_users(make_events):
    row = parse_clean_events(make_events([match("a", "b", "TR", "GB")])).collect()[0]
    assert (row.user_a, row.user_b) == ("a", "b")
    assert (row.user_a_country, row.user_b_country) == ("TR", "GB")
    assert (row.user_a_platform, row.user_b_platform) == ("ANDROID", "IOS")
    assert row.user_id is None


def test_parse_match_platforms_come_from_enriched_fields(make_events):
    """The post-match infos (decoy platform) are ignored in favour of ``user-X-platform``."""
    row = parse_clean_events(make_events([match("a", "b", "TR", "GB", pa="IOS", pb="ANDROID")])).collect()[0]
    assert (row.user_a_platform, row.user_b_platform) == ("IOS", "ANDROID")


def test_parse_match_without_user_b_info_keeps_enriched_platform(make_events):
    row = parse_clean_events(make_events([match("a", "b", "TR", "GB", user_b_info=False)])).collect()[0]
    assert row.user_a_platform == "ANDROID"
    assert row.user_b_platform == "IOS"


def test_participants_match_explodes_to_two_rows(make_events):
    events = parse_clean_events(make_events([match("a", "b", "TR", "GB")]))
    rows = {r.user_id: r for r in to_participants(events).collect()}
    assert set(rows) == {"a", "b"}
    assert (rows["a"].country, rows["a"].platform) == ("TR", "ANDROID")
    assert (rows["b"].country, rows["b"].platform) == ("GB", "IOS")
    assert rows["a"].event_type == "match" and rows["a"].purchase_value is None


def test_participants_enriched_unknown_platform_stays_unknown(make_events):
    events = parse_clean_events(make_events([match("a", "b", "TR", "GB", pb="UNKNOWN", user_b_info=False)]))
    rows = {r.user_id: r for r in to_participants(events).collect()}
    assert rows["a"].platform == "ANDROID"
    assert rows["b"].platform == "UNKNOWN"


def test_participants_missing_enriched_platform_defaults_to_unknown(make_events):
    """Defensive: an event lacking the enriched field (older clean data) maps to UNKNOWN."""
    event = match("a", "b", "TR", "GB")
    del event["user-b-platform"]
    rows = {r.user_id: r for r in to_participants(parse_clean_events(make_events([event]))).collect()}
    assert rows["b"].platform == "UNKNOWN"


def test_participants_init_and_purchase_are_single_rows(make_events):
    events = parse_clean_events(make_events([init("u1"), purchase("u2", 1.99, country="GB")]))
    rows = {r.user_id: r for r in to_participants(events).collect()}
    assert set(rows) == {"u1", "u2"}
    assert rows["u1"].purchase_value is None
    assert rows["u2"].purchase_value == pytest.approx(1.99)
    assert (rows["u2"].country, rows["u2"].platform) == ("GB", "IOS")


def test_participants_columns(make_events):
    events = parse_clean_events(make_events([init("u1")]))
    assert to_participants(events).columns == [
        "event_time",
        "event_type",
        "user_id",
        "country",
        "platform",
        "purchase_value",
    ]
