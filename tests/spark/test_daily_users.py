"""Tests for the daily distinct-users aggregation (batch DataFrames, UTC day)."""

from datetime import date

from spark_events import init, match, purchase

from pipeline.spark.aggregations import daily_users, daily_users_all_days
from pipeline.spark.parsing import parse_clean_events, to_participants

DAY = date(2023, 11, 14)
DAY_START = 1699920000  # 2023-11-14 00:00:00 UTC
DAY_END = DAY_START + 86400  # 2023-11-15 00:00:00 UTC


def _daily(make_events, events, day=DAY):
    return daily_users(to_participants(parse_clean_events(make_events(events))), day)


def _as_dict(frame):
    return {(r.country, r.platform): r.distinct_users for r in frame.collect()}


def test_daily_users_by_country_platform(make_events):
    result = _daily(
        make_events,
        [
            init("A", "TR", "ANDROID", t=DAY_START + 10),
            purchase("A", 1.99, country="TR", t=DAY_START + 20),  # same user, other platform value
            init("A", "TR", "ANDROID", t=DAY_START + 30),  # third event of A
            init("B", "TR", "ANDROID", t=DAY_START + 40),
            init("C", "GB", "IOS", t=DAY_START + 50),
        ],
    )
    assert result.columns == ["day", "country", "platform", "distinct_users"]
    assert dict(result.dtypes)["day"] == "date"
    got = _as_dict(result)
    # A: 2 ANDROID inits (counted once) + 1 IOS purchase; B: ANDROID; C: GB IOS
    assert got == {("TR", "ANDROID"): 2, ("TR", "IOS"): 1, ("GB", "IOS"): 1}
    assert {r.day for r in result.collect()} == {DAY}


def test_daily_users_utc_boundaries(make_events):
    events = [
        init("last", "TR", "ANDROID", t=DAY_START - 1),  # 2023-11-13 23:59:59 UTC
        init("first", "TR", "ANDROID", t=DAY_START),  # 2023-11-14 00:00:00 UTC
        init("end", "TR", "ANDROID", t=DAY_END - 1),  # 2023-11-14 23:59:59 UTC
        init("next", "TR", "ANDROID", t=DAY_END),  # 2023-11-15 00:00:00 UTC
    ]
    assert _as_dict(_daily(make_events, events, DAY)) == {("TR", "ANDROID"): 2}
    assert _as_dict(_daily(make_events, events, date(2023, 11, 13))) == {("TR", "ANDROID"): 1}
    assert _as_dict(_daily(make_events, events, date(2023, 11, 15))) == {("TR", "ANDROID"): 1}


def test_daily_users_includes_both_match_players(make_events):
    result = _daily(make_events, [match("a", "b", "TR", "GB", pa="ANDROID", pb="IOS", t=DAY_START + 5)])
    assert _as_dict(result) == {("TR", "ANDROID"): 1, ("GB", "IOS"): 1}


def test_daily_users_uses_enriched_user_b_platform(make_events):
    """Without user-b post-match info, the enriched ``user-b-platform`` is still counted."""
    result = _daily(
        make_events, [match("a", "b", "TR", "GB", pa="ANDROID", pb="IOS", t=DAY_START + 5, user_b_info=False)]
    )
    assert _as_dict(result) == {("TR", "ANDROID"): 1, ("GB", "IOS"): 1}


def test_daily_users_enriched_unknown_platform_stays_unknown(make_events):
    result = _daily(
        make_events, [match("a", "b", "TR", "GB", pa="ANDROID", pb="UNKNOWN", t=DAY_START + 5, user_b_info=False)]
    )
    assert _as_dict(result) == {("TR", "ANDROID"): 1, ("GB", "UNKNOWN"): 1}


def test_daily_users_empty_day_has_no_rows(make_events):
    assert _daily(make_events, [init("A", t=DAY_START + 1)], date(2000, 1, 1)).collect() == []


def test_daily_users_all_days_reports_every_utc_day(make_events):
    events = [
        init("A", "TR", "ANDROID", t=DAY_START - 1),  # 2023-11-13 23:59:59 UTC
        init("A", "TR", "ANDROID", t=DAY_START + 5),  # same user, next day
        init("B", "TR", "ANDROID", t=DAY_START + 6),
        init("C", "GB", "IOS", t=DAY_END + 1),  # 2023-11-15
    ]
    result = daily_users_all_days(to_participants(parse_clean_events(make_events(events))))
    assert result.columns == ["day", "country", "platform", "distinct_users"]
    got = {(r.day, r.country, r.platform): r.distinct_users for r in result.collect()}
    assert got == {
        (date(2023, 11, 13), "TR", "ANDROID"): 1,
        (DAY, "TR", "ANDROID"): 2,
        (date(2023, 11, 15), "GB", "IOS"): 1,
    }


def test_daily_users_all_days_matches_single_day_result(make_events):
    events = [
        init("A", "TR", "ANDROID", t=DAY_START + 10),
        match("A", "B", "TR", "GB", t=DAY_START + 20),
        purchase("C", 4.99, country="GB", t=DAY_START + 30),
        init("D", "TR", "IOS", t=DAY_END + 30),
    ]
    participants = to_participants(parse_clean_events(make_events(events)))
    single = {(r.country, r.platform): r.distinct_users for r in daily_users(participants, DAY).collect()}
    every = {
        (r.country, r.platform): r.distinct_users
        for r in daily_users_all_days(participants).collect()
        if r.day == DAY
    }
    assert every == single
