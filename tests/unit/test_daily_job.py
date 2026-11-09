"""Unit tests for the daily job's argument parsing and loop logic (no Spark or Kafka needed)."""

from datetime import date

import pytest

from pipeline.spark.daily_job import loop_seconds, parse_args, run_loop


def test_parse_args_defaults_and_date():
    assert parse_args([]).date is None
    assert parse_args([]).loop is False
    assert parse_args(["--date", "2026-11-09"]).date == date(2026, 11, 9)
    assert parse_args(["--loop"]).loop is True


def test_parse_args_allow_empty_flag():
    assert parse_args([]).allow_empty is False
    assert parse_args(["--date", "2026-11-09", "--allow-empty"]).allow_empty is True


def test_parse_args_rejects_allow_empty_with_loop():
    with pytest.raises(SystemExit):
        parse_args(["--loop", "--allow-empty"])


def test_parse_args_rejects_bad_date_and_loop_with_date():
    with pytest.raises(SystemExit):
        parse_args(["--date", "09/11/2026"])
    with pytest.raises(SystemExit):
        parse_args(["--loop", "--date", "2026-11-09"])


def test_loop_recomputes_today_and_finished_day_once_after_midnight():
    days = iter([date(2026, 11, 9), date(2026, 11, 9), date(2026, 11, 10), date(2026, 11, 10)])
    computed, slept = [], []
    run_loop(lambda d: computed.append(d) or 1, 300, today=lambda: next(days), sleep=slept.append, iterations=4)
    assert computed == [
        date(2026, 11, 9),
        date(2026, 11, 9),
        date(2026, 11, 9),  # the day that just ended, recomputed once after midnight
        date(2026, 11, 10),
        date(2026, 11, 10),
    ]
    assert slept == [300, 300, 300]


def test_loop_survives_errors_and_retries_pending_day():
    days = iter([date(2026, 11, 9), date(2026, 11, 10), date(2026, 11, 10)])
    calls = []

    def compute(day):
        calls.append(day)
        if len(calls) in (1, 2):  # today fails, then the pending recomputation of it fails once more
            raise RuntimeError("boom")
        return 1

    run_loop(compute, 1, today=lambda: next(days), sleep=lambda s: None, iterations=3)
    nov9, nov10 = date(2026, 11, 9), date(2026, 11, 10)
    # it1: 9 fails | it2: 9 (pending) fails, 10 ok | it3: 9 ok, 10 ok
    assert calls == [nov9, nov9, nov10, nov9, nov10]


def test_loop_seconds_default_and_valid_values():
    assert loop_seconds({}) == 300
    assert loop_seconds({"DAILY_LOOP_SECONDS": ""}) == 300
    assert loop_seconds({"DAILY_LOOP_SECONDS": "60"}) == 60


@pytest.mark.parametrize("value", ["0", "-5", "abc", "1.5", "5m"])
def test_loop_seconds_rejects_non_positive_or_non_integer(value):
    with pytest.raises(ValueError, match="DAILY_LOOP_SECONDS"):
        loop_seconds({"DAILY_LOOP_SECONDS": value})
