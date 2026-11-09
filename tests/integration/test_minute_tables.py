"""End-to-end test of the live minute aggregator and the Postgres sink.

Requires the whole stack (``docker compose up -d --build``: kafka, dq-service, generator,
spark-streaming, postgres) and runs from the ``tests`` compose service::

    docker compose --profile test run --rm tests pytest tests/integration/test_minute_tables.py -v

The first test polls Postgres (no fixed sleep) until the streaming job has produced rows in
all three minute tables; the others exercise the sink functions against the live database
using sentinel keys far in the past, which they delete again.
"""

import time
import uuid
from datetime import date, datetime, timezone

import psycopg
import pytest

from pipeline.settings import Settings
from pipeline.spark.sink import replace_day, upsert_rows

pytestmark = pytest.mark.integration

DSN = Settings.from_env().postgres_dsn
TIMEOUT_S = 240
FULL_COUNTRY_NAMES = {"Turkey", "United Kingdom", "United States"}
MINUTE_TABLES = ("minute_summary", "minute_revenue_by_country", "minute_matches_by_country")


def _query(sql: str, params=()) -> list[tuple]:
    with psycopg.connect(DSN) as conn:
        return conn.execute(sql, params).fetchall()


def _wait_until(check, timeout_s: float = TIMEOUT_S, interval_s: float = 5.0):
    """Poll ``check()`` until it returns a truthy value or the timeout elapses."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            result = check()
        except (psycopg.OperationalError, psycopg.errors.UndefinedTable):  # postgres/schema not ready yet
            result = None
        if result or time.monotonic() >= deadline:
            return result
        time.sleep(interval_s)


def _snapshot() -> dict:
    counts = {t: _query(f"SELECT count(*) FROM {t}")[0][0] for t in MINUTE_TABLES}  # fixed names
    max_users = _query("SELECT coalesce(max(distinct_users), 0) FROM minute_summary")[0][0]
    countries = {r[0] for r in _query("SELECT DISTINCT country FROM minute_revenue_by_country")}
    return {"counts": counts, "max_distinct_users": max_users, "countries": countries}


def test_minute_tables_fill():
    """Each minute table gets rows; distinct_users > 0; countries are full names."""

    def ready():
        snap = _snapshot()
        ok = (
            all(n >= 1 for n in snap["counts"].values())
            and snap["max_distinct_users"] > 0
            and snap["countries"] & FULL_COUNTRY_NAMES
        )
        return snap if ok else None

    snap = _wait_until(ready)
    final = snap or _snapshot()
    assert all(n >= 1 for n in final["counts"].values()), f"empty minute table(s): {final['counts']}"
    assert final["max_distinct_users"] > 0
    assert final["countries"] & FULL_COUNTRY_NAMES, f"no full country name among {final['countries']}"


def test_upsert_rows_is_idempotent_and_keeps_latest_values():
    window = datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc)  # sentinel, far from live data
    country = f"TEST-{uuid.uuid4().hex[:8]}"
    key = ["window_start", "country"]
    try:
        first = [{"window_start": window, "country": country, "revenue_sum": 1.5}]
        assert upsert_rows(DSN, "minute_revenue_by_country", key, first) == 1
        assert upsert_rows(DSN, "minute_revenue_by_country", key, first) == 1  # replayed batch

        latest = [{"window_start": window, "country": country, "revenue_sum": 7.25}]
        upsert_rows(DSN, "minute_revenue_by_country", key, latest)

        rows = _query(
            "SELECT revenue_sum FROM minute_revenue_by_country WHERE window_start = %s AND country = %s",
            (window, country),
        )
        assert [float(r[0]) for r in rows] == [7.25]
    finally:
        _query_commit("DELETE FROM minute_revenue_by_country WHERE window_start = %s", (window,))


def test_upsert_rows_with_no_rows_is_a_noop():
    assert upsert_rows(DSN, "minute_summary", ["window_start"], []) == 0


def test_replace_day_swaps_one_day_and_leaves_others():
    day, other = date(2000, 1, 1), date(2000, 1, 2)
    try:
        replace_day(DSN, "daily_users_by_country_platform", other, [_daily("Turkey", "ANDROID", 9)])
        replace_day(DSN, "daily_users_by_country_platform", day, [_daily("Turkey", "ANDROID", 1), _daily("Spain", "IOS", 2)])
        replace_day(DSN, "daily_users_by_country_platform", day, [_daily("Turkey", "IOS", 5)])  # rerun

        rows = _query("SELECT country, platform, distinct_users FROM daily_users_by_country_platform WHERE day = %s", (day,))
        assert rows == [("Turkey", "IOS", 5)]
        kept = _query("SELECT distinct_users FROM daily_users_by_country_platform WHERE day = %s", (other,))
        assert kept == [(9,)]
    finally:
        _query_commit("DELETE FROM daily_users_by_country_platform WHERE day IN (%s, %s)", (day, other))


def _daily(country: str, platform: str, users: int) -> dict:
    return {"country": country, "platform": platform, "distinct_users": users}


def _query_commit(sql: str, params=()) -> None:
    with psycopg.connect(DSN) as conn:
        conn.execute(sql, params)
