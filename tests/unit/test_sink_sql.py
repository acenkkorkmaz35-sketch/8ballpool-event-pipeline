"""Unit tests for the SQL builders of the Postgres sink (no database, no psycopg, no Spark)."""

from datetime import date, datetime, timezone

import pytest

from pipeline.spark.sink import build_upsert_sql, day_lock_key, replace_day, to_db_row


def test_build_upsert_sql_revenue_by_country():
    sql = build_upsert_sql(
        "minute_revenue_by_country",
        ["window_start", "country", "revenue_sum"],
        ["window_start", "country"],
    )
    assert sql == (
        "INSERT INTO minute_revenue_by_country (window_start, country, revenue_sum) "
        "VALUES (%(window_start)s, %(country)s, %(revenue_sum)s) "
        "ON CONFLICT (window_start, country) DO UPDATE SET "
        "revenue_sum = EXCLUDED.revenue_sum, updated_at = now()"
    )


def test_build_upsert_sql_updates_every_non_key_column():
    sql = build_upsert_sql(
        "minute_summary",
        ["window_start", "window_end", "purchases_count", "revenue_sum", "distinct_users"],
        ["window_start"],
    )
    assert sql.endswith(
        "ON CONFLICT (window_start) DO UPDATE SET window_end = EXCLUDED.window_end, "
        "purchases_count = EXCLUDED.purchases_count, revenue_sum = EXCLUDED.revenue_sum, "
        "distinct_users = EXCLUDED.distinct_users, updated_at = now()"
    )


def test_build_upsert_sql_with_only_key_columns_still_refreshes_updated_at():
    sql = build_upsert_sql("t", ["a"], ["a"])
    assert sql.endswith("ON CONFLICT (a) DO UPDATE SET updated_at = now()")


@pytest.mark.parametrize(
    "table, columns, keys",
    [
        ("t; DROP TABLE x", ["a"], ["a"]),
        ("t", ["a, b"], ["a"]),
        ("t", ["a", "Bad"], ["a"]),
        ("t", ["a"], ["a)--"]),
        ("t\n", ["a"], ["a"]),
    ],
)
def test_build_upsert_sql_rejects_unsafe_identifiers(table, columns, keys):
    with pytest.raises(ValueError):
        build_upsert_sql(table, columns, keys)


def test_build_upsert_sql_requires_keys_that_are_columns():
    with pytest.raises(ValueError):
        build_upsert_sql("t", ["a", "b"], [])
    with pytest.raises(ValueError):
        build_upsert_sql("t", ["a", "b"], ["c"])


def test_to_db_row_makes_naive_timestamps_utc_aware():
    local = datetime(2023, 11, 14, 22, 14)  # naive, as Spark's collect() returns it
    out = to_db_row({"window_start": local, "country": "Turkey", "revenue_sum": 4.99})
    assert out["window_start"].tzinfo is not None
    assert out["window_start"].utcoffset().total_seconds() == 0
    assert out["window_start"] == local.astimezone(timezone.utc)
    assert out["country"] == "Turkey" and out["revenue_sum"] == 4.99


def test_to_db_row_keeps_aware_timestamps_as_the_same_instant():
    aware = datetime(2023, 11, 14, 22, 14, tzinfo=timezone.utc)
    assert to_db_row({"t": aware})["t"] == aware


def test_build_upsert_sql_rejects_updated_at_column():
    """``updated_at`` is set by the statement itself, so passing it as a column is a bug."""
    with pytest.raises(ValueError, match="updated_at"):
        build_upsert_sql("t", ["a", "updated_at"], ["a"])


def test_day_lock_key_is_deterministic_and_a_signed_64_bit_int():
    key = day_lock_key("daily_users_by_country_platform", date(2026, 11, 9))
    assert -(2**63) <= key < 2**63
    # Pinned literal: the key must be identical across processes and Python versions
    # (a salted hash() would not be).
    assert key == -5511583862423928717


def test_day_lock_key_differs_per_day_and_table():
    base = day_lock_key("t", date(2026, 11, 9))
    assert day_lock_key("t", date(2026, 11, 10)) != base
    assert day_lock_key("u", date(2026, 11, 9)) != base


def test_replace_day_with_no_rows_does_not_touch_the_database():
    """Empty rows without ``allow_empty`` return 0 before any connection is attempted.

    The DSN is unreachable and psycopg may not even be installed: reaching either would fail.
    """
    dsn = "postgresql://nobody:nothing@127.0.0.1:1/none"
    assert replace_day(dsn, "daily_users_by_country_platform", date(2026, 11, 9), []) == 0
