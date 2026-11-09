"""End-to-end test of the daily batch job against the live stack.

Requires the whole stack (``docker compose up -d --build``) and runs from the ``tests``
compose service. The job starts its own local Spark JVM, so with the stack's other Spark
containers running this needs headroom; stop the looping batch service first if memory is
tight (``docker compose stop spark-batch``)::

    docker compose --profile test run --rm tests pytest tests/integration/test_daily_table.py -v

The test waits (polling, no fixed sleep) until the generator's events have reached the
minute tables, runs ``python -m pipeline.spark.daily_job --date <today UTC>`` as a
subprocess, and checks the rows for today. A second run must leave the table consistent:
the day is replaced, not appended to.
"""

import subprocess
import sys
import time
from datetime import datetime, timezone

import psycopg
import pytest

from pipeline.settings import Settings

pytestmark = pytest.mark.integration

DSN = Settings.from_env().postgres_dsn
TIMEOUT_S = 240
JOB_TIMEOUT_S = 420
FULL_COUNTRY_NAMES = {"Turkey", "United Kingdom", "United States"}


def _query(sql: str, params=()) -> list[tuple]:
    with psycopg.connect(DSN) as conn:
        return conn.execute(sql, params).fetchall()


def _commit(sql: str, params=()) -> None:
    with psycopg.connect(DSN) as conn:
        conn.execute(sql, params)


def _wait_for_events() -> None:
    """Poll until the live aggregator has written minute rows, i.e. clean-events has data."""
    deadline = time.monotonic() + TIMEOUT_S
    while True:
        try:
            if _query("SELECT count(*) FROM minute_summary")[0][0] > 0:
                return
        except (psycopg.OperationalError, psycopg.errors.UndefinedTable):
            pass  # postgres not reachable yet, or the schema is not created yet
        if time.monotonic() >= deadline:
            pytest.fail("no events reached the minute tables in time; is the stack running?")
        time.sleep(5)


def _run_job(day: str) -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "pipeline.spark.daily_job", "--date", day],
        capture_output=True,
        text=True,
        timeout=JOB_TIMEOUT_S,
    )
    assert proc.returncode == 0, f"daily_job failed:\n{proc.stdout[-3000:]}\n{proc.stderr[-3000:]}"


def _daily_rows(day) -> dict[tuple[str, str], int]:
    rows = _query(
        "SELECT country, platform, distinct_users FROM daily_users_by_country_platform WHERE day = %s", (day,)
    )
    return {(country, platform): users for country, platform, users in rows}


def test_daily_table_fill():
    """The job fills today's rows, and re-running it replaces them instead of duplicating."""
    _wait_for_events()
    today = datetime.now(timezone.utc).date()

    _run_job(today.isoformat())
    first = _daily_rows(today)
    assert first, "no rows for today after the first run"
    assert all(users > 0 for users in first.values())
    assert {country for country, _ in first} & FULL_COUNTRY_NAMES
    assert {platform for _, platform in first} <= {"ANDROID", "IOS", "UNKNOWN"}

    # A sentinel group no run can produce: a replace removes it, an upsert would leave it.
    stale = ("ZZ-STALE", "TEST")
    try:
        _commit(
            "INSERT INTO daily_users_by_country_platform (day, country, platform, distinct_users) "
            "VALUES (%s, %s, %s, 1)",
            (today, *stale),
        )
        assert stale in _daily_rows(today)

        _run_job(today.isoformat())
        second = _daily_rows(today)
        assert stale not in second, "stale row survived the rerun: the day was upserted, not replaced"
    finally:
        _commit("DELETE FROM daily_users_by_country_platform WHERE day = %s AND country = %s", (today, stale[0]))
    # Idempotent: the day is recomputed, not appended to (a plain INSERT would hit the primary
    # key and fail the job). The generator keeps producing between the runs, so counts can only
    # grow and new (country, platform) groups can appear; existing rows never vanish.
    assert first.keys() <= second.keys()
    assert all(second[key] >= users for key, users in first.items())
