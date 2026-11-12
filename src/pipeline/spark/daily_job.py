"""Daily batch job: distinct users per (day, country, platform) from Kafka ``clean-events``.

Each run reads the whole topic in batch mode (earliest -> latest, ``read_committed`` so only
committed DQ transactions are visible), parses it, expands matches into their two
participants, keeps one UTC day, counts exact distinct users per country and platform and
replaces that day's rows in ``daily_users_by_country_platform`` (DELETE + INSERT in one
transaction). Re-running a day is therefore idempotent: partial days can be recomputed freely
and the row set of a finished day is final.

Usage (the ``spark-batch`` compose service has this module as its entrypoint)::

    docker compose up -d                                   # spark-batch runs --loop
    docker compose run --rm spark-batch --date 2026-11-09  # one day on demand, then exit
    docker compose run --rm spark-batch --all-days         # every day in the topic, then exit
    python -m pipeline.spark.daily_job --date 2026-11-09   # same, outside compose

* ``--date YYYY-MM-DD``: the UTC day to compute (default: today, UTC).
* ``--all-days``: one pass over the topic that computes and replaces every UTC day it holds
  (the "distinct users during each day" report for the whole history).
* ``--allow-empty``: single run only. Normally a day whose recomputation yields zero rows is
  left untouched (the source may simply no longer hold it, e.g. after Kafka retention);
  with this flag the stored rows of that day are deliberately cleared.
* ``--loop``: first backfill every day already in the topic (as ``--all-days``; a failure is
  logged and does not stop the loop), then recompute today every ``DAILY_LOOP_SECONDS``
  (default 300) seconds. Today is re-evaluated every iteration, so the job rolls over at UTC midnight; when the day changes
  the day that just ended is recomputed once more so it is complete up to its last event.
  An error in one iteration is logged and retried on the next one (including the pending
  recomputation of the finished day). Without ``--loop`` errors propagate (non-zero exit).
  In loop mode an empty day is skipped with a log line, never cleared.
"""

import argparse
import logging
import os
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime, timedelta, timezone

from pipeline.constants import LOG_FORMAT

log = logging.getLogger("pipeline.daily")

DEFAULT_LOOP_SECONDS = 300
TABLE = "daily_users_by_country_platform"


def utc_today() -> date:
    """Today's date in UTC."""
    return datetime.now(timezone.utc).date()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse ``--date`` (a ``date``, default today UTC), ``--all-days``, ``--loop``, ``--allow-empty``."""
    parser = argparse.ArgumentParser(description="Daily distinct users by country and platform.")
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="UTC day to compute (default: today, UTC)",
    )
    parser.add_argument(
        "--all-days", action="store_true", help="compute and replace every UTC day held in the topic, then exit"
    )
    parser.add_argument(
        "--loop", action="store_true", help="backfill all days once, then recompute today every DAILY_LOOP_SECONDS"
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="single run only: let a day that computes to zero rows clear its stored rows",
    )
    args = parser.parse_args(argv)
    if args.loop and args.date is not None:
        parser.error("--loop always recomputes today; it cannot be combined with --date")
    if args.loop and args.allow_empty:
        parser.error("--allow-empty is for a single on-demand run; it cannot be combined with --loop")
    if args.all_days and (args.date is not None or args.loop or args.allow_empty):
        parser.error("--all-days cannot be combined with --date, --loop or --allow-empty")
    return args


def group_rows_by_day(rows: Iterable[Mapping[str, object]]) -> dict[date, list[dict]]:
    """Split ``{day, country, platform, distinct_users}`` rows into per-day lists without ``day``.

    ``replace_day`` writes the ``day`` column itself, so it is dropped from each row.
    """
    by_day: dict[date, list[dict]] = {}
    for row in rows:
        by_day.setdefault(row["day"], []).append({k: v for k, v in row.items() if k != "day"})
    return by_day


def loop_seconds(env: Mapping[str, str] | None = None) -> int:
    """``DAILY_LOOP_SECONDS`` as a positive integer (default 300); ``ValueError`` otherwise."""
    raw = (os.environ if env is None else env).get("DAILY_LOOP_SECONDS")
    if raw is None or raw.strip() == "":
        return DEFAULT_LOOP_SECONDS
    try:
        seconds = int(raw)
    except ValueError:
        seconds = 0
    if seconds <= 0:
        raise ValueError(f"DAILY_LOOP_SECONDS must be a positive integer, got {raw!r}")
    return seconds


def _read_participants(spark, settings):
    """Batch-read the whole ``clean-events`` topic (committed data only) as participant rows."""
    from pipeline.spark.parsing import parse_clean_events, to_participants

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.clean_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("kafka.isolation.level", "read_committed")
        .option("failOnDataLoss", "false")  # retention may have removed the oldest offsets
        .load()
    )
    return to_participants(parse_clean_events(raw))


def run_all_days(spark, settings) -> dict[date, int]:
    """Compute every UTC day held in the topic in one pass and replace each day's rows.

    Returns ``{day: rows written}``. Only days that have events are written, so no stored
    day is ever cleared by this path.
    """
    from pipeline.spark.aggregations import daily_users_all_days
    from pipeline.spark.sink import replace_day, to_db_row

    # At most days x countries x platforms rows, so collecting to the driver is cheap.
    rows = [to_db_row(r.asDict()) for r in daily_users_all_days(_read_participants(spark, settings)).collect()]
    written = {}
    for day, day_rows in sorted(group_rows_by_day(rows).items()):
        written[day] = replace_day(settings.postgres_dsn, TABLE, day, day_rows)
    log.info("all days: wrote %d rows for %d days into %s", sum(written.values()), len(written), TABLE)
    return written


def run_day(spark, settings, day: date, allow_empty: bool = False) -> int:
    """Compute ``day`` from the whole topic and replace its rows in Postgres; return the row count.

    A day that computes to zero rows is skipped (existing rows are kept, 0 is returned)
    unless ``allow_empty`` is set.
    """
    from pipeline.spark.aggregations import daily_users
    from pipeline.spark.sink import replace_day, to_db_row

    result = daily_users(_read_participants(spark, settings), day)
    # At most countries x platforms rows, so collecting to the driver is cheap.
    rows = [
        {k: v for k, v in to_db_row(r.asDict()).items() if k != "day"}  # replace_day sets `day`
        for r in result.collect()
    ]
    if not rows and not allow_empty:
        log.info("day %s: no rows computed, leaving %s untouched (use --allow-empty to clear the day)", day, TABLE)
        return 0
    written = replace_day(settings.postgres_dsn, TABLE, day, rows, allow_empty=allow_empty)
    log.info("day %s: wrote %d rows into %s", day, written, TABLE)
    return written


def run_loop(
    compute: Callable[[date], int],
    interval_s: float,
    today: Callable[[], date] = utc_today,
    sleep: Callable[[float], None] = time.sleep,
    iterations: int | None = None,
) -> None:
    """Recompute today every ``interval_s`` seconds, forever (or ``iterations`` times).

    Keeps a list of days that still need their final recomputation: when ``today()`` moves
    past the previously seen day, that day is queued and retried every iteration until it
    succeeds. Exceptions are logged and never end the loop.
    """
    last_seen: date | None = None
    pending: list[date] = []
    done = 0
    while iterations is None or done < iterations:
        current = today()
        if last_seen is not None and current > last_seen:
            pending.append(last_seen)
        last_seen = current
        for day in [*pending, current]:
            try:
                compute(day)
                if day in pending:
                    pending.remove(day)
            except Exception:  # noqa: BLE001 - the next iteration retries; the loop must survive
                log.exception("daily job failed for %s; retrying in %.0f s", day, interval_s)
        done += 1
        if iterations is None or done < iterations:
            sleep(interval_s)


def main(argv: list[str] | None = None) -> None:
    """Entry point: ``--all-days`` once, one run for ``--date`` (default today UTC), or ``--loop``."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    logging.getLogger("py4j").setLevel(logging.WARNING)
    args = parse_args(argv)
    interval = loop_seconds() if args.loop else None  # fail fast, before the JVM starts

    from pipeline.settings import Settings
    from pipeline.spark.session import build_spark_session

    settings = Settings.from_env()
    spark = build_spark_session("pipeline-daily-users")

    def compute(day: date) -> int:
        """Recompute one day with the shared session and settings."""
        return run_day(spark, settings, day, allow_empty=args.allow_empty)

    if args.all_days:
        run_all_days(spark, settings)
    elif args.loop:
        try:  # backfill the history once; today is then kept fresh by the loop
            run_all_days(spark, settings)
        except Exception:  # noqa: BLE001 - the loop must still start; today is retried every iteration
            log.exception("start-up backfill of all days failed; continuing with today only")
        log.info("loop mode: recomputing today (UTC) every %d s", interval)
        run_loop(compute, interval)
    else:
        compute(args.date or utc_today())


if __name__ == "__main__":
    main()
