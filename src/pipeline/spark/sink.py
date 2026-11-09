"""Idempotent Postgres sink for the Spark jobs.

``upsert_rows`` (minute tables) and ``replace_day`` (daily table) each run in ONE
transaction on a fresh connection, so a micro-batch is written entirely or not at all and
replaying it (Spark's at-least-once ``foreachBatch``) leaves the same final rows.

SQL safety: values are always bound as parameters. Table and column names cannot be
parameters, so they are interpolated into the statement; they only ever come from code
constants, and every builder additionally *asserts* that each identifier matches
``^[a-z_]+$`` (raising ``ValueError``) instead of quoting it. Anything with punctuation,
whitespace or upper case is rejected, so injection through an identifier is impossible even
if a caller later passes user input.

``psycopg`` is imported lazily inside the functions that open a connection, so the pure SQL
builder (and its unit tests) work where the driver is not installed.
"""

import hashlib
import logging
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any

log = logging.getLogger("pipeline.sink")

_IDENTIFIER = re.compile(r"[a-z_]+")


def _check_identifier(name: str) -> str:
    if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"unsafe SQL identifier: {name!r} (must match ^[a-z_]+$)")
    return name


def _placeholders(columns: Sequence[str]) -> str:
    return ", ".join(f"%({c})s" for c in columns)


def build_upsert_sql(table: str, columns: list[str], key_columns: list[str]) -> str:
    """Build ``INSERT ... ON CONFLICT (keys) DO UPDATE`` with named ``%(col)s`` placeholders.

    Every non-key column is overwritten from ``EXCLUDED`` and ``updated_at`` is set to
    ``now()`` (so ``updated_at`` must not be listed in ``columns``). Raises ``ValueError`` for
    an identifier outside ``^[a-z_]+$``, for empty ``key_columns`` or for a key that is not
    in ``columns``.
    """
    _check_identifier(table)
    for name in [*columns, *key_columns]:
        _check_identifier(name)
    if "updated_at" in columns:
        raise ValueError("'updated_at' is set by the statement (now()); do not pass it in columns")
    if not key_columns or not set(key_columns) <= set(columns):
        raise ValueError("key_columns must be a non-empty subset of columns")

    updates = [f"{c} = EXCLUDED.{c}" for c in columns if c not in key_columns]
    updates.append("updated_at = now()")
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({_placeholders(columns)}) "
        f"ON CONFLICT ({', '.join(key_columns)}) DO UPDATE SET {', '.join(updates)}"
    )


def to_db_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Make the values of one Spark row (``Row.asDict()``) safe to bind as parameters.

    ``collect()`` hands timestamps over as *naive* datetimes in the driver's local time zone;
    ``astimezone`` on a naive value interprets it exactly that way, so the result is the
    correct timezone-aware UTC instant whatever TZ the process runs in. Other values pass
    through (floats bind as float8 and Postgres rounds them on assignment to
    ``numeric(14,2)``).
    """
    return {
        key: value.astimezone(timezone.utc) if isinstance(value, datetime) else value
        for key, value in row.items()
    }


def _shared_columns(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    columns = list(rows[0])
    for row in rows:
        if set(row) != set(columns):
            raise ValueError("all rows must have the same columns")
    return columns


def upsert_rows(dsn: str, table: str, key_columns: list[str], rows: list[dict]) -> int:
    """Upsert ``rows`` into ``table`` in a single transaction; return the number of rows.

    Columns are taken from the rows' keys (all rows must share them). Empty ``rows`` is a
    no-op that does not even open a connection. Re-running with the same keys overwrites the
    non-key columns, so the call is idempotent.
    """
    if not rows:
        return 0
    import psycopg  # lazy: keeps this module importable where the driver is not installed

    sql = build_upsert_sql(table, _shared_columns(rows), key_columns)
    # `with connect()` is one transaction: commit on success, rollback on error, then close.
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
    return len(rows)


def day_lock_key(table: str, day: date) -> int:
    """Stable signed 64-bit key for ``pg_advisory_xact_lock``, derived from table name and day.

    SHA-256 (not ``hash()``, which is salted per process) so every process and every Python
    version maps the same ``(table, day)`` to the same lock.
    """
    digest = hashlib.sha256(f"{table}:{day.isoformat()}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def replace_day(dsn: str, table: str, day: date, rows: list[dict], allow_empty: bool = False) -> int:
    """Replace everything stored for ``day`` in ``table`` by ``rows``; return the row count.

    ``DELETE ... WHERE day = %s`` plus the ``INSERT`` run in one transaction, so readers see
    either the old day or the new one, never a mix, and rerunning the same day is idempotent
    (this is the daily batch job's write path). ``table`` needs a ``day`` column and ``rows``
    must not contain it (it is set from ``day``).

    Empty ``rows`` are refused by default: nothing is deleted, a warning is logged and 0 is
    returned, because an empty result usually means the source no longer holds that day (for
    example Kafka retention removed it) and replacing would wipe good data. Pass
    ``allow_empty=True`` to clear the day deliberately.

    The transaction first takes a transaction-scoped advisory lock keyed by ``(table, day)``
    (:func:`day_lock_key`), so two concurrent runs for the same day serialise instead of
    interleaving their DELETE and INSERT; it is released automatically at commit/rollback.
    """
    _check_identifier(table)
    if not rows and not allow_empty:
        # Before the psycopg import and any connection: refusing must never touch the database.
        log.warning("refusing to replace day %s in %s with zero rows (pass allow_empty=True to clear it)", day, table)
        return 0
    columns = _shared_columns(rows) if rows else []
    for name in columns:
        _check_identifier(name)
    if "day" in columns:
        raise ValueError("rows must not contain 'day'; it is taken from the day argument")

    import psycopg

    all_columns = ["day", *columns]
    insert = f"INSERT INTO {table} ({', '.join(all_columns)}) VALUES ({_placeholders(all_columns)})"
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (day_lock_key(table, day),))
            cur.execute(f"DELETE FROM {table} WHERE day = %s", (day,))
            if rows:
                cur.executemany(insert, [{"day": day, **row} for row in rows])
    return len(rows)
