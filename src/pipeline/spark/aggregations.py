"""Aggregations over parsed clean events: per-minute windows and the daily distinct users.

Pure DataFrame functions. The minute functions work on batch and streaming inputs (for
streaming, apply ``withWatermark("event_time", ...)`` to the input first); ``daily_users`` is
batch only. Output column names match the Postgres tables ``minute_summary``,
``minute_revenue_by_country``, ``minute_matches_by_country`` and
``daily_users_by_country_platform``.
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.constants import UNKNOWN
from pipeline.spark.parsing import IN_APP_PURCHASE, MATCH

WINDOW = "1 minute"


def minute_summary(participants: DataFrame) -> DataFrame:
    """Per-minute purchases, revenue and exact distinct users.

    Input is the output of ``to_participants``. ``purchases_count`` counts purchase rows,
    ``revenue_sum`` is 0.0 (not null) for a minute without purchases, and ``distinct_users``
    is ``size(collect_set(user_id))``: exact and, unlike ``countDistinct``, allowed in
    streaming update mode. Columns: ``window_start, window_end, purchases_count,
    revenue_sum, distinct_users``.
    """
    return (
        participants.groupBy(F.window("event_time", WINDOW).alias("w"))
        .agg(
            F.count(F.when(F.col("event_type") == IN_APP_PURCHASE, 1)).alias("purchases_count"),
            F.coalesce(F.sum("purchase_value"), F.lit(0.0)).alias("revenue_sum"),
            F.size(F.collect_set("user_id")).alias("distinct_users"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "purchases_count",
            "revenue_sum",
            "distinct_users",
        )
    )


def revenue_by_country(participants: DataFrame) -> DataFrame:
    """Per-minute purchase revenue by country (purchases only).

    Columns: ``window_start, country, revenue_sum``.
    """
    return (
        participants.where(F.col("event_type") == IN_APP_PURCHASE)
        .groupBy(F.window("event_time", WINDOW).alias("w"), "country")
        .agg(F.sum("purchase_value").alias("revenue_sum"))
        .select(F.col("w.start").alias("window_start"), "country", "revenue_sum")
    )


def matches_by_country(events: DataFrame) -> DataFrame:
    """Per-minute match counts by participant country.

    Input is the output of ``parse_clean_events``. A match counts once per distinct
    participant country (TR vs GB gives TR +1 and GB +1; TR vs TR gives TR +1). A missing
    country is counted as ``"UNKNOWN"``. Columns: ``window_start, country, matches_count``.
    """
    countries = F.array_distinct(
        F.array(
            F.coalesce(F.col("user_a_country"), F.lit(UNKNOWN)),
            F.coalesce(F.col("user_b_country"), F.lit(UNKNOWN)),
        )
    )
    return (
        events.where(F.col("event_type") == MATCH)
        .select("event_time", F.explode(countries).alias("country"))
        .groupBy(F.window("event_time", WINDOW).alias("w"), "country")
        .agg(F.count(F.lit(1)).alias("matches_count"))
        .select(F.col("w.start").alias("window_start"), "country", "matches_count")
    )


def daily_users(participants: DataFrame, day: date) -> DataFrame:
    """Exact distinct users per country and platform for one UTC ``day`` (batch only).

    Input is the output of ``to_participants``. Events are kept when
    ``to_date(event_time) == day``; that is a UTC day because the Spark session runs with
    ``spark.sql.session.timeZone=UTC``. ``countDistinct`` is exact and fine in batch (it is
    not supported in streaming update mode, which is why the minute table uses
    ``collect_set``). A user seen with two platforms counts once in each group. Columns:
    ``day`` (date), ``country, platform, distinct_users``.
    """
    return (
        participants.where(F.to_date("event_time") == F.lit(day))
        .groupBy("country", "platform")
        .agg(F.countDistinct("user_id").alias("distinct_users"))
        .select(F.lit(day).cast("date").alias("day"), "country", "platform", "distinct_users")
    )


def daily_users_all_days(participants: DataFrame) -> DataFrame:
    """Exact distinct users per (UTC day, country, platform) for every day in the input.

    Same definition as :func:`daily_users`, but grouped by ``to_date(event_time)`` instead of
    filtered to one day, so one pass over the topic yields a report for each day it holds
    (used for the backfill on start-up and ``--all-days``). Columns: ``day`` (date),
    ``country, platform, distinct_users``.
    """
    return (
        participants.withColumn("day", F.to_date("event_time"))
        .groupBy("day", "country", "platform")
        .agg(F.countDistinct("user_id").alias("distinct_users"))
        .select("day", "country", "platform", "distinct_users")
    )
