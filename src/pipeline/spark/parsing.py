"""Parse the ``clean-events`` JSON contract into flat columns and per-participant rows.

The contract is defined by ``schemas/clean/*.json`` (hyphenated keys, ``time`` in epoch
seconds, user ids as strings). Everything here is a pure DataFrame transformation, usable on
both batch and streaming DataFrames.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from pipeline.constants import UNKNOWN

IN_APP_PURCHASE = "in-app-purchase"
MATCH = "match"

# Union of the fields of the three clean event types; fields an event does not have are null.
CLEAN_EVENT_SCHEMA = StructType(
    [
        StructField("event-type", StringType()),
        StructField("time", LongType()),
        StructField("user-id", StringType()),
        StructField("user-a", StringType()),
        StructField("user-b", StringType()),
        StructField("purchase_value", DoubleType()),
        StructField("country", StringType()),
        StructField("platform", StringType()),
        StructField("user-a-country", StringType()),
        StructField("user-b-country", StringType()),
        StructField("user-a-platform", StringType()),
        StructField("user-b-platform", StringType()),
    ]
)


def parse_clean_events(df: DataFrame) -> DataFrame:
    """Parse a DataFrame with a string/binary ``value`` column of clean-event JSON.

    Returns the columns ``event_type, event_time, user_id, user_a, user_b, country, platform,
    purchase_value, user_a_country, user_b_country, user_a_platform, user_b_platform``;
    fields that do not apply to an event type are null. A match participant's platform is
    the DQ-enriched ``user-a-platform``/``user-b-platform`` (post-match info platform, else
    profile platform, else ``UNKNOWN``); the raw post-match infos are not read here.
    ``event_time`` is a timestamp built from the epoch seconds in ``time``. Rows that are not
    valid JSON objects have no event type and are dropped.
    """
    parsed = F.from_json(F.col("value").cast("string"), CLEAN_EVENT_SCHEMA).alias("e")
    return (
        df.select(parsed)
        .select(
            F.col("e.`event-type`").alias("event_type"),
            F.timestamp_seconds(F.col("e.time")).alias("event_time"),
            F.col("e.`user-id`").alias("user_id"),
            F.col("e.`user-a`").alias("user_a"),
            F.col("e.`user-b`").alias("user_b"),
            F.col("e.country").alias("country"),
            F.col("e.platform").alias("platform"),
            F.col("e.purchase_value").alias("purchase_value"),
            F.col("e.`user-a-country`").alias("user_a_country"),
            F.col("e.`user-b-country`").alias("user_b_country"),
            F.col("e.`user-a-platform`").alias("user_a_platform"),
            F.col("e.`user-b-platform`").alias("user_b_platform"),
        )
        .where(F.col("event_type").isNotNull())
    )


def _participant(user_id: Column, country: Column, platform: Column) -> Column:
    return F.struct(user_id.alias("user_id"), country.alias("country"), platform.alias("platform"))


def to_participants(events: DataFrame) -> DataFrame:
    """Turn parsed events into one row per participating user.

    ``init`` and ``in-app-purchase`` give one row; ``match`` gives two (user A with
    ``user_a_country``/``user_a_platform``, user B with the ``user_b_*`` values). A missing
    country or platform becomes ``"UNKNOWN"``. ``purchase_value`` is only set for purchases.
    Output columns: ``event_time, event_type, user_id, country, platform, purchase_value``.
    """
    participants = F.when(
        F.col("event_type") == MATCH,
        F.array(
            _participant(F.col("user_a"), F.col("user_a_country"), F.col("user_a_platform")),
            _participant(F.col("user_b"), F.col("user_b_country"), F.col("user_b_platform")),
        ),
    ).otherwise(F.array(_participant(F.col("user_id"), F.col("country"), F.col("platform"))))

    return events.select(
        "event_time",
        "event_type",
        F.explode(participants).alias("p"),
        "purchase_value",
    ).select(
        "event_time",
        "event_type",
        F.col("p.user_id").alias("user_id"),
        F.coalesce(F.col("p.country"), F.lit(UNKNOWN)).alias("country"),
        F.coalesce(F.col("p.platform"), F.lit(UNKNOWN)).alias("platform"),
        F.when(F.col("event_type") == IN_APP_PURCHASE, F.col("purchase_value")).alias("purchase_value"),
    )
