"""Live minute aggregator: Kafka ``clean-events`` -> Spark Structured Streaming -> Postgres.

Three update-mode queries (``minute_summary``, ``minute_revenue_by_country``,
``minute_matches_by_country``) read the topic, aggregate into 1-minute event-time windows and
upsert each micro-batch into Postgres. A minute's row therefore shows up after the first
trigger and is refined on every later one until the 2-minute watermark closes the window;
the final value equals what append mode would produce, without the latency.

Delivery semantics per hop: generator -> ``raw-events`` is an idempotent (not transactional)
producer; ``raw-events`` -> ``clean-events`` is exactly-once (Kafka transactions in the DQ
service); ``clean-events`` -> Postgres is at-least-once ``foreachBatch`` made effectively-once
by idempotent full-value upserts keyed by the grouping key, so replaying a micro-batch after
a crash changes nothing. This job reads with ``read_committed`` (aborted records are
invisible). Each query keeps its own checkpoint under ``/checkpoints/<table>``.

Run with ``python -m pipeline.spark.streaming_job`` (the ``spark-streaming`` compose service).
"""

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from pipeline.constants import LOG_FORMAT
from pipeline.settings import Settings
from pipeline.spark.aggregations import matches_by_country, minute_summary, revenue_by_country
from pipeline.spark.parsing import parse_clean_events, to_participants
from pipeline.spark.session import build_spark_session
from pipeline.spark.sink import to_db_row, upsert_rows

log = logging.getLogger("pipeline.streaming")

WATERMARK_DELAY = "2 minutes"
TRIGGER_INTERVAL = "10 seconds"
CHECKPOINT_ROOT = "/checkpoints"


def read_clean_events(spark: SparkSession, settings: Settings) -> DataFrame:
    """Streaming DataFrame of parsed clean events with a 2-minute event-time watermark.

    * ``startingOffsets=earliest``: a fresh checkpoint replays the whole topic; with an
      existing checkpoint Spark resumes from the stored offsets instead.
    * ``kafka.isolation.level=read_committed``: only records of committed DQ transactions.
    * ``failOnDataLoss=false``: retention or compaction may delete offsets the checkpoint
      still points at (for example after a long stop); failing the query forever on that
      would be worse than continuing from the earliest available offset.

    The watermark is applied once, here, before the participants/match aggregations derive
    from the stream, so all three queries use the same lateness rule.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.clean_topic)
        .option("startingOffsets", "earliest")
        .option("kafka.isolation.level", "read_committed")
        .option("failOnDataLoss", "false")
        .load()
    )
    return parse_clean_events(raw).withWatermark("event_time", WATERMARK_DELAY)


def make_batch_writer(dsn: str, table: str, key_columns: list[str]) -> Callable[[DataFrame, int], None]:
    """``foreachBatch`` function that upserts one micro-batch into ``table`` and logs its size.

    The batch holds only the windows updated by this trigger (update mode), a handful of
    rows, so collecting it to the driver is cheap. One Postgres transaction per batch.
    """

    def write(batch: DataFrame, batch_id: int) -> None:
        """Upsert one micro-batch (full current values per key) into ``table``."""
        rows = [to_db_row(r.asDict()) for r in batch.collect()]
        written = upsert_rows(dsn, table, key_columns, rows)
        log.info("batch %d: upserted %d rows into %s", batch_id, written, table)

    return write


def build_queries(events: DataFrame) -> dict[str, tuple[DataFrame, list[str]]]:
    """Map each target table to its aggregated streaming DataFrame and its upsert key."""
    participants = to_participants(events)
    return {
        "minute_summary": (minute_summary(participants), ["window_start"]),
        "minute_revenue_by_country": (revenue_by_country(participants), ["window_start", "country"]),
        "minute_matches_by_country": (matches_by_country(events), ["window_start", "country"]),
    }


def start_queries(spark: SparkSession, settings: Settings) -> list[StreamingQuery]:
    """Start the three update-mode queries, each with its own checkpoint."""
    events = read_clean_events(spark, settings)
    queries = []
    for table, (frame, keys) in build_queries(events).items():
        query = (
            frame.writeStream.queryName(table)
            .outputMode("update")
            .foreachBatch(make_batch_writer(settings.postgres_dsn, table, keys))
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{table}")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        log.info("started query %s (checkpoint %s/%s)", table, CHECKPOINT_ROOT, table)
        queries.append(query)
    return queries


def main() -> None:
    """Start the three queries and block until one of them terminates (then exit non-zero)."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    logging.getLogger("py4j").setLevel(logging.WARNING)  # one INFO line per JVM callback otherwise
    settings = Settings.from_env()
    spark = build_spark_session("pipeline-minute-aggregator")
    start_queries(spark, settings)
    # Raises the query's exception if one fails, so the process exits non-zero and compose
    # (`restart: on-failure`) restarts it; the checkpoints make the restart resume exactly.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
