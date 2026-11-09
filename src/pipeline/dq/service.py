"""Kafka shell around the pure DQ core: an exactly-once consume-transform-produce loop.

Per batch the service (1) consumes up to ``BATCH_SIZE`` raw messages, (2) runs each through
:func:`pipeline.dq.processor.process`, (3) produces the results to ``clean-events`` /
``dead-letter`` plus any new user profiles to ``user-profiles``, and (4) commits the
consumed offsets *inside the same Kafka transaction* as those outputs.

Why a transaction (exactly-once)?
    Events carry no unique id, so downstream cannot deduplicate. Revenue and purchase
    counts are summed per minute; at-least-once delivery would double count them. The
    transaction makes "outputs written" and "input offsets advanced" one atomic step: after
    a crash the batch is either fully visible downstream or not at all, and is re-read.

Why ``read_committed``?
    Records of an open or aborted transaction are written to the log but must stay
    invisible. Our consumer (and the Spark job downstream) therefore only read committed
    data. It also matters when we re-read ``user-profiles`` at startup.

Why are profiles applied to the in-memory store only after the commit?
    ``ProfileStore.stage`` makes a profile visible inside the current batch (a purchase
    right after its ``init`` is enriched), but the committed map is only updated by
    ``store.commit()`` once Kafka confirmed the transaction. If the transaction aborts the
    staged profiles are dropped and the batch is re-consumed, so memory never runs ahead
    of what Kafka durably holds.

Why one partition / one instance?
    ``init`` must be processed before any later event of the same user (including
    ``user-b`` of a match). One ``raw-events`` partition consumed by one service gives that
    order for free; the README documents the scale-out path.
"""

import json
import logging
import signal
import sys
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)

from pipeline.constants import LOG_FORMAT, UNKNOWN
from pipeline.dq.processor import Clean, process
from pipeline.dq.profiles import Profile, ProfileStore
from pipeline.dq.rules import RuleSet, load_rules
from pipeline.schemas import SchemaRegistry
from pipeline.settings import Settings

log = logging.getLogger("pipeline.dq")

TRANSACTIONAL_ID = "dq-service-1"
CONSUMER_GROUP = "dq-service"
BATCH_SIZE = 500
POLL_TIMEOUT_S = 1.0
KAFKA_TIMEOUT_S = 30.0
# Give up bootstrapping if the profile topic yields nothing for this long.
BOOTSTRAP_STALL_S = 30.0
# Attempts for transactional calls that librdkafka flags as retriable.
MAX_ATTEMPTS = 5

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Startup: rebuild the profile map
# ---------------------------------------------------------------------------


def bootstrap_profiles(settings: Settings) -> ProfileStore:
    """Rebuild the user-profile map by reading ``user-profiles`` from the beginning.

    A throwaway consumer (random group id, manual ``assign``) is used so the bootstrap
    never touches the service's own consumer-group offsets. It reads with
    ``read_committed`` so profiles of aborted transactions are ignored.

    End condition: for each partition we read until its high watermark, or until
    librdkafka reports end-of-partition (``enable.partition.eof``). With ``read_committed``
    the consumer stops at the *last stable offset*, which can sit below the high watermark
    (transaction commit/abort markers occupy offsets but are never delivered as records),
    so the EOF event, not the raw watermark, is what reliably ends the loop. Empty
    partitions are detected up front from the watermarks. If nothing arrives for
    ``BOOTSTRAP_STALL_S`` the bootstrap fails loudly rather than starting with a partial map.

    Tombstones (``None`` value) and undecodable records are skipped with a warning.
    """
    topic = settings.profiles_topic
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"dq-bootstrap-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest",
        }
    )
    store = ProfileStore()
    user_ids: set[str] = set()
    try:
        metadata = consumer.list_topics(topic, timeout=KAFKA_TIMEOUT_S).topics[topic]
        if metadata.error is not None:
            raise KafkaException(metadata.error)
        partitions = [TopicPartition(topic, p, OFFSET_BEGINNING) for p in metadata.partitions]

        # partition -> high watermark, only for partitions that contain records.
        pending: dict[int, int] = {}
        for tp in partitions:
            low, high = consumer.get_watermark_offsets(tp, timeout=KAFKA_TIMEOUT_S, cached=False)
            if high > low:
                pending[tp.partition] = high
        consumer.assign(partitions)

        last_progress = time.monotonic()
        while pending:
            msg = consumer.poll(POLL_TIMEOUT_S)
            if msg is None:
                if time.monotonic() - last_progress > BOOTSTRAP_STALL_S:
                    raise TimeoutError(f"no data from {topic} for {BOOTSTRAP_STALL_S:.0f}s while bootstrapping")
                continue
            last_progress = time.monotonic()
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    pending.pop(msg.partition(), None)
                    continue
                raise KafkaException(err)

            user_id = _load_profile(store, msg)
            if user_id is not None:
                user_ids.add(user_id)
            if msg.offset() + 1 >= pending.get(msg.partition(), float("inf")):
                pending.pop(msg.partition(), None)
    finally:
        consumer.close()

    log.info("bootstrapped %d user profiles from %s", len(user_ids), topic)
    return store


def _load_profile(store: ProfileStore, msg: Message) -> str | None:
    """Load one ``user-profiles`` record into ``store``; return its user id, or None if skipped."""
    where = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
    if msg.value() is None:
        log.warning("skipping tombstone at %s", where)
        return None
    try:
        # The store does not normalise keys: ids are plain UTF-8 strings, as produced.
        user_id = msg.key().decode("utf-8")
        profile = Profile.from_json(msg.value())
    except (AttributeError, UnicodeDecodeError, ValueError, KeyError, TypeError):
        log.warning("skipping undecodable profile at %s", where)
        return None
    store.load([(user_id, profile)])
    return user_id


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


@dataclass
class BatchStats:
    """Counters logged once per non-empty batch."""

    clean: int = 0
    invalid: int = 0
    unknown_user: int = 0
    profiles: int = 0

    def record_clean(self, event: dict) -> None:
        """Count a clean event, flagging purchases/matches whose user has no known profile."""
        self.clean += 1
        kind = event["event-type"]
        if kind == "in-app-purchase":
            unknown = event.get("country") == UNKNOWN
        elif kind == "match":
            unknown = UNKNOWN in (event.get("user-a-country"), event.get("user-b-country"))
        else:
            unknown = False
        if unknown:
            self.unknown_user += 1


def _retry_if_retriable(operation: Callable[[], T]) -> T:
    """Run a transactional call, repeating it while Kafka flags the error as retriable."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return operation()
        except KafkaException as exc:
            err = exc.args[0]
            if not err.retriable() or attempt == MAX_ATTEMPTS:
                raise
            log.warning("retriable Kafka error (attempt %d/%d): %s", attempt, MAX_ATTEMPTS, err)
    raise AssertionError("unreachable")


def _encode_event(event: dict) -> bytes:
    """Serialise a clean event as compact UTF-8 JSON.

    ``allow_nan=False`` is a backstop: the processor already rejects non-finite numbers.
    """
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _run_batch(
    msgs: list[Message],
    producer: Producer,
    consumer: Consumer,
    store: ProfileStore,
    registry: SchemaRegistry,
    rules: RuleSet,
    settings: Settings,
) -> BatchStats:
    """Process ``msgs`` inside one Kafka transaction and commit it.

    Raises ``KafkaException`` on a Kafka failure; the caller aborts and rewinds.
    """
    stats = BatchStats()
    producer.begin_transaction()
    for msg in msgs:
        # A tombstone on the raw topic has no value: it becomes a dead-letter record.
        result = process(msg.value() or b"", registry, rules, store)
        if isinstance(result, Clean):
            stats.record_clean(result.event)
            producer.produce(settings.clean_topic, key=result.key.encode("utf-8"), value=_encode_event(result.event))
        else:
            stats.invalid += 1
            producer.produce(settings.dead_letter_topic, key=msg.key(), value=result.to_dead_letter())

    # Profiles staged by this batch's `init` events go out in the same transaction.
    for user_id, profile in store.staged().items():
        producer.produce(settings.profiles_topic, key=user_id.encode("utf-8"), value=profile.to_json())
        stats.profiles += 1

    # The heart of exactly-once: the consumed offsets are committed *by the producer*,
    # atomically with the records above (position = next offset to read per partition).
    producer.send_offsets_to_transaction(
        consumer.position(consumer.assignment()), consumer.consumer_group_metadata()
    )
    _retry_if_retriable(producer.commit_transaction)
    # Only now that Kafka holds the batch durably do the profiles become permanent.
    store.commit()
    return stats


def _abort_and_rewind(producer: Producer, consumer: Consumer, store: ProfileStore) -> None:
    """Undo a failed batch: abort the transaction, drop staged profiles, re-read the input.

    The consumer already advanced its in-memory position past the failed batch, but the
    group's committed offset did not move (the transaction aborted), so every assigned
    partition is sought back to its committed offset, or to the beginning if the group has
    not committed anything yet.
    """
    _retry_if_retriable(producer.abort_transaction)
    store.abort()
    for tp in consumer.committed(consumer.assignment(), timeout=KAFKA_TIMEOUT_S):
        if tp.offset < 0:  # OFFSET_INVALID: nothing committed yet
            tp.offset = OFFSET_BEGINNING
        consumer.seek(tp)


def _abort_quietly(producer: Producer) -> None:
    """Best-effort ``abort_transaction`` for the crash path; its own failure is only logged."""
    try:
        producer.abort_transaction()
    except Exception:  # noqa: BLE001 - the original error is the one that matters
        log.exception("abort_transaction failed while handling an unexpected error")


def _handle_batch(
    msgs: list[Message],
    producer: Producer,
    consumer: Consumer,
    store: ProfileStore,
    registry: SchemaRegistry,
    rules: RuleSet,
    settings: Settings,
) -> BatchStats | None:
    """Run one batch; return its stats, or None if it was aborted and rewound for a retry.

    * Non-fatal Kafka failures (``KafkaException``, ``BufferError``): abort the
      transaction, drop staged profiles, seek back, return None; the loop re-reads the batch.
    * A fatal ``KafkaException``: re-raised untouched (the producer is unusable).
    * Any other exception (a bug): abort the transaction (best effort, so it does not stay
      open until ``transaction.timeout.ms``), drop staged profiles, log and re-raise; the
      process exits non-zero and compose restarts it.
    """
    try:
        return _run_batch(msgs, producer, consumer, store, registry, rules, settings)
    except (KafkaException, BufferError) as exc:
        # BufferError = local producer queue full; handled like a Kafka failure.
        if isinstance(exc, KafkaException) and exc.args[0].fatal():
            raise  # unrecoverable producer state: exit non-zero, container restarts
        log.warning("batch of %d messages failed (%s); aborting and retrying", len(msgs), exc)
        _abort_and_rewind(producer, consumer, store)
        return None
    except Exception:
        log.exception("unexpected error in a batch of %d messages; aborting the transaction", len(msgs))
        _abort_quietly(producer)
        store.abort()
        raise


def _consume_batch(consumer: Consumer) -> list[Message]:
    """Fetch the next batch, dropping (and logging) non-fatal per-message errors."""
    batch = []
    for msg in consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_S):
        err = msg.error()
        if err is None:
            batch.append(msg)
        elif err.fatal():
            raise KafkaException(err)
        else:
            log.warning("consumer error ignored: %s", err)
    return batch


def run(settings: Settings) -> None:
    """Run the exactly-once DQ loop until SIGTERM/SIGINT (or a fatal Kafka error).

    On a signal the loop finishes the batch in flight and then closes the consumer, so a
    stop never leaves a transaction open. Fatal Kafka errors propagate to the caller.
    """
    registry = SchemaRegistry.from_dir(settings.schemas_dir)
    rules = load_rules(settings.rules_path)

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    producer = Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "transactional.id": TRANSACTIONAL_ID,
            "enable.idempotence": True,
        }
    )
    # Fences any previous instance with the same transactional.id and aborts its open
    # transaction. Doing this BEFORE the bootstrap guarantees no zombie transaction holds
    # back the last stable offset of `user-profiles` while we read it.
    _retry_if_retriable(lambda: producer.init_transactions(KAFKA_TIMEOUT_S))

    store = bootstrap_profiles(settings)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": CONSUMER_GROUP,
            # Offsets are committed through the producer transaction, never by the consumer.
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([settings.raw_topic])
    log.info("consuming %s -> %s / %s", settings.raw_topic, settings.clean_topic, settings.dead_letter_topic)

    try:
        while not stop.is_set():
            msgs = _consume_batch(consumer)
            if not msgs:
                continue
            stats = _handle_batch(msgs, producer, consumer, store, registry, rules, settings)
            if stats is None:
                continue
            log.info(
                "batch: consumed=%d clean=%d invalid=%d unknown_user=%d profiles=%d",
                len(msgs), stats.clean, stats.invalid, stats.unknown_user, stats.profiles,
            )
    finally:
        log.info("shutting down")
        consumer.close()


def main() -> None:
    """Console entry point (``pipeline-dq``): configure logging and run the service."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    try:
        run(Settings.from_env())
    except Exception:
        log.exception("DQ service terminated by an unrecoverable error")
        sys.exit(1)


if __name__ == "__main__":
    main()
