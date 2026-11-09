"""Exactly-once control flow of the DQ service, with duck-typed fakes (no broker).

``pipeline.dq.service`` imports ``confluent_kafka`` at module level (for ``KafkaException``,
``TopicPartition`` and ``OFFSET_BEGINNING``), so these tests live outside ``tests/unit`` and run
in the ``tests`` container, where the library is installed::

    docker compose --profile test run --rm tests pytest tests/kafka_unit -q

Locally (no ``confluent_kafka``) the module is skipped. The fake Producer, Consumer and
ProfileStore all append to ONE shared call log, so the tests assert the relative order of calls
across the three objects, which is what makes the processing exactly-once.
"""

import json
from pathlib import Path

import pytest

pytest.importorskip("confluent_kafka")

from confluent_kafka import OFFSET_BEGINNING, KafkaException, TopicPartition  # noqa: E402

from pipeline.dq import service  # noqa: E402
from pipeline.dq.profiles import ProfileStore  # noqa: E402
from pipeline.dq.rules import load_rules  # noqa: E402
from pipeline.schemas import SchemaRegistry  # noqa: E402
from pipeline.settings import Settings  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SETTINGS = Settings()
CONSUMER_GROUP_METADATA = object()


class FakeKafkaError:
    """Stands in for ``confluent_kafka.KafkaError`` (only the predicates the service reads)."""

    def __init__(self, fatal: bool = False, retriable: bool = False):
        self._fatal, self._retriable = fatal, retriable

    def fatal(self) -> bool:
        return self._fatal

    def retriable(self) -> bool:
        return self._retriable

    def __str__(self) -> str:
        return "fake kafka error"


class FakeProducer:
    def __init__(self, calls: list, fail_commit: Exception | None = None, fail_abort: Exception | None = None):
        self.calls, self.fail_commit, self.fail_abort = calls, fail_commit, fail_abort

    def begin_transaction(self):
        self.calls.append("begin")

    def produce(self, topic, key=None, value=None):
        self.calls.append(f"produce:{topic}")

    def send_offsets_to_transaction(self, positions, group_metadata):
        assert group_metadata is CONSUMER_GROUP_METADATA
        self.calls.append("send_offsets")

    def commit_transaction(self):
        self.calls.append("commit")
        if self.fail_commit:
            raise self.fail_commit

    def abort_transaction(self):
        self.calls.append("abort")
        if self.fail_abort:
            raise self.fail_abort


class FakeConsumer:
    def __init__(self, calls: list, committed: list[TopicPartition]):
        self.calls, self._committed = calls, committed
        self.sought: list[tuple[int, int]] = []

    def assignment(self):
        return [TopicPartition(SETTINGS.raw_topic, 0)]

    def position(self, partitions):
        return [TopicPartition(SETTINGS.raw_topic, 0, 100)]

    def consumer_group_metadata(self):
        return CONSUMER_GROUP_METADATA

    def committed(self, partitions, timeout=None):
        self.calls.append("committed")
        return self._committed

    def seek(self, tp):
        self.calls.append("seek")
        self.sought.append((tp.partition, tp.offset))


class RecordingStore(ProfileStore):
    """The real store, logging the two calls that decide when profiles become permanent."""

    def __init__(self, calls: list):
        super().__init__()
        self.calls = calls

    def commit(self):
        self.calls.append("store.commit")
        super().commit()

    def abort(self):
        self.calls.append("store.abort")
        super().abort()


class FakeMessage:
    def __init__(self, event: dict | None = None, raw: bytes | None = None):
        self._value = raw if raw is not None else json.dumps(event).encode("utf-8")

    def value(self):
        return self._value

    def key(self):
        return None


@pytest.fixture(scope="module")
def registry() -> SchemaRegistry:
    return SchemaRegistry.from_dir(ROOT / "schemas" / "raw")


@pytest.fixture(scope="module")
def rules():
    return load_rules(ROOT / "config" / "rules.yaml")


@pytest.fixture
def calls() -> list:
    return []


@pytest.fixture
def batch() -> list[FakeMessage]:
    """An init (stages a profile), a purchase by that user, and one undecodable message."""
    init = {"event-type": "init", "time": 1700000000, "user-id": "user-1", "country": "TR", "platform": "android"}
    purchase = {
        "event-type": "in-app-purchase",
        "time": 1700000120,
        "purchase_value": 4.99,
        "user-id": "user-1",
        "product-id": "coins-500",
    }
    return [FakeMessage(init), FakeMessage(purchase), FakeMessage(raw=b"not json")]


def _handle(batch, producer, consumer, store, registry, rules):
    return service._handle_batch(batch, producer, consumer, store, registry, rules, SETTINGS)


def test_success_path_commits_offsets_in_the_transaction_then_the_store(calls, batch, registry, rules):
    producer, consumer, store = FakeProducer(calls), FakeConsumer(calls, []), RecordingStore(calls)

    stats = _handle(batch, producer, consumer, store, registry, rules)

    assert calls == [
        "begin",
        f"produce:{SETTINGS.clean_topic}",  # init
        f"produce:{SETTINGS.clean_topic}",  # purchase, enriched from the just-staged profile
        f"produce:{SETTINGS.dead_letter_topic}",  # undecodable message
        f"produce:{SETTINGS.profiles_topic}",  # the profile staged by the init
        "send_offsets",
        "commit",
        "store.commit",
    ]
    assert (stats.clean, stats.invalid, stats.profiles) == (2, 1, 1)
    assert store.staged() == {} and store.get("user-1") is not None  # promoted only after commit


def test_kafka_error_aborts_discards_staged_profiles_and_rewinds(calls, batch, registry, rules):
    producer = FakeProducer(calls, fail_commit=KafkaException(FakeKafkaError()))
    # Two assigned partitions: one with a committed offset, one the group never committed to.
    committed = [TopicPartition(SETTINGS.raw_topic, 0, 42), TopicPartition(SETTINGS.raw_topic, 1, -1001)]
    consumer, store = FakeConsumer(calls, committed), RecordingStore(calls)

    stats = _handle(batch, producer, consumer, store, registry, rules)

    assert stats is None  # the loop retries the batch
    assert calls[calls.index("commit"):] == ["commit", "abort", "store.abort", "committed", "seek", "seek"]
    assert "store.commit" not in calls
    assert consumer.sought == [(0, 42), (1, OFFSET_BEGINNING)]
    assert store.staged() == {} and store.get("user-1") is None  # staged profile discarded


def test_fatal_kafka_error_is_reraised_without_abort(calls, batch, registry, rules):
    producer = FakeProducer(calls, fail_commit=KafkaException(FakeKafkaError(fatal=True)))
    store = RecordingStore(calls)

    with pytest.raises(KafkaException):
        _handle(batch, producer, FakeConsumer(calls, []), store, registry, rules)

    assert "abort" not in calls and "store.commit" not in calls


def test_unexpected_exception_aborts_the_transaction_and_reraises(calls, batch, registry, rules, monkeypatch):
    def boom(*args, **kwargs):
        raise KeyError("bug in the processor")

    monkeypatch.setattr(service, "process", boom)
    producer, consumer, store = FakeProducer(calls), FakeConsumer(calls, []), RecordingStore(calls)

    with pytest.raises(KeyError):
        _handle(batch, producer, consumer, store, registry, rules)

    assert calls == ["begin", "abort", "store.abort"]  # nothing committed, no rewind needed
    assert consumer.sought == []


def test_failing_abort_does_not_mask_the_original_exception(calls, batch, registry, rules, monkeypatch):
    def boom(*args, **kwargs):
        raise KeyError("bug in the processor")

    monkeypatch.setattr(service, "process", boom)
    producer = FakeProducer(calls, fail_abort=KafkaException(FakeKafkaError()))
    store = RecordingStore(calls)

    with pytest.raises(KeyError):
        _handle(batch, producer, FakeConsumer(calls, []), store, registry, rules)

    assert calls == ["begin", "abort", "store.abort"]
