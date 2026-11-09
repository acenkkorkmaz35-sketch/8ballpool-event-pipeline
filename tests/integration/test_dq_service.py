"""End-to-end test of the DQ service against a real Kafka broker.

Requires the compose stack (``docker compose up -d kafka init-topics dq-service``) and is
run from the ``tests`` compose service so the broker is reachable as ``kafka:9092``::

    docker compose run --rm tests pytest tests/integration/test_dq_service.py -v

Each run uses a fresh user id, so reruns never collide with earlier data on the topics.
"""

import json
import time
import uuid

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition

from pipeline.settings import Settings

pytestmark = pytest.mark.integration

TIMEOUT_S = 60
SETTINGS = Settings.from_env()


def _collect(topic: str, matches, want: int, timeout_s: float = TIMEOUT_S) -> list[tuple[str, bytes]]:
    """Read ``topic`` from the beginning (``read_committed``) until ``want`` matches or timeout.

    ``matches(key, value)`` selects the records of interest. Reading from the beginning
    with ``read_committed`` also proves that the DQ service's transactions committed:
    records of aborted or open transactions are invisible here.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": SETTINGS.bootstrap_servers,
            "group.id": f"it-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    # Every topic has exactly one partition (see docker/init-topics.sh).
    consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
    found: list[tuple[str, bytes]] = []
    deadline = time.monotonic() + timeout_s
    try:
        while len(found) < want and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            key = msg.key().decode("utf-8") if msg.key() else ""
            if matches(key, msg.value()):
                found.append((key, msg.value()))
    finally:
        consumer.close()
    return found


def test_dq_service_enriches_dead_letters_and_publishes_profile():
    """init + purchase -> enriched purchase, bad bytes -> dead-letter, init -> profile."""
    uid = "9" + str(uuid.uuid4().int)[:9]  # unique per run, never collides with the simulator
    marker = f"not-json-{uuid.uuid4().hex}"
    init = {"event-type": "init", "time": 1700000000, "user-id": uid, "country": "TR", "platform": "android"}
    purchase = {
        "event-type": "in-app-purchase",
        "time": 1700000010,
        "purchase_value": 4.99,
        "user-id": uid,
        "product-id": "coins-500",
    }

    producer = Producer({"bootstrap.servers": SETTINGS.bootstrap_servers})
    producer.produce(SETTINGS.raw_topic, key=uid, value=json.dumps(init))
    producer.produce(SETTINGS.raw_topic, key=uid, value=json.dumps(purchase))
    producer.produce(SETTINGS.raw_topic, key=uid, value=marker.encode())  # not JSON
    assert producer.flush(30) == 0

    clean = _collect(SETTINGS.clean_topic, lambda k, v: k == uid, want=2)
    assert len(clean) == 2, f"expected init + purchase on clean-events, got {len(clean)}"
    events = {json.loads(v)["event-type"]: json.loads(v) for _, v in clean}
    assert events["in-app-purchase"]["country"] == "Turkey"
    assert events["in-app-purchase"]["platform"] == "ANDROID"
    assert events["init"]["country"] == "Turkey"

    dead = _collect(SETTINGS.dead_letter_topic, lambda k, v: marker.encode() in v, want=1)
    assert len(dead) == 1, "expected the non-JSON message on dead-letter"
    payload = json.loads(dead[0][1])
    assert marker in payload["raw"] and payload["errors"]

    profiles = _collect(SETTINGS.profiles_topic, lambda k, v: k == uid, want=1)
    assert len(profiles) == 1, "expected a profile for the new user on user-profiles"
    assert json.loads(profiles[0][1]) == {"country": "Turkey", "platform": "ANDROID"}
