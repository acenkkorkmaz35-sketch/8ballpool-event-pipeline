"""Unit tests for the deterministic 8 Ball Pool event simulator."""

import json
from pathlib import Path

import pytest

from pipeline.generator.simulator import UserSimulator
from pipeline.schemas import SchemaRegistry

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"
NOW = 1_700_000_000


@pytest.fixture(scope="module")
def registry() -> SchemaRegistry:
    """Registry built from the ``schemas/raw`` directory."""
    return SchemaRegistry.from_dir(SCHEMAS / "raw")


def _events(sim: UserSimulator, n: int) -> list[tuple[str, dict]]:
    """Generate ``n`` events and decode them (only valid for ``invalid_rate=0``)."""
    out = []
    for i in range(n):
        key, value = sim.next_event(NOW + i)
        out.append((key, json.loads(value)))
    return out


def _uid(value) -> str:
    """Normalise an int-or-string user id to a string."""
    return str(value)


def test_first_event_per_user_is_init():
    """Every user's first appearance (as key or as a match participant) is an init."""
    sim = UserSimulator(seed=1, invalid_rate=0)
    seen: set[str] = set()
    for key, event in _events(sim, 1000):
        users = (
            [event["user-a"], event["user-b"]]
            if event["event-type"] == "match"
            else [_uid(event["user-id"])]
        )
        for user in users:
            if user not in seen:
                assert event["event-type"] == "init"
                seen.add(user)
        assert key == (event["user-a"] if event["event-type"] == "match" else _uid(event["user-id"]))


def test_valid_events_pass_raw_schemas(registry):
    """With ``invalid_rate=0`` every event validates against the raw schemas."""
    sim = UserSimulator(seed=2, invalid_rate=0)
    for key, value in (sim.next_event(NOW + i) for i in range(2000)):
        assert isinstance(key, str) and isinstance(value, bytes)
        assert registry.validate(json.loads(value)) == []


def test_invalid_events_fail(registry):
    """With ``invalid_rate=1`` no event is valid (non-JSON counts as invalid)."""
    sim = UserSimulator(seed=3, invalid_rate=1)
    for i in range(200):
        _, value = sim.next_event(NOW + i)
        try:
            event = json.loads(value)
        except ValueError:
            continue
        assert registry.validate(event) != []


def test_match_users_distinct_and_initialised():
    """A match references two different users that were both initialised before."""
    sim = UserSimulator(seed=4, invalid_rate=0)
    initialised: set[str] = set()
    matches = 0
    for _, event in _events(sim, 1500):
        if event["event-type"] == "init":
            initialised.add(_uid(event["user-id"]))
        elif event["event-type"] == "match":
            matches += 1
            assert event["user-a"] != event["user-b"]
            assert {event["user-a"], event["user-b"]} <= initialised
            assert event["winner"] in (event["user-a"], event["user-b"])
    assert matches > 0


def test_deterministic_with_seed():
    """The same seed and inputs yield identical output; another seed differs."""
    def run(seed: int) -> list[tuple[str, bytes]]:
        sim = UserSimulator(seed=seed, invalid_rate=0.1)
        return [sim.next_event(NOW + i) for i in range(300)]

    assert run(7) == run(7)
    assert run(7) != run(8)


def test_integer_and_string_ids_refer_to_same_user():
    """A user initialised with an int id is referenced in matches as a string."""
    sim = UserSimulator(seed=5, invalid_rate=0)
    int_init_ids: set[str] = set()
    matched: set[str] = set()
    for _, event in _events(sim, 3000):
        if event["event-type"] == "init" and isinstance(event["user-id"], int):
            int_init_ids.add(str(event["user-id"]))
        elif event["event-type"] == "match":
            assert isinstance(event["user-a"], str) and isinstance(event["user-b"], str)
            matched |= {event["user-a"], event["user-b"]}
    assert int_init_ids & matched


def test_event_mix_and_value_ranges():
    """Event shapes respect the documented ranges and exercise DQ edge cases."""
    sim = UserSimulator(seed=6, invalid_rate=0)
    events = [e for _, e in _events(sim, 4000)]
    by_type = {t: [e for e in events if e["event-type"] == t] for t in ("init", "match", "in-app-purchase")}
    assert all(by_type.values())
    assert len(by_type["match"]) > len(by_type["in-app-purchase"])

    assert {e["platform"] for e in by_type["init"]} == {"android", "ios", "Android", "iOS"}
    countries = {e["country"] for e in by_type["init"]}
    assert {"TR", "GB", "US"} <= countries and len(countries) >= 10
    assert any(isinstance(e["user-id"], int) for e in by_type["init"])
    assert any(isinstance(e["user-id"], str) for e in by_type["init"])

    for e in by_type["in-app-purchase"]:
        assert 0.99 <= e["purchase_value"] <= 49.99
        assert round(e["purchase_value"], 2) == e["purchase_value"]
    for e in by_type["match"]:
        assert 1 <= e["game-tier"] <= 5
        assert 60 <= e["duration"] <= 900
        assert e["time"] >= 0
    assert any("user-b-postmatch-info" not in e for e in by_type["match"])
    assert any("user-b-postmatch-info" in e for e in by_type["match"])


def test_user_ids_start_at_100000_and_increment():
    """Fresh users get consecutive numeric ids starting from 100000."""
    sim = UserSimulator(seed=9, invalid_rate=0, new_user_prob=1.0)
    keys = [sim.next_event(NOW + i)[0] for i in range(5)]
    assert keys == ["100000", "100001", "100002", "100003", "100004"]


def test_late_event_jitter():
    """Event time lags ``now`` by 0..30 seconds, with some variation."""
    sim = UserSimulator(seed=10, invalid_rate=0)
    lags = set()
    for _ in range(500):
        _, value = sim.next_event(NOW)
        lags.add(NOW - json.loads(value)["time"])
    assert min(lags) >= 0 and max(lags) <= 30
    assert len(lags) > 10
