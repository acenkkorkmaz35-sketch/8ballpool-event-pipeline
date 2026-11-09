"""Unit tests for the pure DQ processor (parse -> validate -> transform -> enrich)."""

import json
from pathlib import Path

import pytest

from pipeline.dq.processor import (
    MAX_DEAD_LETTER_ERROR_CHARS,
    MAX_DEAD_LETTER_ERRORS,
    MAX_DEAD_LETTER_RAW_CHARS,
    MAX_FUTURE_SKEW_SECONDS,
    MAX_PURCHASE_VALUE,
    Clean,
    Invalid,
    process,
)
from pipeline.dq.profiles import Profile, ProfileStore
from pipeline.dq.rules import load_rules
from pipeline.schemas import SchemaRegistry

ROOT = Path(__file__).resolve().parents[2]

# Fixture events use times around 1_700_000_000; the injected clock sits just after them.
NOW = 1_700_000_200.0


@pytest.fixture(scope="module")
def registry() -> SchemaRegistry:
    """Registry for the raw (input) schemas."""
    return SchemaRegistry.from_dir(ROOT / "schemas" / "raw")


@pytest.fixture(scope="module")
def clean_registry() -> SchemaRegistry:
    """Registry for the clean-events (output) schemas."""
    return SchemaRegistry.from_dir(ROOT / "schemas" / "clean")


@pytest.fixture(scope="module")
def rules():
    """The shipped rule set."""
    return load_rules(ROOT / "config" / "rules.yaml")


@pytest.fixture
def store() -> ProfileStore:
    """A fresh empty profile store."""
    return ProfileStore()


def _raw(event: dict) -> bytes:
    """Encode an event as JSON bytes, as it would arrive from Kafka."""
    return json.dumps(event).encode("utf-8")


def test_non_json_is_invalid(registry, rules, store):
    """Garbage bytes yield an Invalid with a parse error and keep the raw bytes."""
    result = process(b"not json {", registry, rules, store)
    assert isinstance(result, Invalid)
    assert result.raw == b"not json {"
    assert result.errors and "JSON" in result.errors[0]


def test_non_utf8_is_invalid(registry, rules, store):
    """Bytes that are not UTF-8 yield an Invalid instead of raising."""
    result = process(b"\xff\xfe{}", registry, rules, store)
    assert isinstance(result, Invalid)
    assert "UTF-8" in result.errors[0]


def test_schema_error_is_invalid(registry, rules, store, valid_purchase):
    """A schema-violating event yields an Invalid with the validation errors."""
    valid_purchase["purchase_value"] = -1
    result = process(_raw(valid_purchase), registry, rules, store)
    assert isinstance(result, Invalid)
    assert result.errors


def test_init_stages_transformed_profile(registry, rules, store, valid_init):
    """Init stages the profile using the rule-transformed country and platform."""
    valid_init["country"] = "TR"
    valid_init["platform"] = "android"
    result = process(_raw(valid_init), registry, rules, store)
    assert isinstance(result, Clean)
    assert store.get("user-1") == Profile("Turkey", "ANDROID")
    assert store.staged() == {"user-1": Profile("Turkey", "ANDROID")}
    assert result.key == "user-1"
    assert result.event["country"] == "Turkey"


def test_integer_init_user_id_staged_as_string(registry, rules, store, valid_init, valid_purchase):
    """An integer init user-id is staged under its string form and found by later events."""
    valid_init["user-id"] = 42
    init = process(_raw(valid_init), registry, rules, store)
    assert isinstance(init, Clean)
    assert "42" in store.staged()
    assert init.key == "42"
    assert init.event["user-id"] == "42"

    valid_purchase["user-id"] = "42"
    purchase = process(_raw(valid_purchase), registry, rules, store)
    assert isinstance(purchase, Clean)
    assert purchase.event["country"] == "Turkey"


def test_purchase_after_init_same_batch_is_enriched(registry, rules, store, valid_init, valid_purchase):
    """A purchase right after its init (no commit in between) gets the country."""
    process(_raw(valid_init), registry, rules, store)
    result = process(_raw(valid_purchase), registry, rules, store)
    assert isinstance(result, Clean)
    assert result.event["country"] == "Turkey"
    assert result.event["platform"] == "ANDROID"
    assert set(store.staged()) == {"user-1"}  # only the init staged a profile, not the purchase
    assert result.key == "user-1"


def test_match_key_is_user_a_and_enriched(registry, rules, store, valid_init, valid_match):
    """Match events are keyed by user-a and carry both users' countries."""
    process(_raw(valid_init), registry, rules, store)
    result = process(_raw(valid_match), registry, rules, store)
    assert isinstance(result, Clean)
    assert result.key == "user-1"
    assert result.event["user-a-country"] == "Turkey"
    assert result.event["user-b-country"] == "UNKNOWN"
    # Platforms come from the (uppercased) post-match info.
    assert result.event["user-a-platform"] == "ANDROID"
    assert result.event["user-b-platform"] == "IOS"


def test_match_without_user_b_info_uses_profile_platform(
    registry, rules, store, clean_registry, valid_init, valid_match
):
    """A match without ``user-b-postmatch-info`` takes user-b's platform from its init."""
    del valid_match["user-b-postmatch-info"]
    user_b_init = {**valid_init, "user-id": "user-2", "platform": "ios"}
    process(_raw(valid_init), registry, rules, store)
    process(_raw(user_b_init), registry, rules, store)
    result = process(_raw(valid_match), registry, rules, store)
    assert isinstance(result, Clean)
    assert result.event["user-b-platform"] == "IOS"
    assert clean_registry.validate(result.event) == []


def test_match_without_user_b_info_and_unknown_user_is_unknown(
    registry, rules, store, clean_registry, valid_match
):
    """No post-match info and no profile gives ``UNKNOWN`` and still validates."""
    del valid_match["user-b-postmatch-info"]
    result = process(_raw(valid_match), registry, rules, store)
    assert isinstance(result, Clean)
    assert result.event["user-b-platform"] == "UNKNOWN"
    assert clean_registry.validate(result.event) == []


def test_clean_events_validate_against_clean_schemas(
    registry, rules, store, clean_registry, valid_init, valid_match, valid_purchase
):
    """Every processed fixture conforms to the clean-events contract."""
    for event in (valid_init, valid_match, valid_purchase):
        result = process(_raw(event), registry, rules, store)
        assert isinstance(result, Clean)
        assert clean_registry.validate(result.event) == []


def test_dead_letter_payload():
    """The dead-letter payload is JSON with the errors and the raw text."""
    payload = Invalid(raw=b"\xffbad", errors=["boom"]).to_dead_letter()
    data = json.loads(payload)
    assert data["errors"] == ["boom"]
    assert data["raw"] == "\ufffdbad"


def test_dead_letter_truncates_large_raw():
    """Huge invalid input is cut so the dead-letter record stays far below 1 MB."""
    raw = b"\xff" * 2_000_000  # invalid UTF-8: each byte becomes a 3-byte replacement char
    payload = Invalid(raw=raw, errors=["boom"]).to_dead_letter()
    assert len(payload) < 64 * 1024
    data = json.loads(payload)
    assert data["truncated"] is True
    assert data["raw_size_bytes"] == 2_000_000
    assert len(data["raw"]) == MAX_DEAD_LETTER_RAW_CHARS
    assert data["errors"] == ["boom"]


def test_dead_letter_small_raw_is_not_truncated():
    """Small input is kept whole and flagged as not truncated."""
    data = json.loads(Invalid(raw=b"not json", errors=["boom"]).to_dead_letter())
    assert data["truncated"] is False
    assert data["raw_size_bytes"] == 8
    assert data["raw"] == "not json"


def test_dead_letter_bounds_error_list():
    """Many or very long error messages are clipped."""
    errors = ["x" * 5000] * 50
    data = json.loads(Invalid(raw=b"{}", errors=errors).to_dead_letter())
    assert len(data["errors"]) == MAX_DEAD_LETTER_ERRORS + 1
    assert data["errors"][-1] == "... and 30 more errors"
    assert all(len(e) <= MAX_DEAD_LETTER_ERROR_CHARS + 3 for e in data["errors"][:-1])


def test_invalid_init_does_not_stage_profile(registry, rules, store, valid_init):
    """An init that fails validation (negative time) is Invalid and stages no profile."""
    valid_init["time"] = -1
    result = process(_raw(valid_init), registry, rules, store)
    assert isinstance(result, Invalid)
    assert store.staged() == {}


def _clean_purchase() -> dict:
    return {
        "event-type": "in-app-purchase",
        "time": 1700000120,
        "purchase_value": 4.99,
        "user-id": "user-1",
        "product-id": "coins-500",
        "country": "Turkey",
        "platform": "ANDROID",
    }


def test_clean_schema_accepts_valid_purchase(clean_registry):
    """Sanity check for the negative contract tests below."""
    assert clean_registry.validate(_clean_purchase()) == []


def test_clean_schema_rejects_integer_user_id_on_purchase(clean_registry):
    """The clean contract carries string user ids only (the DQ service normalises them)."""
    event = {**_clean_purchase(), "user-id": 42}
    assert clean_registry.validate(event) != []


def test_clean_schema_rejects_purchase_without_country(clean_registry):
    """The enrichment field ``country`` is mandatory on a clean purchase."""
    event = _clean_purchase()
    del event["country"]
    assert clean_registry.validate(event) != []


def test_clean_schema_rejects_match_without_user_b_platform(
    registry, rules, store, clean_registry, valid_match
):
    """The enrichment field ``user-b-platform`` is mandatory on a clean match."""
    result = process(_raw(valid_match), registry, rules, store)
    assert isinstance(result, Clean)
    assert clean_registry.validate(result.event) == []
    del result.event["user-b-platform"]
    assert clean_registry.validate(result.event) != []


# ---------------------------------------------------------------------------
# Plausibility: schema-valid is not semantically valid
# ---------------------------------------------------------------------------


def test_millisecond_time_is_invalid(registry, rules, store, valid_purchase):
    """A millisecond epoch (a typical client bug) is far in the future and is dead-lettered."""
    valid_purchase["time"] = 1_700_000_100_000
    result = process(_raw(valid_purchase), registry, rules, store, now=lambda: NOW)
    assert isinstance(result, Invalid)
    assert any("time" in e and "future" in e for e in result.errors)


def test_future_time_beyond_skew_is_invalid(registry, rules, store, valid_purchase):
    """A time one second past the allowed skew is rejected."""
    valid_purchase["time"] = int(NOW) + MAX_FUTURE_SKEW_SECONDS + 1
    result = process(_raw(valid_purchase), registry, rules, store, now=lambda: NOW)
    assert isinstance(result, Invalid)
    assert any("future" in e for e in result.errors)


def test_time_within_skew_is_valid(registry, rules, store, valid_purchase):
    """A slightly-ahead client clock (exactly the skew) is accepted; there is no lower bound."""
    valid_purchase["time"] = int(NOW) + MAX_FUTURE_SKEW_SECONDS
    assert isinstance(process(_raw(valid_purchase), registry, rules, store, now=lambda: NOW), Clean)
    valid_purchase["time"] = 1  # very old events are harmless: Spark drops them as late
    assert isinstance(process(_raw(valid_purchase), registry, rules, store, now=lambda: NOW), Clean)


def test_infinite_purchase_value_is_invalid(registry, rules, store, valid_purchase):
    """``1e400`` parses to ``inf`` in Python; it must not reach clean-events or Postgres."""
    raw = _raw(valid_purchase).replace(b"4.99", b"1e400")
    assert b"1e400" in raw
    result = process(raw, registry, rules, store, now=lambda: NOW)
    assert isinstance(result, Invalid)
    assert any("finite" in e for e in result.errors)


def test_purchase_value_above_cap_is_invalid(registry, rules, store, valid_purchase):
    """A purchase above ``MAX_PURCHASE_VALUE`` (it could overflow numeric(14,2)) and is rejected."""
    valid_purchase["purchase_value"] = MAX_PURCHASE_VALUE + 0.01
    result = process(_raw(valid_purchase), registry, rules, store, now=lambda: NOW)
    assert isinstance(result, Invalid)
    assert any("purchase_value" in e for e in result.errors)
    valid_purchase["purchase_value"] = MAX_PURCHASE_VALUE
    assert isinstance(process(_raw(valid_purchase), registry, rules, store, now=lambda: NOW), Clean)


def test_implausible_init_does_not_stage_profile(registry, rules, store, valid_init):
    """An init with an implausible time is dead-lettered and stages no profile."""
    valid_init["time"] = 1_700_000_100_000
    result = process(_raw(valid_init), registry, rules, store, now=lambda: NOW)
    assert isinstance(result, Invalid)
    assert store.staged() == {}
