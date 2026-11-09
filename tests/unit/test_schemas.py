"""Unit tests for the JSON Schema registry (draft-03 validation of raw events)."""

from pathlib import Path

import pytest

from pipeline.schemas import SchemaRegistry

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"


@pytest.fixture(scope="module")
def registry() -> SchemaRegistry:
    """Registry built from the corrected ``schemas/raw`` directory."""
    return SchemaRegistry.from_dir(SCHEMAS / "raw")


def test_valid_events_pass(registry, valid_init, valid_match, valid_purchase):
    """Each valid fixture event produces no errors."""
    for event in (valid_init, valid_match, valid_purchase):
        assert registry.validate(event) == []


def test_init_accepts_integer_user_id(registry, valid_init):
    """init allows an integer user-id."""
    assert registry.validate(valid_init | {"user-id": 42}) == []


def test_match_rejects_integer_user(registry, valid_match):
    """match requires string user ids."""
    assert registry.validate(valid_match | {"user-a": 42}) != []


def test_game_tier_bounds(registry, valid_match):
    """game-tier is valid for 1..5 and invalid outside that range."""
    assert registry.validate(valid_match | {"game-tier": 1}) == []
    assert registry.validate(valid_match | {"game-tier": 5}) == []
    assert registry.validate(valid_match | {"game-tier": 0}) != []
    assert registry.validate(valid_match | {"game-tier": 6}) != []


def test_original_match_schema_is_buggy(valid_match):
    """The supplied schema declares ``minimum`` twice, so tier 3 is rejected."""
    original = SchemaRegistry.from_dir(SCHEMAS / "original")
    assert original.validate(valid_match | {"game-tier": 3}) != []


def test_missing_required_field(registry, valid_init):
    """A missing required field is reported by name."""
    event = {k: v for k, v in valid_init.items() if k != "country"}
    errors = registry.validate(event)
    assert errors != []
    assert any("country" in e for e in errors)


def test_additional_property_rejected(registry, valid_init):
    """Unknown properties are rejected."""
    assert registry.validate(valid_init | {"extra": "x"}) != []


def test_negative_purchase_value_rejected(registry, valid_purchase):
    """purchase_value must be non-negative."""
    assert registry.validate(valid_purchase | {"purchase_value": -1}) != []


def test_unknown_event_type(registry):
    """An unknown event-type yields exactly one error naming it."""
    errors = registry.validate({"event-type": "logout"})
    assert len(errors) == 1
    assert "logout" in errors[0]


def test_missing_event_type(registry):
    """A missing event-type yields exactly one error."""
    errors = registry.validate({"time": 1})
    assert len(errors) == 1


def test_user_b_postmatch_info_optional(registry, valid_match):
    """user-b-postmatch-info may be absent."""
    event = {k: v for k, v in valid_match.items() if k != "user-b-postmatch-info"}
    assert registry.validate(event) == []
