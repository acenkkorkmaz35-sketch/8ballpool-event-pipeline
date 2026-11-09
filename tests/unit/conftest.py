"""Shared pytest fixtures: one valid example event of each type."""

import pytest


@pytest.fixture
def valid_init() -> dict:
    """A schema-valid ``init`` event."""
    return {
        "event-type": "init",
        "time": 1700000000,
        "user-id": "user-1",
        "country": "TR",
        "platform": "android",
    }


@pytest.fixture
def valid_match() -> dict:
    """A schema-valid ``match`` event with post-match info for both users."""
    return {
        "event-type": "match",
        "time": 1700000060,
        "user-a": "user-1",
        "user-b": "user-2",
        "user-a-postmatch-info": {
            "coin-balance-after-match": 1500,
            "level-after-match": 12,
            "device": "Pixel 7",
            "platform": "android",
        },
        "user-b-postmatch-info": {
            "coin-balance-after-match": 900,
            "level-after-match": 8,
            "device": "iPhone 14",
            "platform": "ios",
        },
        "winner": "user-1",
        "game-tier": 3,
        "duration": 240,
    }


@pytest.fixture
def valid_purchase() -> dict:
    """A schema-valid ``in-app-purchase`` event."""
    return {
        "event-type": "in-app-purchase",
        "time": 1700000120,
        "purchase_value": 4.99,
        "user-id": "user-1",
        "product-id": "coins-500",
    }
