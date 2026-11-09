"""Unit tests for event enrichment with country/platform from the profile store."""

import copy

from pipeline.dq.enrich import enrich
from pipeline.dq.profiles import Profile, ProfileStore


def _store() -> ProfileStore:
    """Build a store with two committed users."""
    store = ProfileStore()
    store.load(
        [
            ("user-1", Profile(country="TR", platform="android")),
            ("user-2", Profile(country="US", platform="ios")),
        ]
    )
    return store


def test_enrich_purchase(valid_purchase):
    """A purchase gets the buyer's country and platform."""
    out = enrich(valid_purchase, _store())
    assert out["country"] == "TR"
    assert out["platform"] == "android"


def test_enrich_match_both_users(valid_match):
    """A match gets the country of both users."""
    out = enrich(valid_match, _store())
    assert out["user-a-country"] == "TR"
    assert out["user-b-country"] == "US"


def test_enrich_match_without_user_b_info(valid_match):
    """``user-b-country`` is set even when user-b has no post-match info."""
    del valid_match["user-b-postmatch-info"]
    out = enrich(valid_match, _store())
    assert out["user-b-country"] == "US"


def test_enrich_match_platform_from_postmatch_info(valid_match):
    """Post-match info wins over the profile platform for both participants."""
    valid_match["user-a-postmatch-info"]["platform"] = "ANDROID"
    valid_match["user-b-postmatch-info"]["platform"] = "IOS"
    store = ProfileStore()
    store.load([("user-1", Profile("TR", "WINDOWS")), ("user-2", Profile("US", "WINDOWS"))])
    out = enrich(valid_match, store)
    assert out["user-a-platform"] == "ANDROID"
    assert out["user-b-platform"] == "IOS"


def test_enrich_match_platform_falls_back_to_profile(valid_match):
    """Without ``user-b-postmatch-info`` the platform comes from the profile store."""
    del valid_match["user-b-postmatch-info"]
    store = ProfileStore()
    store.load([("user-2", Profile("US", "IOS"))])
    out = enrich(valid_match, store)
    assert out["user-b-platform"] == "IOS"


def test_enrich_match_platform_empty_info_platform_falls_back_to_profile(valid_match):
    """An empty post-match platform is treated as missing."""
    valid_match["user-b-postmatch-info"]["platform"] = ""
    store = ProfileStore()
    store.load([("user-2", Profile("US", "IOS"))])
    assert enrich(valid_match, store)["user-b-platform"] == "IOS"


def test_enrich_match_platform_unknown_when_no_info_and_unknown_user(valid_match):
    """No post-match info and no profile gives ``UNKNOWN``."""
    del valid_match["user-b-postmatch-info"]
    out = enrich(valid_match, ProfileStore())
    assert out["user-b-platform"] == "UNKNOWN"


def test_enrich_unknown_user(valid_purchase, valid_match):
    """Users without a profile map to ``UNKNOWN``."""
    empty = ProfileStore()
    purchase = enrich(valid_purchase, empty)
    assert purchase["country"] == "UNKNOWN"
    assert purchase["platform"] == "UNKNOWN"
    match = enrich(valid_match, empty)
    assert match["user-a-country"] == "UNKNOWN"
    assert match["user-b-country"] == "UNKNOWN"
    # Post-match info still attributes the platform when the profile is missing.
    assert match["user-a-platform"] == "android"
    assert match["user-b-platform"] == "ios"


def test_integer_user_id_normalised(valid_purchase):
    """An integer user id is normalised to a string and found in the store."""
    store = ProfileStore()
    store.load([("42", Profile(country="DE", platform="ios"))])
    valid_purchase["user-id"] = 42
    out = enrich(valid_purchase, store)
    assert out["user-id"] == "42"
    assert out["country"] == "DE"


def test_integer_match_users_normalised(valid_match):
    """Integer ``user-a``/``user-b`` ids are normalised for match events."""
    store = ProfileStore()
    store.load([("1", Profile("TR", "android")), ("2", Profile("US", "ios"))])
    valid_match["user-a"] = 1
    valid_match["user-b"] = 2
    out = enrich(valid_match, store)
    assert (out["user-a"], out["user-b"]) == ("1", "2")
    assert (out["user-a-country"], out["user-b-country"]) == ("TR", "US")


def test_enrich_sees_staged_profile(valid_purchase):
    """A profile staged earlier in the batch enriches later events."""
    store = ProfileStore()
    store.stage("user-1", Profile(country="FR", platform="ios"))
    assert enrich(valid_purchase, store)["country"] == "FR"


def test_enrich_init_only_normalises(valid_init):
    """Init events gain no fields; only the id is normalised."""
    valid_init["user-id"] = 7
    out = enrich(valid_init, _store())
    assert out == {**valid_init, "user-id": "7"}


def test_enrich_does_not_mutate_input(valid_match):
    """The input event is left untouched and the result is a deep copy."""
    original = copy.deepcopy(valid_match)
    out = enrich(valid_match, _store())
    assert valid_match == original
    out["user-a-postmatch-info"]["device"] = "changed"
    assert valid_match == original


def test_unknown_event_type_copied_unchanged():
    """Unknown event types are returned as an equal but distinct copy."""
    event = {"event-type": "other", "payload": {"x": 1}}
    out = enrich(event, _store())
    assert out == event
    assert out is not event
    assert out["payload"] is not event["payload"]
