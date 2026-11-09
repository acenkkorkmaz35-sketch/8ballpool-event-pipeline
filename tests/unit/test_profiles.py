"""Unit tests for the transactional profile store."""

from pipeline.dq.profiles import Profile, ProfileStore

TR = Profile(country="TR", platform="android")
US = Profile(country="US", platform="ios")


def test_staged_visible_before_commit():
    """A staged profile is visible to ``get`` before any commit."""
    store = ProfileStore()
    store.stage("u1", TR)
    assert store.get("u1") == TR
    assert store.staged() == {"u1": TR}


def test_staged_overrides_committed():
    """A staged profile shadows the committed one for the same user."""
    store = ProfileStore()
    store.load([("u1", TR)])
    store.stage("u1", US)
    assert store.get("u1") == US


def test_abort_discards_staged():
    """Abort drops staged profiles and leaves committed ones untouched."""
    store = ProfileStore()
    store.load([("u1", TR)])
    store.stage("u1", US)
    store.stage("u2", US)
    store.abort()
    assert store.get("u1") == TR
    assert store.get("u2") is None
    assert store.staged() == {}


def test_commit_persists():
    """Commit moves staged profiles into the committed map and clears staging."""
    store = ProfileStore()
    store.stage("u1", TR)
    store.commit()
    assert store.staged() == {}
    assert store.get("u1") == TR
    store.abort()
    assert store.get("u1") == TR


def test_load_bootstrap():
    """Bulk load fills the committed map directly."""
    store = ProfileStore()
    store.load([("u1", TR), ("u2", US)])
    assert store.get("u1") == TR
    assert store.get("u2") == US
    assert store.staged() == {}


def test_staged_returns_copy():
    """Mutating the returned dict does not affect the store."""
    store = ProfileStore()
    store.stage("u1", TR)
    snapshot = store.staged()
    snapshot.clear()
    assert store.staged() == {"u1": TR}


def test_get_unknown_returns_none():
    """An unknown user yields ``None``."""
    assert ProfileStore().get("nobody") is None


def test_profile_json_roundtrip():
    """to_json is compact UTF-8 JSON and from_json is its inverse."""
    raw = TR.to_json()
    assert raw == b'{"country":"TR","platform":"android"}'
    assert Profile.from_json(raw) == TR
    unicode_profile = Profile(country="T\u00fcrkiye", platform="android")
    assert Profile.from_json(unicode_profile.to_json()) == unicode_profile
