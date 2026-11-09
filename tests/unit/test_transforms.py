"""Unit tests for the transform registry and the built-in transforms."""

from pathlib import Path

import pytest

from pipeline.dq import transforms
from pipeline.dq.rules import load_rules

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _build(name: str, params: dict | None = None):
    """Build a transform with config/ as the base directory."""
    return transforms.build(name, params or {}, CONFIG_DIR)


def test_upper():
    """upper converts strings to upper case."""
    assert _build("upper")("android") == "ANDROID"


def test_upper_non_string_passthrough():
    """Non-string input is returned unchanged."""
    assert _build("upper")(42) == 42
    assert _build("upper")(None) is None


def test_lower():
    """lower converts strings to lower case."""
    assert _build("lower")("IOS") == "ios"


def test_trim():
    """trim strips surrounding whitespace."""
    assert _build("trim")("  tr \n") == "tr"


def test_map_known_code():
    """map resolves known codes through the CSV mapping."""
    fn = _build("map", {"mapping_file": "country_codes.csv"})
    assert fn("TR") == "Turkey"
    assert fn("GB") == "United Kingdom"


def test_map_on_missing_keep():
    """The default on_missing policy keeps the original value."""
    fn = _build("map", {"mapping_file": "country_codes.csv", "on_missing": "keep"})
    assert fn("ZZ") == "ZZ"
    assert _build("map", {"mapping_file": "country_codes.csv"})("ZZ") == "ZZ"


def test_map_on_missing_null():
    """on_missing 'null' replaces unknown values with None."""
    fn = _build("map", {"mapping_file": "country_codes.csv", "on_missing": "null"})
    assert fn("ZZ") is None


def test_map_on_missing_literal():
    """Any other on_missing string is used as a literal replacement."""
    fn = _build("map", {"mapping_file": "country_codes.csv", "on_missing": "UNKNOWN"})
    assert fn("ZZ") == "UNKNOWN"


def test_map_requires_mapping_file():
    """map without mapping_file is rejected."""
    with pytest.raises(ValueError, match="mapping_file"):
        _build("map")


def test_map_missing_file_raises():
    """A mapping file that does not exist is rejected at build time."""
    with pytest.raises(ValueError, match="nope.csv"):
        _build("map", {"mapping_file": "nope.csv"})


def test_map_invalid_on_missing_type_raises():
    """on_missing must be a string."""
    with pytest.raises(ValueError, match="on_missing"):
        _build("map", {"mapping_file": "country_codes.csv", "on_missing": 5})


def test_unknown_transform_raises():
    """Building an unregistered transform fails with ValueError."""
    with pytest.raises(ValueError, match="no_such_transform"):
        _build("no_such_transform")


def test_custom_transform_registration():
    """A transform registered from outside the module can be built and applied."""

    @transforms.register("reverse")
    def _reverse(params, base_dir):
        return lambda value: value[::-1] if isinstance(value, str) else value

    try:
        assert _build("reverse")("abc") == "cba"
    finally:
        transforms.unregister("reverse")
    with pytest.raises(ValueError):
        _build("reverse")


def test_map_on_missing_yaml_null_behaves_like_string_null(tmp_path):
    """An unquoted YAML ``on_missing: null`` (parsed to None) maps a missing code to None."""
    (tmp_path / "codes.csv").write_text("code,name\nTR,Turkey\n", encoding="utf-8")
    (tmp_path / "rules.yaml").write_text(
        "rules:\n"
        "  - events: [init]\n"
        "    field: country\n"
        "    transform: map\n"
        "    params: { mapping_file: codes.csv, on_missing: null }\n",
        encoding="utf-8",
    )
    rules = load_rules(tmp_path / "rules.yaml")
    known = rules.apply({"event-type": "init", "country": "TR"})
    missing = rules.apply({"event-type": "init", "country": "ZZ"})
    assert known["country"] == "Turkey"
    assert missing["country"] is None
