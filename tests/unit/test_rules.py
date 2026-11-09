"""Unit tests for the YAML-driven rules engine."""

import copy
from pathlib import Path

import pytest

from pipeline.dq import fieldpath
from pipeline.dq.rules import load_rules

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


@pytest.fixture
def shipped_rules():
    """The rule set shipped in config/rules.yaml."""
    return load_rules(CONFIG_DIR / "rules.yaml")


def test_init_platform_uppercased_and_country_mapped(shipped_rules):
    """The init rules uppercase the platform and map the country code."""
    out = shipped_rules.apply({"event-type": "init", "platform": "android", "country": "TR"})
    assert out["platform"] == "ANDROID"
    assert out["country"] == "Turkey"


def test_nested_match_platform_uppercased(shipped_rules, valid_match):
    """Dot-notation rules reach into nested post-match objects."""
    out = shipped_rules.apply(valid_match)
    assert out["user-a-postmatch-info"]["platform"] == "ANDROID"
    assert out["user-b-postmatch-info"]["platform"] == "IOS"


def test_missing_user_b_info_skipped(shipped_rules, valid_match):
    """A rule whose path is absent is skipped without creating the key."""
    del valid_match["user-b-postmatch-info"]
    out = shipped_rules.apply(valid_match)
    assert "user-b-postmatch-info" not in out
    assert out["user-a-postmatch-info"]["platform"] == "ANDROID"


def test_rule_ignores_other_event_types(shipped_rules, valid_purchase):
    """Rules only fire for the event types they list."""
    valid_purchase["platform"] = "android"
    out = shipped_rules.apply(valid_purchase)
    assert out == valid_purchase


def test_apply_does_not_mutate_input(shipped_rules, valid_init):
    """apply returns a transformed deep copy and leaves the input intact."""
    original = copy.deepcopy(valid_init)
    out = shipped_rules.apply(valid_init)
    assert valid_init == original
    assert out is not valid_init
    assert out["platform"] == "ANDROID"


def test_shipped_rules_load(shipped_rules):
    """The shipped rules file is valid and contains the four spec rules."""
    assert len(shipped_rules.rules) == 4


def _write_rules(tmp_path: Path, body: str) -> Path:
    """Write a rules YAML file into tmp_path and return its path."""
    path = tmp_path / "rules.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def test_load_rejects_unknown_transform(tmp_path):
    """An unknown transform name fails at load time, naming the rule."""
    path = _write_rules(
        tmp_path, "rules:\n  - events: [init]\n    field: platform\n    transform: shout\n"
    )
    with pytest.raises(ValueError, match="rule #1.*shout"):
        load_rules(path)


@pytest.mark.parametrize("missing", ["events", "field", "transform"])
def test_load_rejects_missing_keys(tmp_path, missing):
    """Each required key is validated at load time."""
    entry = {"events": "[init]", "field": "platform", "transform": "upper"}
    del entry[missing]
    body = "rules:\n  - " + "\n    ".join(f"{k}: {v}" for k, v in entry.items()) + "\n"
    with pytest.raises(ValueError, match=f"rule #1.*{missing}"):
        load_rules(_write_rules(tmp_path, body))


def test_load_rejects_bad_params(tmp_path):
    """Invalid transform params fail at load time, naming the rule."""
    path = _write_rules(
        tmp_path,
        "rules:\n  - events: [init]\n    field: country\n    transform: map\n"
        "    params: { mapping_file: missing.csv }\n",
    )
    with pytest.raises(ValueError, match="rule #1.*missing.csv"):
        load_rules(path)


def test_load_rejects_empty_events(tmp_path):
    """events must be a non-empty list of strings."""
    path = _write_rules(
        tmp_path, "rules:\n  - events: []\n    field: platform\n    transform: upper\n"
    )
    with pytest.raises(ValueError, match="rule #1.*events"):
        load_rules(path)


def test_load_rejects_missing_rules_key(tmp_path):
    """The file must have a top-level rules list."""
    with pytest.raises(ValueError, match="rules"):
        load_rules(_write_rules(tmp_path, "other: []\n"))


def test_map_resolved_relative_to_yaml_directory(tmp_path):
    """mapping_file is resolved against the YAML file's directory."""
    (tmp_path / "m.csv").write_text("code,name\nAA,Aland\n", encoding="utf-8")
    path = _write_rules(
        tmp_path,
        "rules:\n  - events: [init]\n    field: country\n    transform: map\n"
        "    params: { mapping_file: m.csv, on_missing: 'NONE' }\n",
    )
    rules = load_rules(path)
    assert rules.apply({"event-type": "init", "country": "AA"})["country"] == "Aland"
    assert rules.apply({"event-type": "init", "country": "TR"})["country"] == "NONE"


def test_fieldpath_get_set_has():
    """fieldpath handles dot-notation paths and raises KeyError on missing segments."""
    d = {"a": {"b": 1}, "c": 2}
    assert fieldpath.get(d, "a.b") == 1
    assert fieldpath.has(d, "a.b") and not fieldpath.has(d, "a.x")
    assert not fieldpath.has(d, "c.x")  # c is not a dict
    with pytest.raises(KeyError):
        fieldpath.get(d, "a.x")
    fieldpath.set(d, "a.b", 5)
    assert d["a"]["b"] == 5
    with pytest.raises(KeyError):
        fieldpath.set(d, "z.y", 1)
