"""YAML-driven rules engine: apply registered transforms to event fields.

A rules file has a top-level ``rules`` list; each entry has ``events`` (event types),
``field`` (dot-notation path), ``transform`` (registered name) and optional ``params``.
Everything is validated when the file is loaded so misconfiguration fails fast.
"""

import copy
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from pipeline.dq import fieldpath, transforms


@dataclass(frozen=True)
class Rule:
    """One compiled rule: where it applies and the transform to run."""

    events: frozenset[str]
    field: str
    fn: Callable[[Any], Any]


class RuleSet:
    """An ordered collection of compiled rules."""

    def __init__(self, rules: list[Rule]):
        """Create a rule set from compiled rules, applied in the given order."""
        self.rules = rules

    def apply(self, event: dict) -> dict:
        """Return a deep copy of ``event`` with every matching rule applied.

        A rule is skipped when the event type is not listed or the field path is absent.
        """
        result = copy.deepcopy(event)
        event_type = result.get("event-type")
        for rule in self.rules:
            if event_type in rule.events and fieldpath.has(result, rule.field):
                fieldpath.set(result, rule.field, rule.fn(fieldpath.get(result, rule.field)))
        return result


def _compile_rule(index: int, entry: Any, base_dir: Path) -> Rule:
    """Validate one YAML entry and compile it; raise ValueError naming rule ``index``."""
    label = f"rule #{index}"
    if not isinstance(entry, dict):
        raise ValueError(f"{label}: must be a mapping")
    for key in ("events", "field", "transform"):
        if key not in entry:
            raise ValueError(f"{label}: missing required key '{key}'")
    events = entry["events"]
    if (
        not isinstance(events, list)
        or not events
        or not all(isinstance(e, str) for e in events)
    ):
        raise ValueError(f"{label}: 'events' must be a non-empty list of event types")
    for key in ("field", "transform"):
        if not isinstance(entry[key], str) or not entry[key]:
            raise ValueError(f"{label}: '{key}' must be a non-empty string")
    params = entry.get("params") or {}
    if not isinstance(params, dict):
        raise ValueError(f"{label}: 'params' must be a mapping")
    try:
        fn = transforms.build(entry["transform"], params, base_dir)
    except ValueError as exc:
        raise ValueError(f"{label} ({entry['field']}): {exc}") from exc
    return Rule(events=frozenset(events), field=entry["field"], fn=fn)


def load_rules(path: Path) -> RuleSet:
    """Load and validate a rules YAML file; relative params resolve against its directory."""
    path = Path(path)
    with path.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    if not isinstance(document, dict) or not isinstance(document.get("rules"), list):
        raise ValueError(f"{path}: expected a top-level 'rules' list")
    base_dir = path.parent
    return RuleSet(
        [_compile_rule(i, entry, base_dir) for i, entry in enumerate(document["rules"], 1)]
    )
