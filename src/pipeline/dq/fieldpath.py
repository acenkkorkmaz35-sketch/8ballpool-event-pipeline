"""Dot-notation access to nested dict fields (``user-a-postmatch-info.platform``)."""

from typing import Any


def _walk(d: dict, segments: list[str]) -> dict:
    """Return the dict that holds the last segment, raising KeyError if a step is missing."""
    node: Any = d
    for segment in segments[:-1]:
        if not isinstance(node, dict) or segment not in node:
            raise KeyError(".".join(segments))
        node = node[segment]
    if not isinstance(node, dict):
        raise KeyError(".".join(segments))
    return node


def get(d: dict, path: str) -> Any:
    """Return the value at ``path``; raise KeyError if any segment is missing."""
    segments = path.split(".")
    parent = _walk(d, segments)
    if segments[-1] not in parent:
        raise KeyError(path)
    return parent[segments[-1]]


def set(d: dict, path: str, value: Any) -> None:  # noqa: A001 - mirrors the task interface
    """Set the value at ``path`` in place; raise KeyError if a parent segment is missing."""
    segments = path.split(".")
    _walk(d, segments)[segments[-1]] = value


def has(d: dict, path: str) -> bool:
    """Return True if every segment of ``path`` exists."""
    try:
        get(d, path)
    except KeyError:
        return False
    return True
