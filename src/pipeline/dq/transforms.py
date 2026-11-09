"""Transform registry and the built-in value transforms.

A transform is a factory ``(params, base_dir) -> Callable[[value], value]`` registered
under a name with :func:`register`. Factories validate their params and do any expensive
setup (such as loading a mapping file) once, when the rule set is loaded.
"""

import csv
from collections.abc import Callable
from pathlib import Path
from typing import Any

TransformFactory = Callable[[dict, Path], Callable[[Any], Any]]

_REGISTRY: dict[str, TransformFactory] = {}


def register(name: str) -> Callable[[TransformFactory], TransformFactory]:
    """Decorator registering a transform factory under ``name``."""

    def decorator(factory: TransformFactory) -> TransformFactory:
        _REGISTRY[name] = factory
        return factory

    return decorator


def unregister(name: str) -> None:
    """Remove a registered transform (used to undo registrations, e.g. in tests)."""
    _REGISTRY.pop(name, None)


def build(name: str, params: dict, base_dir: Path) -> Callable[[Any], Any]:
    """Build the transform ``name``; raise ValueError if it is unknown or its params are bad."""
    try:
        factory = _REGISTRY[name]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY))
        raise ValueError(f"unknown transform '{name}' (known: {known})") from None
    return factory(params, base_dir)


def _string_transform(method: str) -> TransformFactory:
    """Create a factory for a str method; non-string values pass through unchanged."""

    def factory(params: dict, base_dir: Path) -> Callable[[Any], Any]:
        return lambda value: getattr(value, method)() if isinstance(value, str) else value

    return factory


register("upper")(_string_transform("upper"))
register("lower")(_string_transform("lower"))
register("trim")(_string_transform("strip"))


@register("map")
def _map_factory(params: dict, base_dir: Path) -> Callable[[Any], Any]:
    """Map values through a ``code,name`` CSV file.

    Params: ``mapping_file`` (required, relative to ``base_dir``) and ``on_missing``:
    ``"keep"`` (default) keeps the original value, ``"null"`` or an unquoted YAML ``null``
    (parsed to None) yields None, any other string is used as a literal replacement.
    """
    mapping_file = params.get("mapping_file")
    if not isinstance(mapping_file, str) or not mapping_file:
        raise ValueError("map requires a 'mapping_file' param (string)")
    on_missing = params.get("on_missing", "keep")
    if on_missing is None:  # unquoted YAML `null` means the same as the string "null"
        on_missing = "null"
    if not isinstance(on_missing, str):
        raise ValueError(f"map param 'on_missing' must be a string, got {on_missing!r}")
    path = base_dir / mapping_file
    if not path.is_file():
        raise ValueError(f"map mapping_file not found: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        mapping = {row["code"]: row["name"] for row in csv.DictReader(handle)}

    def apply(value: Any) -> Any:
        if isinstance(value, str) and value in mapping:
            return mapping[value]
        if on_missing == "keep":
            return value
        if on_missing == "null":
            return None
        return on_missing

    return apply
