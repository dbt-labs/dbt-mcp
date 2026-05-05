"""Lenient attribute-access wrapper for raw dbt artifact dicts.

Used as a fallback when dbt-artifacts-parser strict Pydantic validation fails
(e.g. preview dbt builds or schema divergence like unknown status values).
"""

from __future__ import annotations

from typing import Any


class _AttrDict:
    """Recursive attribute-access wrapper for raw artifact dicts.

    The extractors use ``getattr(obj, field, default)`` throughout, so they
    work transparently with both parsed Pydantic models and ``_AttrDict``
    instances.

    Handles the ``schema_`` → ``schema`` alias: dbt-artifacts-parser exposes
    the JSON key ``schema`` as a Python attribute named ``schema_`` (to avoid
    shadowing the built-in).  ``_AttrDict`` does the same mapping so callers
    using ``getattr(node, "schema_", None)`` get the right value.

    IMPORTANT: ``__getattr__`` raises ``AttributeError`` for missing keys so
    that ``hasattr()`` returns the correct boolean and patterns like
    ``hasattr(obj, "dict")`` / ``hasattr(obj, "model_dump")`` behave as
    expected by ``_json()`` in the extractor layer.
    """

    # Map Python attribute names → raw JSON dict keys where they differ.
    _PY_TO_JSON: dict[str, str] = {"schema_": "schema"}

    __slots__ = ("_d",)

    def __init__(self, d: dict[str, Any]) -> None:
        object.__setattr__(self, "_d", d)

    def __getattr__(self, name: str) -> Any:
        d: dict[str, Any] = object.__getattribute__(self, "_d")
        # Try the Python name first, then the aliased JSON key.
        for key in (name, self._PY_TO_JSON.get(name)):
            if key is not None and key in d:
                return _wrap(d[key])
        raise AttributeError(name)

    # ── Mapping protocol so iteration works in extractors ─────────────────

    def items(self) -> Any:
        d: dict[str, Any] = object.__getattribute__(self, "_d")
        return list((k, _wrap(v)) for k, v in d.items())

    def values(self) -> Any:
        d: dict[str, Any] = object.__getattribute__(self, "_d")
        return list(_wrap(v) for v in d.values())

    def keys(self) -> Any:
        return object.__getattribute__(self, "_d").keys()

    def get(self, key: str, default: Any = None) -> Any:
        d: dict[str, Any] = object.__getattribute__(self, "_d")
        return _wrap(d[key]) if key in d else default

    def __iter__(self) -> Any:
        return iter(object.__getattribute__(self, "_d"))

    def __len__(self) -> int:
        return len(object.__getattribute__(self, "_d"))

    def __bool__(self) -> bool:
        return bool(object.__getattribute__(self, "_d"))

    def __contains__(self, item: Any) -> bool:
        return item in object.__getattribute__(self, "_d")


def _wrap(val: Any) -> Any:
    """Recursively wrap dicts in ``_AttrDict``; leave other types as-is."""
    if isinstance(val, dict):
        return _AttrDict(val)
    if isinstance(val, list):
        return [_wrap(v) for v in val]
    return val
