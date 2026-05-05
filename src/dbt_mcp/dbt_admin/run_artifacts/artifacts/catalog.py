"""Parsing and mapping for catalog.json artifacts."""

from __future__ import annotations

from typing import Any

from dbt_artifacts_parser.parser import parse_catalog  # type: ignore[import-untyped]


def parse(raw: dict[str, Any]) -> Any:
    """Parse catalog.json using dbt-artifacts-parser (version-aware).

    No lenient fallback — catalog parsing is not used in the current
    error/warning path.  If strict parsing fails, the exception propagates
    to the caller.

    TODO: Add lenient fallback if catalog is wired into the extraction layer in Part 2.
    """
    return parse_catalog(catalog=raw)
