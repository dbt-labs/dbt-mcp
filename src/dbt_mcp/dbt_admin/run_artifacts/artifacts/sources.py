"""Parsing and mapping for sources.json artifacts."""

from __future__ import annotations

import logging
from typing import Any

from dbt_artifacts_parser.parser import parse_sources  # type: ignore[import-untyped]

from dbt_mcp.dbt_admin.constants import RunResultsStatus
from dbt_mcp.dbt_admin.run_artifacts.artifacts.lenient import _AttrDict
from dbt_mcp.dbt_admin.run_artifacts.schemas.output import OutputResultSchema

logger = logging.getLogger(__name__)


def parse(raw: dict[str, Any]) -> Any:
    """Parse sources.json using dbt-artifacts-parser (version-aware).

    Falls back to lenient ``_AttrDict``-based parsing when strict Pydantic
    validation fails.  This covers sources.json variants that omit required
    fields (e.g. ``error``) in result entries.
    """
    try:
        return parse_sources(sources=raw)
    except Exception as e:
        logger.warning(
            "Strict sources parsing failed (%s: %s); "
            "falling back to lenient dict-based parsing.",
            type(e).__name__,
            str(e)[:200],
        )
        return _AttrDict(raw)


def to_freshness_error(result: Any) -> OutputResultSchema | None:
    """Map a source freshness result to an error output, or None if not an error/fail."""
    status_val = getattr(result.status, "value", result.status)
    if status_val not in [RunResultsStatus.ERROR.value, RunResultsStatus.FAIL.value]:
        return None
    unique_id = getattr(result, "unique_id", None)
    age = getattr(result, "max_loaded_at_time_ago_in_s", None) or 0
    return OutputResultSchema(
        unique_id=unique_id,
        relation_name=unique_id.split(".")[-1] if unique_id else "Unknown",
        message=f"Source freshness error: {age:.0f}s since last load (max allowed exceeded)",
    )


def to_freshness_warning(result: Any) -> OutputResultSchema | None:
    """Map a source freshness result to a warning output, or None if not a warning."""
    status_val = getattr(result.status, "value", result.status)
    if status_val != RunResultsStatus.WARN.value:
        return None
    unique_id = getattr(result, "unique_id", None)
    age = getattr(result, "max_loaded_at_time_ago_in_s", None) or 0
    return OutputResultSchema(
        unique_id=unique_id,
        relation_name=unique_id.split(".")[-1] if unique_id else "Unknown",
        message=f"Source freshness warning: {age:.0f}s since last load",
        status="warn",
    )
