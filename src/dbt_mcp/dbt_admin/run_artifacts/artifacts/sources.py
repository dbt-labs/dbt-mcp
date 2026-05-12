"""Parsing and mapping for sources.json artifacts."""

from __future__ import annotations

import logging
from typing import Any

from dbt_artifacts_parser.parser import parse_sources
from dbt_artifacts_parser.parsers.sources.sources_v1 import (
    SourceFreshnessOutput as SourceFreshnessOutputV1,
    SourceFreshnessRuntimeError as SourceFreshnessRuntimeErrorV1,
    SourcesV1,
)
from dbt_artifacts_parser.parsers.sources.sources_v2 import (
    SourceFreshnessOutput as SourceFreshnessOutputV2,
    SourceFreshnessRuntimeError as SourceFreshnessRuntimeErrorV2,
    SourcesV2,
)
from dbt_artifacts_parser.parsers.sources.sources_v3 import (
    Results as ResultsV3,
    Results1 as Results1V3,
    SourcesV3,
)

from dbt_mcp.dbt_admin.constants import RunResultsStatus
from dbt_mcp.dbt_admin.run_artifacts.artifacts.lenient import (
    LenientSourceResult,
    LenientSources,
)
from dbt_mcp.dbt_admin.run_artifacts.schemas.output import OutputResultSchema

logger = logging.getLogger(__name__)

SourcesParsed = SourcesV1 | SourcesV2 | SourcesV3 | LenientSources

# Individual result entry type — union of all versioned result subtypes plus the lenient fallback.
# SourceFreshnessOutput/Results1 carry max_loaded_at_time_ago_in_s; runtime-error types do not.
SourceResultEntry = (
    SourceFreshnessOutputV1
    | SourceFreshnessRuntimeErrorV1
    | SourceFreshnessOutputV2
    | SourceFreshnessRuntimeErrorV2
    | ResultsV3
    | Results1V3
    | LenientSourceResult
)


def parse(raw: dict[str, Any]) -> SourcesParsed:
    """Parse sources.json using dbt-artifacts-parser (version-aware).

    Falls back to ``LenientSources`` when strict Pydantic validation fails.
    This covers sources.json variants that omit required
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
        return LenientSources.model_validate(raw)


def to_freshness_error(result: SourceResultEntry) -> OutputResultSchema | None:
    """Map a source freshness result to an error output, or None if not an error/fail."""
    if isinstance(result, LenientSourceResult):
        status_val = result.status
        # "fail" is not in the strict sources.json schema, so artifacts with that status reach
        # here via the LenientSources fallback. Keep the check so those cases aren't silently dropped.
        if status_val not in [
            RunResultsStatus.ERROR.value,
            RunResultsStatus.FAIL.value,
        ]:
            return None
        unique_id = result.unique_id
        age = result.max_loaded_at_time_ago_in_s or 0
        return OutputResultSchema(
            unique_id=unique_id,
            relation_name=unique_id.split(".")[-1] if unique_id else "Unknown",
            message=f"Source freshness error: {age:.0f}s since last load (max allowed exceeded)",
        )
    # Strict models: status is always an enum
    status_val = result.status.value
    if status_val not in [RunResultsStatus.ERROR.value, RunResultsStatus.FAIL.value]:
        return None
    unique_id = result.unique_id
    # Only SourceFreshnessOutput (v1/v2) and Results1 (v3) carry timing data
    strict_age: float = (
        result.max_loaded_at_time_ago_in_s
        if isinstance(
            result, (SourceFreshnessOutputV1, SourceFreshnessOutputV2, Results1V3)
        )
        else 0
    )
    return OutputResultSchema(
        unique_id=unique_id,
        relation_name=unique_id.split(".")[-1] if unique_id else "Unknown",
        message=f"Source freshness error: {strict_age:.0f}s since last load (max allowed exceeded)",
    )


def to_freshness_warning(result: SourceResultEntry) -> OutputResultSchema | None:
    """Map a source freshness result to a warning output, or None if not a warning."""
    if isinstance(result, LenientSourceResult):
        status_val = result.status
        if status_val != RunResultsStatus.WARN.value:
            return None
        unique_id = result.unique_id
        age = result.max_loaded_at_time_ago_in_s or 0
        return OutputResultSchema(
            unique_id=unique_id,
            relation_name=unique_id.split(".")[-1] if unique_id else "Unknown",
            message=f"Source freshness warning: {age:.0f}s since last load",
            status="warn",
        )
    # Strict models: status is always an enum
    status_val = result.status.value
    if status_val != RunResultsStatus.WARN.value:
        return None
    unique_id = result.unique_id
    strict_age: float = (
        result.max_loaded_at_time_ago_in_s
        if isinstance(
            result, (SourceFreshnessOutputV1, SourceFreshnessOutputV2, Results1V3)
        )
        else 0
    )
    return OutputResultSchema(
        unique_id=unique_id,
        relation_name=unique_id.split(".")[-1] if unique_id else "Unknown",
        message=f"Source freshness warning: {strict_age:.0f}s since last load",
        status="warn",
    )
