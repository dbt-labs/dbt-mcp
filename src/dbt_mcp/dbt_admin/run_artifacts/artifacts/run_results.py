"""Parsing and mapping for run_results.json artifacts."""

from __future__ import annotations

import logging
from typing import Any

from dbt_artifacts_parser.parser import parse_run_results
from dbt_artifacts_parser.parsers.run_results.run_results_v1 import (
    RunResultOutput as RunResultOutputV1,
    RunResultsV1,
)
from dbt_artifacts_parser.parsers.run_results.run_results_v2 import (
    RunResultOutput as RunResultOutputV2,
    RunResultsV2,
)
from dbt_artifacts_parser.parsers.run_results.run_results_v3 import (
    RunResultOutput as RunResultOutputV3,
    RunResultsV3,
)
from dbt_artifacts_parser.parsers.run_results.run_results_v4 import (
    RunResultOutput as RunResultOutputV4,
    RunResultsV4,
)
from dbt_artifacts_parser.parsers.run_results.run_results_v5 import (
    RunResultOutput as RunResultOutputV5,
    RunResultsV5,
)
from dbt_artifacts_parser.parsers.run_results.run_results_v6 import (
    Result as ResultV6,
    RunResultsV6,
)

from dbt_mcp.dbt_admin.constants import RunResultsStatus
from dbt_mcp.dbt_admin.run_artifacts.artifacts.lenient import (
    LenientRunResults,
    LenientRunResultsResult,
)
from dbt_mcp.dbt_admin.run_artifacts.schemas.output import OutputResultSchema

logger = logging.getLogger(__name__)

RunResultsParsed = (
    RunResultsV1
    | RunResultsV2
    | RunResultsV3
    | RunResultsV4
    | RunResultsV5
    | RunResultsV6
    | LenientRunResults
)

# Individual result entry type — union of all versioned result types plus the lenient fallback.
# v5 and v6 add relation_name and compiled_code; v1-v4 do not.
RunResultEntry = (
    RunResultOutputV1
    | RunResultOutputV2
    | RunResultOutputV3
    | RunResultOutputV4
    | RunResultOutputV5
    | ResultV6
    | LenientRunResultsResult
)


def parse(raw: dict[str, Any]) -> RunResultsParsed:
    """Parse run_results.json using dbt-artifacts-parser (version-aware).

    Falls back to ``LenientRunResults`` when strict Pydantic validation fails.
    This covers dbt runs that emit a ``'reused'`` status
    (incremental build optimization) or other fields not yet in the published
    schema.
    """
    try:
        return parse_run_results(run_results=raw)
    except Exception as e:
        logger.warning(
            "Strict run_results parsing failed (%s: %s); "
            "falling back to lenient dict-based parsing.",
            type(e).__name__,
            str(e)[:200],
        )
        return LenientRunResults.model_validate(raw)


def get_target(run_results: RunResultsParsed) -> str | None:
    """Extract the dbt target from parsed run_results args."""
    if isinstance(run_results, LenientRunResults):
        return run_results.args.target if run_results.args else None
    # Strict versions: args is dict[str, Any] | None
    return run_results.args.get("target") if run_results.args else None


def to_error_result(result: RunResultEntry) -> OutputResultSchema | None:
    """Map a run result to an error output, or None if the result is not an error/fail."""
    if isinstance(result, LenientRunResultsResult):
        status_val = result.status
        if status_val not in [
            RunResultsStatus.ERROR.value,
            RunResultsStatus.FAIL.value,
        ]:
            return None
        return OutputResultSchema(
            unique_id=result.unique_id,
            relation_name=result.relation_name or "No database relation",
            message=result.message or "",
            compiled_code=result.compiled_code or result.compiled_sql,
        )
    # Strict models: status is always an enum
    status_val = result.status.value
    if status_val not in [RunResultsStatus.ERROR.value, RunResultsStatus.FAIL.value]:
        return None
    relation_name: str | None = None
    compiled_code: str | None = None
    if isinstance(result, (RunResultOutputV5, ResultV6)):
        relation_name = result.relation_name
        compiled_code = result.compiled_code
    return OutputResultSchema(
        unique_id=result.unique_id,
        relation_name=relation_name or "No database relation",
        message=str(result.message) if result.message is not None else "",
        compiled_code=compiled_code,
    )


def to_warning_result(result: RunResultEntry) -> OutputResultSchema | None:
    """Map a run result to a warning output, or None if the result is not a warning."""
    if isinstance(result, LenientRunResultsResult):
        status_val = result.status
        if status_val != RunResultsStatus.WARN.value:
            return None
        return OutputResultSchema(
            unique_id=result.unique_id,
            relation_name=result.relation_name or "No database relation",
            message=result.message or "Warning detected",
            status="warn",
            compiled_code=result.compiled_code or result.compiled_sql,
        )
    # Strict models: status is always an enum
    status_val = result.status.value
    if status_val != RunResultsStatus.WARN.value:
        return None
    relation_name: str | None = None
    compiled_code: str | None = None
    if isinstance(result, (RunResultOutputV5, ResultV6)):
        relation_name = result.relation_name
        compiled_code = result.compiled_code
    return OutputResultSchema(
        unique_id=result.unique_id,
        relation_name=relation_name or "No database relation",
        message=str(result.message)
        if result.message is not None
        else "Warning detected",
        status="warn",
        compiled_code=compiled_code,
    )
