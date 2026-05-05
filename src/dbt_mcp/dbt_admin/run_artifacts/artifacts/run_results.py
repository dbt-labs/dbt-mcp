"""Parsing and mapping for run_results.json artifacts."""

from __future__ import annotations

import logging
from typing import Any

from dbt_artifacts_parser.parser import parse_run_results  # type: ignore[import-untyped]

from dbt_mcp.dbt_admin.constants import RunResultsStatus
from dbt_mcp.dbt_admin.run_artifacts.artifacts.lenient import LenientRunResults
from dbt_mcp.dbt_admin.run_artifacts.schemas.output import OutputResultSchema

logger = logging.getLogger(__name__)


def parse(raw: dict[str, Any]) -> Any:
    """Parse run_results.json using dbt-artifacts-parser (version-aware).

    Falls back to lenient ``_AttrDict``-based parsing when strict Pydantic
    validation fails.  This covers dbt runs that emit a ``'reused'`` status
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


def get_target(run_results: Any) -> str | None:
    """Extract the dbt target from parsed run_results args."""
    args = getattr(run_results, "args", None)
    return getattr(args, "target", None)


def to_error_result(result: Any) -> OutputResultSchema | None:
    """Map a run result to an error output, or None if the result is not an error/fail."""
    status_val = getattr(result.status, "value", result.status)
    if status_val not in [RunResultsStatus.ERROR.value, RunResultsStatus.FAIL.value]:
        return None
    return OutputResultSchema(
        unique_id=getattr(result, "unique_id", None),
        relation_name=getattr(result, "relation_name", None) or "No database relation",
        message=getattr(result, "message", None) or "",
        compiled_code=getattr(result, "compiled_code", None)
        or getattr(result, "compiled_sql", None),
    )


def to_warning_result(result: Any) -> OutputResultSchema | None:
    """Map a run result to a warning output, or None if the result is not a warning."""
    status_val = getattr(result.status, "value", result.status)
    if status_val != RunResultsStatus.WARN.value:
        return None
    return OutputResultSchema(
        unique_id=getattr(result, "unique_id", None),
        relation_name=getattr(result, "relation_name", None) or "No database relation",
        message=getattr(result, "message", None) or "Warning detected",
        status="warn",
        compiled_code=getattr(result, "compiled_code", None)
        or getattr(result, "compiled_sql", None),
    )
