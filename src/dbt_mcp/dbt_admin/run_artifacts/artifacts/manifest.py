"""Parsing and mapping for manifest.json artifacts."""

from __future__ import annotations

import logging
from typing import Any

from dbt_artifacts_parser.parser import parse_manifest  # type: ignore[import-untyped]

from dbt_mcp.dbt_admin.run_artifacts.artifacts.lenient import LenientManifest

logger = logging.getLogger(__name__)


def parse(raw: dict[str, Any]) -> Any:
    """Parse manifest.json using dbt-artifacts-parser (version-aware).

    Falls back to lenient ``_AttrDict``-based parsing when strict Pydantic
    validation fails.  This covers preview / unreleased dbt versions that
    emit a manifest claiming a published schema version (e.g. v12) but
    containing additional fields not yet in that schema.
    """
    try:
        return parse_manifest(manifest=raw)
    except Exception as e:
        logger.warning(
            "Strict manifest parsing failed (%s: %s); "
            "falling back to lenient dict-based parsing.  "
            "This typically occurs with dbt preview builds.",
            type(e).__name__,
            str(e)[:200],
        )
        return LenientManifest.model_validate(raw)
