"""Parsing and mapping for catalog.json artifacts."""

from __future__ import annotations

import logging
from typing import Any

from dbt_artifacts_parser.parser import parse_catalog  # type: ignore[import-untyped]

from dbt_mcp.dbt_admin.run_artifacts.artifacts.lenient import LenientCatalog

logger = logging.getLogger(__name__)


def parse(raw: dict[str, Any]) -> Any:
    """Parse catalog.json using dbt-artifacts-parser (version-aware).

    Falls back to lenient ``_AttrDict``-based parsing when strict Pydantic
    validation fails.
    """
    try:
        return parse_catalog(catalog=raw)
    except Exception as e:
        logger.warning(
            "Strict catalog parsing failed (%s: %s); "
            "falling back to lenient dict-based parsing.",
            type(e).__name__,
            str(e)[:200],
        )
        return LenientCatalog.model_validate(raw)
