"""Parsing and mapping for catalog.json artifacts."""

from __future__ import annotations

import logging
from typing import Any

from dbt_artifacts_parser.parser import parse_catalog
from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogV1

from dbt_mcp.dbt_admin.run_artifacts.artifacts.lenient import LenientCatalog

logger = logging.getLogger(__name__)

CatalogParsed = CatalogV1 | LenientCatalog


def parse(raw: dict[str, Any]) -> CatalogParsed:
    """Parse catalog.json using dbt-artifacts-parser (version-aware).

    Falls back to ``LenientCatalog`` when strict Pydantic validation fails.
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
