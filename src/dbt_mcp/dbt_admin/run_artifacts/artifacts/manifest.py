"""Parsing and mapping for manifest.json artifacts."""

from __future__ import annotations

import logging
from typing import Any

from dbt_artifacts_parser.parser import parse_manifest
from dbt_artifacts_parser.parsers.manifest.manifest_v1 import ManifestV1
from dbt_artifacts_parser.parsers.manifest.manifest_v2 import ManifestV2
from dbt_artifacts_parser.parsers.manifest.manifest_v3 import ManifestV3
from dbt_artifacts_parser.parsers.manifest.manifest_v4 import ManifestV4
from dbt_artifacts_parser.parsers.manifest.manifest_v5 import ManifestV5
from dbt_artifacts_parser.parsers.manifest.manifest_v6 import ManifestV6
from dbt_artifacts_parser.parsers.manifest.manifest_v7 import ManifestV7
from dbt_artifacts_parser.parsers.manifest.manifest_v8 import ManifestV8
from dbt_artifacts_parser.parsers.manifest.manifest_v9 import ManifestV9
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import ManifestV10
from dbt_artifacts_parser.parsers.manifest.manifest_v11 import ManifestV11
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import ManifestV12

from dbt_mcp.dbt_admin.run_artifacts.artifacts.lenient import LenientManifest

logger = logging.getLogger(__name__)

ManifestParsed = (
    ManifestV1
    | ManifestV2
    | ManifestV3
    | ManifestV4
    | ManifestV5
    | ManifestV6
    | ManifestV7
    | ManifestV8
    | ManifestV9
    | ManifestV10
    | ManifestV11
    | ManifestV12
    | LenientManifest
)


def parse(raw: dict[str, Any]) -> ManifestParsed:
    """Parse manifest.json using dbt-artifacts-parser (version-aware).

    Falls back to ``LenientManifest`` when strict Pydantic validation fails.
    This covers preview / unreleased dbt versions that
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
