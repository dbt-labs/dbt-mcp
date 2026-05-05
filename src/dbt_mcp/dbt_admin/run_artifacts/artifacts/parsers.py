"""ArtifactType enum and ARTIFACT_PARSERS dispatch table."""

from __future__ import annotations

from collections.abc import Callable
from enum import Enum
from typing import Any

from dbt_mcp.dbt_admin.run_artifacts.artifacts import (
    catalog,
    manifest,
    run_results,
    sources,
)


class ArtifactType(str, Enum):
    RUN_RESULTS = "run_results.json"
    SOURCES = "sources.json"
    MANIFEST = "manifest.json"
    CATALOG = "catalog.json"


# Used by the DuckDB materialization layer added in Part 2 of this series.
ARTIFACT_PARSERS: dict[ArtifactType, Callable[[dict[str, Any]], Any]] = {
    ArtifactType.RUN_RESULTS: run_results.parse,
    ArtifactType.SOURCES: sources.parse,
    ArtifactType.MANIFEST: manifest.parse,
    ArtifactType.CATALOG: catalog.parse,
}
