"""ArtifactType enum and ARTIFACT_PARSERS dispatch table.

ARTIFACT_PARSERS always returns a plain ``dict[str, Any]``:
- Happy path: strict dbt-artifacts-parser Pydantic model → ``.model_dump(mode="json")``
  which normalises enums to strings and aliases (e.g. ``schema_``) to their JSON
  keys (e.g. ``"schema"``).
- Fallback: raw dict passed through as-is — same JSON shape, just unvalidated.

Downstream extractors can therefore use ``.get()`` uniformly on every path.

Note: the ``parse()`` helpers in the sibling artifact modules (manifest.py, catalog.py,
run_results.py, sources.py) are a separate API used by the job error/warning fetcher in
parser.py and are intentionally left unchanged.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from enum import Enum
from typing import Any

from dbt_artifacts_parser.parser import (
    parse_catalog,
    parse_manifest,
    parse_run_results,
    parse_sources,
)

logger = logging.getLogger(__name__)


class ArtifactType(str, Enum):
    RUN_RESULTS = "run_results.json"
    SOURCES = "sources.json"
    MANIFEST = "manifest.json"
    CATALOG = "catalog.json"


def _to_dict(raw: dict[str, Any], strict_parse_fn: Callable[[], Any]) -> dict[str, Any]:
    """Try strict parsing and dump to a plain dict; fall back to raw on any error."""
    try:
        return strict_parse_fn().model_dump(mode="json")
    except Exception as exc:
        logger.warning(
            "Strict artifact parsing failed (%s: %s); falling back to raw dict. "
            "This is expected for dbt Fusion or preview builds.",
            type(exc).__name__,
            str(exc)[:200],
        )
        return raw


ARTIFACT_PARSERS: dict[ArtifactType, Callable[[dict[str, Any]], dict[str, Any]]] = {
    ArtifactType.MANIFEST: lambda raw: _to_dict(
        raw, lambda: parse_manifest(manifest=raw)
    ),
    ArtifactType.CATALOG: lambda raw: _to_dict(raw, lambda: parse_catalog(catalog=raw)),
    ArtifactType.RUN_RESULTS: lambda raw: _to_dict(
        raw, lambda: parse_run_results(run_results=raw)
    ),
    ArtifactType.SOURCES: lambda raw: _to_dict(raw, lambda: parse_sources(sources=raw)),
}
