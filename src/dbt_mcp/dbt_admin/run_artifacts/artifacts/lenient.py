"""Lenient Pydantic schemas used as fallbacks when dbt-artifacts-parser fails.

dbt-artifacts-parser uses Pydantic internally with strict enum validation.
It fails on real-world artifacts that deviate from the published schema —
e.g. a ``"reused"`` status from incremental builds, or preview dbt versions
that emit extra fields.

These schemas are maximally permissive:
- ``extra="allow"`` — unknown fields don't cause failures
- All non-essential fields are optional with safe defaults
- ``status`` is ``str | None`` (not an enum) — accepts any value dbt may emit

The ``parse()`` functions in each artifact module always return a Pydantic
``BaseModel`` — either the strict dbt-artifacts-parser model (happy path) or
one of these lenient models (fallback). Downstream extractors receive a typed
object in both cases.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class LenientRunResultsResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str | None = None
    unique_id: str | None = None
    relation_name: str | None = None
    message: str | None = None
    compiled_code: str | None = None
    compiled_sql: str | None = None  # older dbt versions used compiled_sql


class LenientRunResultsArgs(BaseModel):
    model_config = ConfigDict(extra="allow")

    target: str | None = None


class LenientRunResults(BaseModel):
    model_config = ConfigDict(extra="allow")

    results: list[LenientRunResultsResult] = Field(default_factory=list)
    args: LenientRunResultsArgs | None = None

    @field_validator("results", mode="before")
    @classmethod
    def coerce_results(cls, v: Any) -> list[Any]:
        return v if isinstance(v, list) else []


class LenientSourceResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str | None = None
    unique_id: str | None = None
    max_loaded_at_time_ago_in_s: float | None = None


class LenientSources(BaseModel):
    model_config = ConfigDict(extra="allow")

    results: list[LenientSourceResult] = Field(default_factory=list)

    @field_validator("results", mode="before")
    @classmethod
    def coerce_results(cls, v: Any) -> list[Any]:
        return v if isinstance(v, list) else []


class LenientCatalog(BaseModel):
    """Minimal lenient catalog schema — nodes/sources dicts for PR 2/3 extraction."""

    model_config = ConfigDict(extra="allow")

    nodes: dict[str, Any] = Field(default_factory=dict)
    sources: dict[str, Any] = Field(default_factory=dict)

    @field_validator("nodes", "sources", mode="before")
    @classmethod
    def coerce_dict(cls, v: Any) -> dict[str, Any]:
        return v if isinstance(v, dict) else {}


class LenientManifest(BaseModel):
    """Minimal lenient manifest schema — nodes/sources dicts for PR 2/3 extraction."""

    model_config = ConfigDict(extra="allow")

    nodes: dict[str, Any] = Field(default_factory=dict)
    sources: dict[str, Any] = Field(default_factory=dict)

    @field_validator("nodes", "sources", mode="before")
    @classmethod
    def coerce_dict(cls, v: Any) -> dict[str, Any]:
        return v if isinstance(v, dict) else {}
