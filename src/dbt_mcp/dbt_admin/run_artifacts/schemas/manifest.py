"""Pydantic schemas for manifest.json artifacts."""

from typing import Any

from pydantic import ConfigDict, Field

from dbt_mcp.dbt_admin.run_artifacts.schemas.common import (
    ArtifactBaseModel,
    ArtifactMetadataSchema,
    ChecksumSchema,
    ColumnConstraintSchema,
    ContractSchema,
    DependsOnSchema,
    DocsSchema,
    NodeConfigSchema,
    OwnerSchema,
    TestMetadataInnerSchema,
)


# ── Column schema ──────────────────────────────────────────────────────


class ManifestColumnSchema(ArtifactBaseModel):
    """A column definition within a manifest node."""

    name: str = ""
    data_type: str = ""
    type: str = ""  # legacy fallback for data_type
    description: str = ""
    constraints: list[ColumnConstraintSchema] = []
    meta: dict[str, Any] = {}
    tags: list[str] = []
    tests: list[Any] = []
    config: dict[str, Any] = {}


# ── Node schema ────────────────────────────────────────────────────────


class ManifestNodeSchema(ArtifactBaseModel):
    """A node or source entry in manifest.json.

    Covers: model, test, seed, snapshot, analysis, source.
    Uses ``Field(alias="schema")`` for the reserved ``schema`` key.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    unique_id: str = ""
    name: str = ""
    resource_type: str = ""
    package_name: str = ""
    path: str = ""
    file_path: str = ""
    original_file_path: str = ""
    fqn: list[str] = []
    alias: str = ""
    checksum: ChecksumSchema | str = ChecksumSchema()
    description: str = ""
    language: str = ""
    raw_code: str = ""
    raw_sql: str = ""  # legacy
    database: str = ""
    schema_: str = Field(default="", alias="schema")
    relation_name: str | None = None
    identifier: str = ""
    enabled: bool = True
    materialized: str = ""
    config: NodeConfigSchema | dict[str, Any] = NodeConfigSchema()
    access: str = ""
    group: str = ""
    contract: ContractSchema | dict[str, Any] = ContractSchema()
    version: str | int | None = None
    latest_version: str | int | None = None
    deprecation_date: str | None = None
    constraints: list[ColumnConstraintSchema] = []
    tags: list[str] = []
    meta: dict[str, Any] = {}
    columns: dict[str, ManifestColumnSchema] = {}
    depends_on: DependsOnSchema | dict[str, Any] = DependsOnSchema()
    docs: DocsSchema | dict[str, Any] = DocsSchema()
    quoting: dict[str, Any] | None = None
    compiled_code: str = ""
    compiled_sql: str = ""  # legacy
    compiled_path: str = ""
    extra_ctes: list[dict[str, Any]] = []
    patch_path: str | None = None
    # Source-specific fields
    source_name: str = ""
    source_description: str = ""
    loader: str = ""
    loaded_at_field: str = ""
    freshness: dict[str, Any] | None = None
    # Test-specific fields
    test_metadata: TestMetadataInnerSchema | None = None


# ── Exposure schema ────────────────────────────────────────────────────


class ManifestExposureSchema(ArtifactBaseModel):
    """An exposure entry in manifest.json."""

    unique_id: str = ""
    name: str = ""
    type: str = ""
    label: str = ""
    owner: OwnerSchema = OwnerSchema()
    url: str = ""
    maturity: str | None = None
    description: str = ""
    package_name: str = ""
    path: str = ""
    original_file_path: str = ""
    fqn: list[str] = []
    depends_on: DependsOnSchema = DependsOnSchema()
    tags: list[str] = []
    meta: dict[str, Any] = {}
    config: dict[str, Any] = {}


# ── Metric schema ──────────────────────────────────────────────────────


class ManifestMetricSchema(ArtifactBaseModel):
    """A metric entry in manifest.json."""

    unique_id: str = ""
    name: str = ""
    label: str = ""
    type: str = ""
    calculation_method: str = ""  # legacy
    description: str = ""
    package_name: str = ""
    path: str = ""
    original_file_path: str = ""
    fqn: list[str] = []
    type_params: dict[str, Any] = {}
    time_granularity: str | None = None
    depends_on: DependsOnSchema = DependsOnSchema()
    group: str | None = None
    tags: list[str] = []
    meta: dict[str, Any] = {}
    config: dict[str, Any] = {}


# ── Group schema ───────────────────────────────────────────────────────


class ManifestGroupSchema(ArtifactBaseModel):
    """A group entry in manifest.json."""

    unique_id: str = ""
    name: str = ""
    description: str = ""
    package_name: str = ""
    path: str = ""
    original_file_path: str = ""
    owner: OwnerSchema = OwnerSchema()


# ── Macro schema ───────────────────────────────────────────────────────


class ManifestMacroSchema(ArtifactBaseModel):
    """A macro entry in manifest.json."""

    unique_id: str = ""
    name: str = ""
    package_name: str = ""
    path: str = ""
    original_file_path: str = ""
    macro_sql: str = ""
    description: str = ""
    depends_on: DependsOnSchema | dict[str, Any] = DependsOnSchema()
    arguments: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}


# ── Top-level manifest ─────────────────────────────────────────────────


class ManifestArtifactSchema(ArtifactBaseModel):
    """Top-level schema wrapping the entire manifest.json."""

    metadata: ArtifactMetadataSchema = ArtifactMetadataSchema()
    nodes: dict[str, ManifestNodeSchema] = {}
    sources: dict[str, ManifestNodeSchema] = {}
    macros: dict[str, ManifestMacroSchema] = {}
    exposures: dict[str, ManifestExposureSchema] = {}
    metrics: dict[str, ManifestMetricSchema] = {}
    groups: dict[str, ManifestGroupSchema] = {}
    child_map: dict[str, list[str]] = {}
    parent_map: dict[str, list[str]] = {}
