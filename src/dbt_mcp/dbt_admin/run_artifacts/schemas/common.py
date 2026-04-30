"""Shared sub-schemas used across multiple artifact types."""

from typing import Any

from pydantic import BaseModel, ConfigDict


class ArtifactBaseModel(BaseModel):
    """Base model with ``extra="allow"`` so unknown API fields pass through."""

    model_config = ConfigDict(extra="allow")


class ArtifactMetadataSchema(ArtifactBaseModel):
    """Top-level metadata block present in all artifact files."""

    dbt_schema_version: str = ""
    dbt_version: str = ""
    generated_at: str = ""
    invocation_id: str = ""
    env: dict[str, str] = {}
    project_name: str | None = None
    project_id: str | None = None
    adapter_type: str | None = None


class OwnerSchema(ArtifactBaseModel):
    """Owner info for exposures and groups.

    ``email`` is ``str`` on exposures but can be ``list[str]`` on groups.
    """

    name: str = ""
    email: str | list[str] = ""


class DependsOnSchema(ArtifactBaseModel):
    """Dependency tracking for nodes, exposures, metrics."""

    nodes: list[str] = []
    macros: list[str] = []


class ChecksumSchema(ArtifactBaseModel):
    """File checksum for nodes."""

    name: str = ""
    checksum: str = ""


class ContractSchema(ArtifactBaseModel):
    """Model contract configuration."""

    enforced: bool = False
    alias_types: bool = True


class DocsSchema(ArtifactBaseModel):
    """Docs configuration on a node."""

    show: bool = True


class ColumnConstraintSchema(ArtifactBaseModel):
    """Column-level or model-level constraint."""

    type: str = ""
    expression: str | None = None
    name: str | None = None
    columns: list[str] = []
    warn_unenforced: bool | None = True
    warn_unsupported: bool | None = True


class NodeConfigSchema(ArtifactBaseModel):
    """The ``config`` block on a manifest node."""

    enabled: bool = True
    materialized: str = ""
    incremental_strategy: str | None = None
    on_schema_change: str | None = None
    unique_key: str | list[str] | None = None
    full_refresh: bool | None = None
    severity: str = ""
    warn_if: str = ""
    error_if: str = ""
    fail_calc: str = ""
    store_failures: bool | None = None
    store_failures_as: str | None = None


class TestMetadataInnerSchema(ArtifactBaseModel):
    """The ``test_metadata`` block on test nodes."""

    name: str = ""
    namespace: str = ""
    kwargs: dict[str, Any] = {}
