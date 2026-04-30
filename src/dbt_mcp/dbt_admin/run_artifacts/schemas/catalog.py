"""Pydantic schemas for catalog.json artifacts."""

from typing import Any

from pydantic import ConfigDict, Field

from dbt_mcp.dbt_admin.run_artifacts.schemas.common import (
    ArtifactBaseModel,
    ArtifactMetadataSchema,
)


class CatalogMetadataSchema(ArtifactBaseModel):
    """Metadata block within a catalog entry (per-table, not top-level)."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    type: str = ""
    database: str = ""
    schema_: str = Field(default="", alias="schema")
    name: str = ""
    owner: str = ""
    comment: str | None = None


class CatalogColumnSchema(ArtifactBaseModel):
    """A column entry in catalog.json."""

    name: str = ""
    type: str = ""
    index: int | None = None
    comment: str | None = None


class CatalogStatSchema(ArtifactBaseModel):
    """A stat entry in catalog.json."""

    id: str = ""
    label: str = ""
    value: Any = ""
    include: bool = True
    description: str = ""


class CatalogNodeSchema(ArtifactBaseModel):
    """A node entry in catalog.json (within nodes or sources dict)."""

    unique_id: str = ""
    metadata: CatalogMetadataSchema = CatalogMetadataSchema()
    columns: dict[str, CatalogColumnSchema] = {}
    stats: dict[str, CatalogStatSchema] = {}


class CatalogArtifactSchema(ArtifactBaseModel):
    """Top-level schema wrapping the entire catalog.json."""

    metadata: ArtifactMetadataSchema = ArtifactMetadataSchema()
    nodes: dict[str, CatalogNodeSchema] = {}
    sources: dict[str, CatalogNodeSchema] = {}
    errors: list[Any] | None = None
