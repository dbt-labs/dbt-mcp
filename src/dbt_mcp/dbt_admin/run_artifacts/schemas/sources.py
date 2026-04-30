"""Pydantic schemas for sources.json artifacts."""

from typing import Any

from pydantic import BaseModel


class SourceFreshnessResultSchema(BaseModel):
    """Schema for source freshness results from sources.json."""

    unique_id: str
    max_loaded_at: str | None = None
    snapshotted_at: str | None = None
    max_loaded_at_time_ago_in_s: float | None = None
    status: str  # "pass", "warn", "fail"
    criteria: dict[str, Any] | None = None
    execution_time: float | None = None
    thread_id: str | None = None
    error: str | None = None
    adapter_response: dict[str, Any] | None = None
    timing: list[dict[str, Any]] | None = None

    class Config:
        extra = "allow"


class SourcesArtifactSchema(BaseModel):
    """Schema for sources.json artifact."""

    results: list[SourceFreshnessResultSchema]
    metadata: dict[str, Any] | None = None
    elapsed_time: float | None = None

    class Config:
        extra = "allow"
