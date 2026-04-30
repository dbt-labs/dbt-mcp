"""Pydantic schemas for run_results.json artifacts and Admin API run detail responses."""

from typing import Any

from pydantic import BaseModel


class RunStepSchema(BaseModel):
    """Schema for individual "run_step" key from get_job_run_details()."""

    name: str
    status: int  # 20 = error
    index: int
    finished_at: str | None = None
    logs: str | None = None

    class Config:
        extra = "allow"


class RunDetailsSchema(BaseModel):
    """Schema for get_job_run_details() response."""

    is_cancelled: bool
    run_steps: list[RunStepSchema]
    finished_at: str | None = None

    class Config:
        extra = "allow"


class RunResultSchema(BaseModel):
    """Schema for individual result in "results" key of run_results.json."""

    unique_id: str
    status: str  # "success", "error", "fail", "skip", "warn"
    message: str | None = None
    relation_name: str | None = None
    compiled_code: str | None = None
    execution_time: float | None = None
    thread_id: str | None = None
    adapter_response: dict[str, Any] | None = None
    timing: list[dict[str, Any]] | None = None

    class Config:
        extra = "allow"


class RunResultsArgsSchema(BaseModel):
    """Schema for "args" key in run_results.json."""

    target: str | None = None

    class Config:
        extra = "allow"


class RunResultsArtifactSchema(BaseModel):
    """Schema for get_job_run_artifact() response (run_results.json)."""

    results: list[RunResultSchema]
    args: RunResultsArgsSchema | None = None
    metadata: dict[str, Any] | None = None

    class Config:
        extra = "allow"
