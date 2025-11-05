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
    status: str  # "success", "error", "fail", "skip"
    message: str | None = None
    relation_name: str | None = None
    compiled_code: str | None = None

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


class ErrorResultSchema(BaseModel):
    """Schema for individual error result."""

    unique_id: str | None = None
    relation_name: str | None = None
    message: str
    compiled_code: str | None = None
    truncated_logs: str | None = None


class ErrorStepSchema(BaseModel):
    """Schema for a single failed step with its errors."""

    target: str | None = None
    step_name: str | None = None
    finished_at: str | None = None
    errors: list[ErrorResultSchema]
