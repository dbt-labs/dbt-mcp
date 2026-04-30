"""Output data schemas used by the run artifact parser."""

from pydantic import BaseModel


class OutputResultSchema(BaseModel):
    """Schema for individual error/warning result."""

    unique_id: str | None = None
    relation_name: str | None = None
    message: str
    status: str | None = None  # "error", "fail", "warn" - helps distinguish result type
    compiled_code: str | None = None
    truncated_logs: str | None = None


class OutputStepSchema(BaseModel):
    """Schema for a single step with its results (errors/warnings)."""

    target: str | None = None
    step_name: str | None = None
    finished_at: str | None = None
    results: list[OutputResultSchema]
