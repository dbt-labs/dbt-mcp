"""Intermediate schemas for dbt Platform Admin API job run responses.

These represent the raw shape of get_job_run_details() API responses and are
used internally by ErrorFetcher and WarningFetcher in parser.py to navigate
run steps before fetching and parsing artifact files.

For the final tool output contract, see schemas/output.py.
"""

from pydantic import BaseModel, ConfigDict


class RunStepSchema(BaseModel):
    """Schema for individual "run_step" key from get_job_run_details()."""

    model_config = ConfigDict(extra="allow")

    name: str
    status: int  # 20 = error
    index: int
    finished_at: str | None = None
    logs: str | None = None


class RunDetailsSchema(BaseModel):
    """Schema for get_job_run_details() response."""

    model_config = ConfigDict(extra="allow")

    is_cancelled: bool
    run_steps: list[RunStepSchema]
    finished_at: str | None = None
