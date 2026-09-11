from typing import Any

from pydantic import BaseModel, Field


class OnboardingModel(BaseModel):
    """Mirrors the backend onboarding resource returned by GET/POST /onboarding/."""

    id: int
    account_id: int
    status: str
    project_id: int | None = None
    connection_id: int | None = None
    repository_id: int | None = None
    dev_environment_id: int | None = None
    prod_environment_id: int | None = None
    production_job_id: int | None = None
    credential_tested: bool = False
    details: dict[str, Any] | None = None

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "OnboardingModel":
        return cls(
            id=data["id"],
            account_id=data["account_id"],
            status=data.get("status", "in_progress"),
            project_id=data.get("project_id"),
            connection_id=data.get("connection_id"),
            repository_id=data.get("repository_id"),
            dev_environment_id=data.get("dev_environment_id"),
            prod_environment_id=data.get("prod_environment_id"),
            production_job_id=data.get("production_job_id"),
            credential_tested=data.get("credential_tested", False),
            details=data.get("details"),
        )


class OnboardingGetResult(BaseModel):
    onboarding: OnboardingModel | None


class OnboardingValidateResult(BaseModel):
    valid: bool
    errors: list[str] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)


class OnboardingApplyResult(BaseModel):
    onboarding: OnboardingModel
