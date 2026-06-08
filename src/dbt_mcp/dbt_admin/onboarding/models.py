from typing import Any

from pydantic import BaseModel, Field

from dbt_mcp.dbt_admin.onboarding.decision_points import DecisionPoint


class OnboardingInitResult(BaseModel):
    session_id: str
    phase: str
    account_id: int
    decision_points: list[dict[str, Any]]


class ServerOnboardingState(BaseModel):
    """Mirrors the GET /api/v3/accounts/{id}/onboarding/state/ response.

    Resource FK fields are added incrementally as each phase ships.
    """

    status: str
    data: dict[str, Any] = Field(default_factory=dict)


class OnboardingStateResult(BaseModel):
    phase: str | None
    session_id: str | None
    server_state: ServerOnboardingState | None


def decision_points_to_dicts(points: list[DecisionPoint]) -> list[dict[str, Any]]:
    return [
        {
            "key": p.key,
            "label": p.label,
            "description": p.description,
            "sensitive": p.sensitive,
            "rationale_hint": p.rationale_hint,
            "examples": p.examples,
        }
        for p in points
    ]
