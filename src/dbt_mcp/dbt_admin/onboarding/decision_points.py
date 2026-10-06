from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DecisionPoint:
    """A single decision the user must make during onboarding.

    Populated in Phase 0+ tickets as each resource type is added to the flow.
    The LLM proposes values; the user confirms every one.
    """

    key: str
    label: str
    description: str
    sensitive: bool = False
    rationale_hint: str = ""
    examples: list[str] = field(default_factory=list)


def get_decision_points() -> list[DecisionPoint]:
    """Return the current set of decision points for the onboarding flow.

    Empty in the Foundation phase (CC-3674/CC-3675). Decision points are added
    incrementally in Phase 0 (account), Phase 1 (project), Phase 2 (environment),
    and Phase 3 (job) tickets.
    """
    return []
