from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SessionPhase(str, Enum):
    DECIDING = "deciding"
    REVIEWING = "reviewing"
    APPLYING = "applying"
    APPLY_FAILED = "apply_failed"
    AWAITING_VALIDATION = "awaiting_validation"
    VALIDATING = "validating"
    COMPLETE = "complete"
    VALIDATION_FAILED = "validation_failed"
    APPLIED_UNVALIDATED = "applied_unvalidated"


# Phases where server-side state should be fetched alongside local session phase
_PHASES_WITH_SERVER_STATE = {
    SessionPhase.APPLYING,
    SessionPhase.APPLY_FAILED,
    SessionPhase.AWAITING_VALIDATION,
    SessionPhase.VALIDATING,
    SessionPhase.COMPLETE,
    SessionPhase.VALIDATION_FAILED,
    SessionPhase.APPLIED_UNVALIDATED,
}


@dataclass
class OnboardingSession:
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    account_id: int = 0
    phase: SessionPhase = SessionPhase.DECIDING
    decisions: dict[str, Any] = field(default_factory=dict)

    def has_server_state(self) -> bool:
        return self.phase in _PHASES_WITH_SERVER_STATE


class InMemorySessionStore:
    """In-memory session store keyed by account_id.

    Per-process — survives the lifetime of the MCP server process but not restarts.
    Sufficient for stdio transport. Remote MCP persistence is a future concern.
    """

    def __init__(self) -> None:
        self._sessions: dict[int, OnboardingSession] = {}

    def get_or_create(self, account_id: int) -> OnboardingSession:
        if account_id not in self._sessions:
            self._sessions[account_id] = OnboardingSession(account_id=account_id)
        return self._sessions[account_id]

    def get(self, account_id: int) -> OnboardingSession | None:
        return self._sessions.get(account_id)

    def update(self, session: OnboardingSession) -> None:
        self._sessions[session.account_id] = session
