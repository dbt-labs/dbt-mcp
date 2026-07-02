"""Session phase enum kept for future use when local state tracking is needed."""

from enum import Enum


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
