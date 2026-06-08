from dbt_mcp.dbt_admin.onboarding.session import SessionPhase


def test_session_phase_values():
    assert SessionPhase.DECIDING.value == "deciding"
    assert SessionPhase.COMPLETE.value == "complete"
    assert SessionPhase.VALIDATION_FAILED.value == "validation_failed"
