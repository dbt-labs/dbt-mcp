import pytest

from dbt_mcp.dbt_admin.onboarding.session import (
    InMemorySessionStore,
    OnboardingSession,
    SessionPhase,
)


async def test_get_or_create_creates_new_session():
    store = InMemorySessionStore()
    session = await store.get_or_create(account_id=42)

    assert session.account_id == 42
    assert session.phase == SessionPhase.DECIDING
    assert len(session.session_id) > 0


async def test_get_or_create_returns_existing_session():
    store = InMemorySessionStore()
    first = await store.get_or_create(account_id=42)
    second = await store.get_or_create(account_id=42)

    assert first.session_id == second.session_id


def test_get_returns_none_when_no_session():
    store = InMemorySessionStore()
    assert store.get(account_id=99) is None


async def test_get_returns_existing_session():
    store = InMemorySessionStore()
    await store.get_or_create(account_id=1)
    session = store.get(account_id=1)

    assert session is not None
    assert session.account_id == 1


async def test_update_persists_phase_change():
    store = InMemorySessionStore()
    session = await store.get_or_create(account_id=1)
    session.phase = SessionPhase.REVIEWING
    store.update(session)

    retrieved = store.get(account_id=1)
    assert retrieved is not None
    assert retrieved.phase == SessionPhase.REVIEWING


async def test_sessions_are_isolated_by_account():
    store = InMemorySessionStore()
    s1 = await store.get_or_create(account_id=1)
    s2 = await store.get_or_create(account_id=2)

    assert s1.session_id != s2.session_id


@pytest.mark.parametrize(
    "phase, expected",
    [
        (SessionPhase.DECIDING, False),
        (SessionPhase.REVIEWING, False),
        (SessionPhase.APPLYING, True),
        (SessionPhase.APPLY_FAILED, True),
        (SessionPhase.AWAITING_VALIDATION, True),
        (SessionPhase.VALIDATING, True),
        (SessionPhase.COMPLETE, True),
        (SessionPhase.VALIDATION_FAILED, True),
        (SessionPhase.APPLIED_UNVALIDATED, True),
    ],
)
def test_has_server_state(phase: SessionPhase, expected: bool):
    session = OnboardingSession(session_id="s", account_id=1, phase=phase)
    assert session.has_server_state() == expected
