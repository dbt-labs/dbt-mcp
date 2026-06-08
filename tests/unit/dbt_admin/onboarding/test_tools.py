from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.config.config_providers import AdminApiConfig
from dbt_mcp.config.config_providers.base import StaticConfigProvider
from dbt_mcp.dbt_admin.onboarding.session import InMemorySessionStore, SessionPhase
from dbt_mcp.dbt_admin.onboarding.tools import (
    ONBOARDING_TOOLS,
    OnboardingToolContext,
    dbt_admin_onboarding_init,
    dbt_admin_onboarding_state,
    register_onboarding_tools,
)
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset
from tests.conftest import MockFastMCP

NUM_ONBOARDING_TOOLS = 2


@pytest.fixture
def admin_config():
    return AdminApiConfig(
        url="https://example.dbt.com",
        headers_provider=MagicMock(),
        account_id=1,
    )


@pytest.fixture
def config_provider(admin_config):
    return StaticConfigProvider(admin_config)


@pytest.fixture
def session_store():
    return InMemorySessionStore()


@pytest.fixture
def onboarding_client():
    client = MagicMock()
    client.get_state = AsyncMock(
        return_value={"data": {"status": "in_progress"}}
    )
    return client


@pytest.fixture
def context(config_provider, session_store, onboarding_client):
    return OnboardingToolContext(
        admin_api_config_provider=config_provider,
        session_store=session_store,
        onboarding_client=onboarding_client,
    )


def test_onboarding_tools_count():
    assert len(ONBOARDING_TOOLS) == NUM_ONBOARDING_TOOLS


def test_register_onboarding_tools_registers_all():
    mock_mcp = MockFastMCP()
    mock_config_provider = StaticConfigProvider(
        AdminApiConfig(
            url="https://example.dbt.com",
            headers_provider=MagicMock(),
            account_id=1,
        )
    )
    register_onboarding_tools(
        mock_mcp,
        mock_config_provider,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    registered = {kwargs["name"] for kwargs in mock_mcp.tool_kwargs.values()}
    assert ToolName.ONBOARDING_INIT.value in registered
    assert ToolName.ONBOARDING_STATE.value in registered


def test_register_onboarding_tools_respects_disabled_toolset():
    mock_mcp = MockFastMCP()
    mock_config_provider = StaticConfigProvider(
        AdminApiConfig(
            url="https://example.dbt.com",
            headers_provider=MagicMock(),
            account_id=1,
        )
    )
    register_onboarding_tools(
        mock_mcp,
        mock_config_provider,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets={Toolset.ADMIN_API},
    )
    assert len(mock_mcp.tools) == 0


async def test_onboarding_init_creates_session(context):
    result = await dbt_admin_onboarding_init.fn(context)

    assert result.account_id == 1
    assert result.phase == SessionPhase.DECIDING.value
    assert len(result.session_id) > 0
    assert result.decision_points == []


async def test_onboarding_init_resumes_existing_session(context, session_store):
    first = await dbt_admin_onboarding_init.fn(context)
    second = await dbt_admin_onboarding_init.fn(context)

    assert first.session_id == second.session_id


async def test_onboarding_state_no_session(context):
    result = await dbt_admin_onboarding_state.fn(context)

    assert result.phase is None
    assert result.session_id is None
    assert result.server_state is None


async def test_onboarding_state_with_deciding_session(context, session_store):
    session_store.get_or_create(account_id=1)
    result = await dbt_admin_onboarding_state.fn(context)

    assert result.phase == SessionPhase.DECIDING.value
    assert result.server_state is None  # not in APPLYING phase yet


async def test_onboarding_state_fetches_server_state_when_applying(
    context, session_store, onboarding_client
):
    session = session_store.get_or_create(account_id=1)
    session.phase = SessionPhase.APPLYING
    session_store.update(session)

    result = await dbt_admin_onboarding_state.fn(context)

    onboarding_client.get_state.assert_awaited_once_with(1)
    assert result.server_state is not None
    assert result.server_state.status == "in_progress"
