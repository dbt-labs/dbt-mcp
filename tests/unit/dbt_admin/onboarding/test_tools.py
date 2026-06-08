from unittest.mock import AsyncMock, MagicMock

import pytest

from dbt_mcp.config.config_providers import AdminApiConfig
from dbt_mcp.config.config_providers.base import StaticConfigProvider
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

_BACKEND_DATA = {
    "id": 1,
    "account_id": 1,
    "status": "in_progress",
    "project_id": None,
    "connection_id": None,
    "repository_id": None,
    "dev_environment_id": None,
    "prod_environment_id": None,
    "production_job_id": None,
    "credential_tested": False,
    "details": None,
}


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
def onboarding_client():
    client = MagicMock()
    client.get = AsyncMock(return_value=_BACKEND_DATA)
    client.create_or_get = AsyncMock(return_value=_BACKEND_DATA)
    return client


@pytest.fixture
def context(config_provider, onboarding_client):
    return OnboardingToolContext(
        admin_api_config_provider=config_provider,
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


async def test_onboarding_init_returns_existing_when_backend_has_one(context, onboarding_client):
    result = await dbt_admin_onboarding_init.fn(context)

    onboarding_client.get.assert_awaited_once_with(1)
    onboarding_client.create_or_get.assert_not_awaited()
    assert result.created is False
    assert result.onboarding.id == 1
    assert result.onboarding.status == "in_progress"


async def test_onboarding_init_creates_when_backend_has_none(context, onboarding_client):
    onboarding_client.get = AsyncMock(return_value=None)

    result = await dbt_admin_onboarding_init.fn(context)

    onboarding_client.get.assert_awaited_once_with(1)
    onboarding_client.create_or_get.assert_awaited_once_with(1)
    assert result.created is True
    assert result.onboarding.id == 1


async def test_onboarding_state_returns_none_when_no_backend_model(context, onboarding_client):
    onboarding_client.get = AsyncMock(return_value=None)

    result = await dbt_admin_onboarding_state.fn(context)

    assert result.onboarding is None


async def test_onboarding_state_returns_model_when_exists(context, onboarding_client):
    result = await dbt_admin_onboarding_state.fn(context)

    onboarding_client.get.assert_awaited_once_with(1)
    assert result.onboarding is not None
    assert result.onboarding.status == "in_progress"
    assert result.onboarding.project_id is None
