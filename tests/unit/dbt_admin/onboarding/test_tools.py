from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from dbt_mcp.config.config_providers import AdminApiConfig
from dbt_mcp.config.config_providers.base import StaticConfigProvider
from dbt_mcp.dbt_admin.onboarding.tools import (
    ONBOARDING_TOOLS,
    OnboardingToolContext,
    dbt_admin_account_create,
    dbt_admin_onboarding_apply,
    dbt_admin_onboarding_get,
    dbt_admin_onboarding_validate,
    register_onboarding_tools,
)
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset
from tests.conftest import MockFastMCP

NUM_ONBOARDING_TOOLS = 4

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
def config_provider():
    return StaticConfigProvider(
        AdminApiConfig(
            url="https://example.dbt.com",
            headers_provider=MagicMock(),
            account_id=1,
        )
    )


@pytest.fixture
def onboarding_client():
    client = MagicMock()
    client.get = AsyncMock(return_value=_BACKEND_DATA)
    client.validate = AsyncMock(
        return_value={"status": {"is_success": True, "developer_message": ""}}
    )
    client.apply = AsyncMock(return_value=_BACKEND_DATA)
    return client


@pytest.fixture
def credentials_provider():
    # Minimal stand-in: the tool only touches settings.dbt_account_id and token_provider.
    return SimpleNamespace(
        settings=SimpleNamespace(dbt_account_id=None),
        token_provider=None,
    )


@pytest.fixture
def account_client():
    client = MagicMock()
    client.create = AsyncMock(
        return_value={"account_id": 42, "owner_token": "tok_abc", "created_at": "now"}
    )
    return client


@pytest.fixture
def context(config_provider, onboarding_client, credentials_provider, account_client):
    return OnboardingToolContext(
        admin_api_config_provider=config_provider,
        onboarding_client=onboarding_client,
        credentials_provider=credentials_provider,
        account_client=account_client,
    )


def test_onboarding_tools_count():
    assert len(ONBOARDING_TOOLS) == NUM_ONBOARDING_TOOLS


def test_register_onboarding_tools_registers_all():
    mock_mcp = MockFastMCP()
    register_onboarding_tools(
        mock_mcp,
        StaticConfigProvider(
            AdminApiConfig(
                url="https://example.dbt.com",
                headers_provider=MagicMock(),
                account_id=1,
            )
        ),
        MagicMock(),
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    registered = {kwargs["name"] for kwargs in mock_mcp.tool_kwargs.values()}
    assert ToolName.ACCOUNT_CREATE.value in registered
    assert ToolName.ONBOARDING_GET.value in registered
    assert ToolName.ONBOARDING_VALIDATE.value in registered
    assert ToolName.ONBOARDING_APPLY.value in registered


def test_register_onboarding_tools_respects_disabled_toolset():
    mock_mcp = MockFastMCP()
    register_onboarding_tools(
        mock_mcp,
        StaticConfigProvider(
            AdminApiConfig(
                url="https://example.dbt.com",
                headers_provider=MagicMock(),
                account_id=1,
            )
        ),
        MagicMock(),
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets={Toolset.ADMIN_API},
    )
    assert len(mock_mcp.tools) == 0


async def test_account_create_returns_result(context, account_client):
    result = await dbt_admin_account_create.fn(
        context, name="acme", owner_email="owner@acme.example"
    )

    account_client.create.assert_awaited_once_with(
        name="acme", owner_email="owner@acme.example", created_via=None
    )
    assert result.account_id == 42
    assert result.owner_token == "tok_abc"


async def test_account_create_stashes_identity(context, credentials_provider):
    await dbt_admin_account_create.fn(
        context, name="acme", owner_email="owner@acme.example"
    )

    # The new account id + owner token are stashed for later account-scoped tools.
    assert credentials_provider.settings.dbt_account_id == 42
    assert isinstance(credentials_provider.token_provider, StaticTokenProvider)
    assert credentials_provider.token_provider.get_token() == "tok_abc"


async def test_account_create_passes_created_via(context, account_client):
    await dbt_admin_account_create.fn(
        context,
        name="acme",
        owner_email="owner@acme.example",
        created_via="onboarding_api",
    )

    account_client.create.assert_awaited_once_with(
        name="acme", owner_email="owner@acme.example", created_via="onboarding_api"
    )


async def test_onboarding_get_returns_model(context, onboarding_client):
    result = await dbt_admin_onboarding_get.fn(context)

    onboarding_client.get.assert_awaited_once_with(1)
    assert result.onboarding is not None
    assert result.onboarding.status == "in_progress"


async def test_onboarding_get_returns_none_when_no_record(context, onboarding_client):
    onboarding_client.get = AsyncMock(return_value=None)

    result = await dbt_admin_onboarding_get.fn(context)

    assert result.onboarding is None


async def test_onboarding_validate_returns_valid(context, onboarding_client):
    result = await dbt_admin_onboarding_validate.fn(context, data={})

    onboarding_client.validate.assert_awaited_once_with(1, {})
    assert result.valid is True
    assert result.errors == []


async def test_onboarding_validate_returns_errors(context, onboarding_client):
    onboarding_client.validate = AsyncMock(
        return_value={
            "status": {"is_success": False, "developer_message": "field X is required"}
        }
    )

    result = await dbt_admin_onboarding_validate.fn(context, data={"bad": "data"})

    assert result.valid is False
    assert "field X is required" in result.errors


async def test_onboarding_apply_submits_data(context, onboarding_client):
    result = await dbt_admin_onboarding_apply.fn(context, data={})

    onboarding_client.apply.assert_awaited_once_with(1, {})
    assert result.onboarding.id == 1
    assert result.onboarding.status == "in_progress"
