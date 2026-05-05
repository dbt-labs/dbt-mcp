import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from dbt_mcp.config.config_providers import ProxiedToolConfig
from dbt_mcp.config.config_providers.proxied_tool import (
    DefaultProxiedToolConfigProvider,
)
from dbt_mcp.config.headers import ProxiedToolHeadersProvider
from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.errors.common import MissingHostError
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.proxy.tools import get_proxied_tools, register_proxied_tools
from dbt_mcp.tools.tool_names import ToolName


def make_config() -> ProxiedToolConfig:
    return ProxiedToolConfig(
        user_id=1,
        dev_environment_id=2,
        prod_environment_id=3,
        url="https://example.com",
        headers_provider=ProxiedToolHeadersProvider(
            token_provider=StaticTokenProvider(token="test-token")
        ),
    )


async def test_register_proxied_tools_skips_get_config_when_all_proxied_toolsets_disabled():
    """Regression: in 1.17.0, get_config() was always called before the tool filter check,
    causing an AssertionError crash for CLI-only users who have all proxied toolsets disabled."""
    mock_config_provider = AsyncMock()

    await register_proxied_tools(
        dbt_mcp=MagicMock(),
        config_provider=mock_config_provider,
        disabled_tools=set(),
        enabled_tools=set(),  # empty allowlist — no tools enabled regardless of future additions
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    mock_config_provider.get_config.assert_not_called()


async def test_proxied_tool_config_raises_missing_host_error_not_assertion_error():
    """Regression: the bare assert raised AssertionError("") — not a MissingHostError —
    so the except MissingHostError handler in app_lifespan didn't catch it, and the server
    crashed with an empty 'Error in MCP server:' log line."""
    settings = DbtMcpSettings.model_construct()  # actual_host is None
    mock_credentials = AsyncMock()
    mock_credentials.get_credentials.return_value = (
        settings,
        StaticTokenProvider(token=None),
    )

    provider = DefaultProxiedToolConfigProvider(credentials_provider=mock_credentials)

    with pytest.raises(MissingHostError):
        await provider.get_config()


async def test_get_proxied_tools_filters_to_configured_tools():
    proxied_tool = SimpleNamespace(name="execute_sql")
    non_proxied_tool = SimpleNamespace(name="generate_model_yaml")

    session = AsyncMock()
    session.list_tools.return_value = SimpleNamespace(
        tools=[proxied_tool, non_proxied_tool]
    )

    result = await get_proxied_tools(session, {ToolName.EXECUTE_SQL})

    assert result == [proxied_tool]
