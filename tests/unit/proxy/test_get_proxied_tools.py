from types import SimpleNamespace
from unittest.mock import AsyncMock

from dbt_mcp.config.config_providers import ProxiedToolConfig
from dbt_mcp.config.headers import ProxiedToolHeadersProvider
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.proxy.tools import (
    get_proxied_tools,
)
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


async def test_get_proxied_tools_filters_to_configured_tools():
    proxied_tool = SimpleNamespace(name="execute_sql")
    non_proxied_tool = SimpleNamespace(name="generate_model_yaml")

    session = AsyncMock()
    session.list_tools.return_value = SimpleNamespace(
        tools=[proxied_tool, non_proxied_tool]
    )

    result = await get_proxied_tools(session, {ToolName.EXECUTE_SQL})

    assert result == [proxied_tool]
