from types import SimpleNamespace
from unittest.mock import AsyncMock

from dbt_mcp.config.config_providers import ProxiedToolConfig
from dbt_mcp.config.headers import ProxiedToolHeadersProvider
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.proxy.tools import (
    get_proxied_tools,
    resolve_proxied_tools_configuration,
)
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import proxied_tools


def make_config(
    *,
    are_sql_tools_disabled: bool = False,
    are_discovery_tools_disabled: bool = False,
) -> ProxiedToolConfig:
    return ProxiedToolConfig(
        user_id=1,
        dev_environment_id=2,
        prod_environment_id=3,
        url="https://example.com",
        headers_provider=ProxiedToolHeadersProvider(
            token_provider=StaticTokenProvider(token="test-token")
        ),
        are_sql_tools_disabled=are_sql_tools_disabled,
        are_discovery_tools_disabled=are_discovery_tools_disabled,
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


def test_resolve_proxied_tools_configuration_includes_all_proxied_tools_by_default():
    config = make_config()

    result = resolve_proxied_tools_configuration(config, [])

    assert result == proxied_tools


def test_resolve_proxied_tools_configuration_respects_exclude_tools():
    config = make_config()

    result = resolve_proxied_tools_configuration(
        config,
        exclude_tools=[ToolName.TEXT_TO_SQL, ToolName.SEARCH],
    )

    assert result == {
        ToolName.EXECUTE_SQL,
        ToolName.GET_RELATED_MODELS,
    }


def test_resolve_proxied_tools_configuration_filters_disabled_sql_tools():
    config = make_config(are_sql_tools_disabled=True)

    result = resolve_proxied_tools_configuration(config, [])

    assert ToolName.TEXT_TO_SQL not in result
    assert ToolName.EXECUTE_SQL not in result
    assert ToolName.GET_RELATED_MODELS in result
    assert ToolName.SEARCH in result


def test_resolve_proxied_tools_configuration_filters_disabled_discovery_tools():
    config = make_config(are_discovery_tools_disabled=True)

    result = resolve_proxied_tools_configuration(config, [])

    assert ToolName.GET_RELATED_MODELS not in result
    assert ToolName.SEARCH not in result
    assert ToolName.TEXT_TO_SQL in result
    assert ToolName.EXECUTE_SQL in result
