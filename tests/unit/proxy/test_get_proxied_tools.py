import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, TextContent, Tool

from dbt_mcp.config.config_providers import ProxiedToolConfig
from dbt_mcp.config.config_providers.proxied_tool import (
    DefaultProxiedToolConfigProvider,
)
from dbt_mcp.config.headers import ProxiedToolHeadersProvider
from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.errors.common import MissingHostError
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.proxy.tools import (
    format_remote_tool_error,
    get_proxied_tools,
    get_remote_tool_fn_metadata,
    register_proxied_tools,
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


def test_get_remote_tool_fn_metadata_builds_arg_model_from_input_schema():
    """Regression for #921: this path previously depended on a private pydantic
    helper (`eval_type_backport`) that was removed in pydantic 2.14."""
    tool = Tool(
        name="execute_sql",
        inputSchema={
            "type": "object",
            "properties": {"sql": {"type": "string"}, "limit": {"type": "integer"}},
        },
    )

    metadata = get_remote_tool_fn_metadata(tool)

    assert metadata.arg_model.__name__ == "execute_sqlArguments"
    assert set(metadata.arg_model.model_fields) == {"sql", "limit"}
    parsed = metadata.arg_model.model_validate({"sql": "select 1"})
    assert parsed.model_dump_one_level() == {"sql": "select 1", "limit": None}


async def test_register_proxied_tool_is_listed_and_callable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote_tool = Tool(
        name="execute_sql",
        description="Execute a SQL query",
        inputSchema={
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    )
    session = AsyncMock()
    session.list_tools.return_value = SimpleNamespace(tools=[remote_tool])
    session.call_tool.return_value = CallToolResult(
        content=[TextContent(type="text", text="query complete")]
    )
    manager = MagicMock()
    manager.return_value.get_remote_mcp_session = AsyncMock(return_value=session)
    monkeypatch.setattr("dbt_mcp.proxy.tools.ProxiedToolsManager", manager)
    config_provider = AsyncMock()
    config_provider.get_config.return_value = make_config()
    server = FastMCP("test")

    await register_proxied_tools(
        dbt_mcp=server,
        config_provider=config_provider,
        disabled_tools=set(),
        enabled_tools={ToolName.EXECUTE_SQL},
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    assert [tool.name for tool in await server.list_tools()] == ["execute_sql"]
    result = await server.call_tool("execute_sql", {"query": "select 1"})
    assert result == [TextContent(type="text", text="query complete")]
    session.call_tool.assert_awaited_once_with("execute_sql", {"query": "select 1"})


def text_blocks(*texts: str) -> list[TextContent]:
    return [TextContent(type="text", text=text) for text in texts]


async def test_format_remote_tool_error_appends_hint_on_warehouse_auth_error():
    hint_provider = MagicMock()
    hint_provider.get_hint = AsyncMock(return_value="HINT: reconnect")
    config = make_config()
    config.warehouse_auth_hint_provider = hint_provider

    message = await format_remote_tool_error(
        "execute_sql",
        text_blocks("SSO authentication has expired, please re-connect to Snowflake"),
        config,
    )

    assert message.startswith("Tool execute_sql reported an error: SSO authentication")
    assert message.endswith("<hint>HINT: reconnect</hint>")
    hint_provider.get_hint.assert_awaited_once_with(
        environment_id=2, developer_credentials=True, user_id=1
    )


async def test_format_remote_tool_error_falls_back_to_prod_environment():
    hint_provider = MagicMock()
    hint_provider.get_hint = AsyncMock(return_value="HINT")
    config = make_config()
    config.dev_environment_id = None
    config.warehouse_auth_hint_provider = hint_provider

    await format_remote_tool_error(
        "execute_sql", text_blocks("authentication has expired"), config
    )

    hint_provider.get_hint.assert_awaited_once_with(
        environment_id=3, developer_credentials=True, user_id=1
    )


async def test_format_remote_tool_error_leaves_other_errors_untouched():
    hint_provider = MagicMock()
    hint_provider.get_hint = AsyncMock(return_value="HINT")
    config = make_config()
    config.warehouse_auth_hint_provider = hint_provider

    message = await format_remote_tool_error(
        "execute_sql", text_blocks("syntax error"), config
    )

    assert message == "Tool execute_sql reported an error: syntax error"
    hint_provider.get_hint.assert_not_called()


async def test_format_remote_tool_error_survives_hint_failure():
    hint_provider = MagicMock()
    hint_provider.get_hint = AsyncMock(side_effect=RuntimeError("boom"))
    config = make_config()
    config.warehouse_auth_hint_provider = hint_provider

    message = await format_remote_tool_error(
        "execute_sql", text_blocks("authentication has expired"), config
    )

    assert message == "Tool execute_sql reported an error: authentication has expired"


async def test_format_remote_tool_error_uses_the_text_of_content_blocks():
    """The message must not contain the Python repr of the content blocks."""
    message = await format_remote_tool_error(
        "execute_sql", text_blocks("first problem", "second problem"), make_config()
    )

    assert (
        message == "Tool execute_sql reported an error: first problem\nsecond problem"
    )
