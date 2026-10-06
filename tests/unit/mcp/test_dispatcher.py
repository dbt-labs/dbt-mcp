"""The local server binds one registry to current credentials."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from mcp.server.fastmcp import FastMCP
from mcp.types import ClientCapabilities, Implementation, InitializeRequestParams
from dbt_mcp.config.config import Config
from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.errors.common import MissingHostError
from dbt_mcp.mcp.server import DbtMCP, app_lifespan
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.tracking.tracking import UsageTracker


def make_server(projects: list[int] | None = None) -> DbtMCP:
    credentials = MagicMock(spec=CredentialsProvider)
    credentials.get_credentials = AsyncMock(
        return_value=(
            DbtMcpSettings.model_construct(dbt_project_ids=projects),
            StaticTokenProvider(token="test-token"),
        )
    )
    config = MagicMock(spec=Config)
    config.credentials_provider = credentials
    config.proxied_tool_config_provider = None
    config.lsp_config = None
    tracker = MagicMock(spec=UsageTracker)
    tracker.emit_tool_called_event = AsyncMock()
    registry = FastMCP()

    async def get_all_models(project_id: int | None = None) -> str:
        return str(project_id)

    async def show(sql_query: str, limit: int = 5) -> str:
        return "ok"

    registry.add_tool(get_all_models)
    registry.add_tool(show)
    return DbtMCP(
        name="dbt",
        config=config,
        usage_tracker=tracker,
        lifespan=None,
        tool_server=registry,
    )


@pytest.mark.parametrize(
    "projects,expected", [(None, "None"), ([10], "10"), ([10, 20], "20")]
)
async def test_project_selection_binds_schema_and_invocation(projects, expected):
    server = make_server(projects)
    tool = next(t for t in await server.list_tools() if t.name == "get_all_models")
    if projects is not None and len(projects) > 1:
        assert tool.inputSchema["properties"]["project_id"]["type"] == "integer"
        assert tool.inputSchema["properties"]["project_id"]["enum"] == projects
        assert tool.inputSchema["required"] == ["project_id"]
        arguments = {"project_id": 20}
    else:
        assert tool.inputSchema["properties"] == {}
        arguments = {}
    assert (await server.call_tool("get_all_models", arguments))[0][0].text == expected


async def test_credential_refresh_changes_binding_without_replacing_registry():
    server = make_server([10, 20])
    registry = server.tool_server
    assert "project_id" in (await server.list_tools())[0].inputSchema["properties"]
    server.config.credentials_provider.get_credentials.return_value = (
        DbtMcpSettings.model_construct(dbt_project_ids=[20]),
        StaticTokenProvider(token="refreshed"),
    )
    tool = next(t for t in await server.list_tools() if t.name == "get_all_models")
    assert tool.inputSchema["properties"] == {}
    assert (await server.call_tool("get_all_models", {}))[0][0].text == "20"
    assert server.tool_server is registry
    assert "project_id" in (await registry.list_tools())[0].inputSchema["properties"]


@pytest.mark.parametrize(
    "projects,arguments",
    [
        (None, {"project_id": 20}),
        ([20], {"project_id": 20}),
        ([10, 20], {}),
        ([10, 20], {"project_id": None}),
        ([10, 20], {"project_id": 30}),
        ([10, 20], {"project_id": True}),
    ],
)
async def test_calls_must_match_the_advertised_project_selector(projects, arguments):
    with pytest.raises(ValueError, match="project_id"):
        await make_server(projects).call_tool("get_all_models", arguments)


async def test_local_tools_are_available_with_configured_environment():
    server = make_server()
    assert "show" in {t.name for t in await server.list_tools()}
    assert (await server.call_tool("show", {"sql_query": "select 1"}))[0][
        0
    ].text == "ok"


async def test_project_selection_preserves_local_tool_eligibility():
    server = make_server([10, 20])
    assert {t.name for t in await server.list_tools()} == {"get_all_models"}
    with pytest.raises(ValueError, match="unavailable"):
        await server.call_tool("show", {"sql_query": "select 1"})


async def test_listing_before_credentials_are_available():
    server = make_server()
    server.config.credentials_provider.get_credentials.side_effect = MissingHostError(
        "DBT_HOST is required"
    )
    assert "show" in {t.name for t in await server.list_tools()}


async def test_credential_errors_propagate():
    server = make_server()
    server.config.credentials_provider.get_credentials.side_effect = ValueError(
        "No decoded access token"
    )
    with pytest.raises(ValueError, match="No decoded access token"):
        await server.list_tools()


async def test_tracking_failure_preserves_tool_error():
    server = make_server()
    server.tool_server.call_tool = AsyncMock(
        side_effect=RuntimeError("something broke")
    )
    server.usage_tracker.emit_tool_called_event.side_effect = RuntimeError(
        "tracking failed"
    )
    with pytest.raises(RuntimeError, match="something broke"):
        await server.call_tool("get_all_models", {})
    server.usage_tracker.emit_tool_called_event.assert_awaited_once()


async def test_client_info_and_results_are_tracked():
    server = make_server()
    ctx = MagicMock()
    ctx.request_context.session.client_params = InitializeRequestParams(
        protocolVersion="2024-11-05",
        capabilities=ClientCapabilities(),
        clientInfo=Implementation(name="Claude", version="1.2.3"),
    )
    with patch.object(server, "get_context", return_value=ctx):
        await server.call_tool("get_all_models", {})
    event = server.usage_tracker.emit_tool_called_event.call_args.kwargs[
        "tool_called_event"
    ]
    assert (event.mcp_client_name, event.mcp_client_version) == ("Claude", "1.2.3")
    assert event.result[0][0].text == "None"


async def test_sensitive_arguments_are_redacted(caplog):
    with caplog.at_level(logging.INFO, logger="dbt_mcp.mcp.server"):
        await make_server().call_tool(
            "show", {"sql_query": "SELECT secret", "limit": 5}
        )
    assert "SELECT secret" not in caplog.text
    assert "***" in caplog.text
    assert "limit" in caplog.text


async def test_lifespan_logs_exception_with_traceback(caplog):
    server = make_server()
    server.config.proxied_tool_config_provider = MagicMock()
    server.config.disable_tools = []
    server.config.enable_tools = None
    server.config.enabled_toolsets = set()
    server.config.disabled_toolsets = set()
    with (
        patch(
            "dbt_mcp.mcp.server.register_proxied_tools", side_effect=AssertionError()
        ),
        patch("dbt_mcp.mcp.server.ProxiedToolsManager.close", new_callable=AsyncMock),
        patch("dbt_mcp.mcp.server.shutdown"),
        caplog.at_level(logging.ERROR, logger="dbt_mcp.mcp.server"),
    ):
        with pytest.raises(AssertionError):
            async with app_lifespan(server):
                pass
    assert any(
        record.exc_info
        for record in caplog.records
        if "Error in MCP server" in record.message
    )
