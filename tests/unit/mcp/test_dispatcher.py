"""Unit tests for DbtMCP tool dispatcher routing."""

from unittest.mock import AsyncMock, MagicMock, patch

from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent, Tool

from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.mcp.server import DbtMCP
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.tracking.tracking import UsageTracker


def _make_dispatcher(
    *,
    multi_project_mcp: FastMCP | None = None,
    single_project_mcp: FastMCP | None = None,
    settings: DbtMcpSettings | None = None,
) -> DbtMCP:
    """Build a DbtMCP dispatcher with lightweight mock internals."""
    from dbt_mcp.config.config import Config
    from dbt_mcp.config.credentials import CredentialsProvider

    if settings is None:
        settings = DbtMcpSettings.model_construct()

    credentials_provider = MagicMock(spec=CredentialsProvider)
    credentials_provider.get_credentials = AsyncMock(
        return_value=(settings, StaticTokenProvider(token="test-token"))
    )

    config = MagicMock(spec=Config)
    config.credentials_provider = credentials_provider
    config.eliciting_credentials_provider = credentials_provider

    usage_tracker = MagicMock(spec=UsageTracker)
    usage_tracker.emit_tool_called_event = AsyncMock()

    return DbtMCP(
        name="dbt",
        config=config,
        usage_tracker=usage_tracker,
        lifespan=None,
        multi_project_mcp=multi_project_mcp or FastMCP(),
        single_project_mcp=single_project_mcp or FastMCP(),
    )


def _make_tool(name: str) -> Tool:
    return Tool(
        name=name, description="", inputSchema={"type": "object", "properties": {}}
    )


class TestIsMultiProject:
    async def test_returns_true_when_project_ids_set(self):
        settings = DbtMcpSettings.model_construct(dbt_project_ids=[1, 2, 3])
        dispatcher = _make_dispatcher(settings=settings)
        assert await dispatcher._is_multi_project() is True

    async def test_returns_false_when_project_ids_none(self):
        settings = DbtMcpSettings.model_construct(dbt_project_ids=None)
        dispatcher = _make_dispatcher(settings=settings)
        assert await dispatcher._is_multi_project() is False

    async def test_returns_false_when_project_ids_empty(self):
        settings = DbtMcpSettings.model_construct(dbt_project_ids=[])
        dispatcher = _make_dispatcher(settings=settings)
        assert await dispatcher._is_multi_project() is False


class TestListToolsRouting:
    async def test_routes_to_multi_project_when_multi(self):
        multi = MagicMock(spec=FastMCP)
        multi.list_tools = AsyncMock(return_value=[_make_tool("multi_tool")])
        single = MagicMock(spec=FastMCP)
        single.list_tools = AsyncMock(return_value=[_make_tool("single_tool")])

        dispatcher = _make_dispatcher(
            multi_project_mcp=multi, single_project_mcp=single
        )
        with patch.object(
            dispatcher, "_is_multi_project", AsyncMock(return_value=True)
        ):
            tools = await dispatcher.list_tools()

        assert [t.name for t in tools] == ["multi_tool"]
        multi.list_tools.assert_awaited_once()
        single.list_tools.assert_not_awaited()

    async def test_routes_to_single_project_when_not_multi(self):
        multi = MagicMock(spec=FastMCP)
        multi.list_tools = AsyncMock(return_value=[_make_tool("multi_tool")])
        single = MagicMock(spec=FastMCP)
        single.list_tools = AsyncMock(return_value=[_make_tool("single_tool")])

        dispatcher = _make_dispatcher(
            multi_project_mcp=multi, single_project_mcp=single
        )
        with patch.object(
            dispatcher, "_is_multi_project", AsyncMock(return_value=False)
        ):
            tools = await dispatcher.list_tools()

        assert [t.name for t in tools] == ["single_tool"]
        single.list_tools.assert_awaited_once()
        multi.list_tools.assert_not_awaited()


class TestCallToolRouting:
    async def test_routes_to_multi_project_when_multi(self):
        expected = [TextContent(type="text", text="multi result")]
        multi = MagicMock(spec=FastMCP)
        multi.call_tool = AsyncMock(return_value=expected)
        single = MagicMock(spec=FastMCP)
        single.call_tool = AsyncMock(
            return_value=[TextContent(type="text", text="single result")]
        )

        dispatcher = _make_dispatcher(
            multi_project_mcp=multi, single_project_mcp=single
        )
        with patch.object(
            dispatcher, "_is_multi_project", AsyncMock(return_value=True)
        ):
            result = await dispatcher.call_tool("some_tool", {"arg": "val"})

        assert result == expected
        multi.call_tool.assert_awaited_once_with("some_tool", {"arg": "val"})
        single.call_tool.assert_not_awaited()

    async def test_routes_to_single_project_when_not_multi(self):
        expected = [TextContent(type="text", text="single result")]
        multi = MagicMock(spec=FastMCP)
        multi.call_tool = AsyncMock(
            return_value=[TextContent(type="text", text="multi result")]
        )
        single = MagicMock(spec=FastMCP)
        single.call_tool = AsyncMock(return_value=expected)

        dispatcher = _make_dispatcher(
            multi_project_mcp=multi, single_project_mcp=single
        )
        with patch.object(
            dispatcher, "_is_multi_project", AsyncMock(return_value=False)
        ):
            result = await dispatcher.call_tool("some_tool", {})

        assert result == expected
        single.call_tool.assert_awaited_once_with("some_tool", {})
        multi.call_tool.assert_not_awaited()

    async def test_returns_text_content_on_tool_error(self):
        multi = MagicMock(spec=FastMCP)
        single = MagicMock(spec=FastMCP)
        single.call_tool = AsyncMock(side_effect=RuntimeError("something broke"))

        dispatcher = _make_dispatcher(
            multi_project_mcp=multi, single_project_mcp=single
        )
        with patch.object(
            dispatcher, "_is_multi_project", AsyncMock(return_value=False)
        ):
            result = await dispatcher.call_tool("bad_tool", {})

        assert len(result) == 1
        assert isinstance(result[0], TextContent)
        assert "something broke" in result[0].text
