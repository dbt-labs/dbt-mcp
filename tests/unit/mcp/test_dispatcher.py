"""Unit tests for DbtMCP tool dispatcher routing."""

import logging
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent, Tool

from dbt_mcp.errors.common import MissingHostError
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
    from dbt_mcp.config.elicitation import ElicitingCredentialsProvider

    if settings is None:
        settings = DbtMcpSettings.model_construct()

    inner_credentials_provider = MagicMock(spec=CredentialsProvider)
    inner_credentials_provider.get_credentials = AsyncMock(
        return_value=(settings, StaticTokenProvider(token="test-token"))
    )

    credentials_provider = MagicMock(spec=ElicitingCredentialsProvider)
    credentials_provider.inner_provider = inner_credentials_provider
    credentials_provider.get_credentials = AsyncMock(
        return_value=(settings, StaticTokenProvider(token="test-token"))
    )

    config = MagicMock(spec=Config)
    config.credentials_provider = credentials_provider

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

    async def test_returns_false_when_credentials_raise(self):
        dispatcher = _make_dispatcher()
        dispatcher.config.credentials_provider.inner_provider.get_credentials = (
            AsyncMock(
                side_effect=MissingHostError(
                    "DBT_HOST is a required environment variable"
                )
            )
        )
        assert await dispatcher._is_multi_project() is False

    async def test_raises_non_host_value_errors(self):
        dispatcher = _make_dispatcher()
        dispatcher.config.credentials_provider.inner_provider.get_credentials = (
            AsyncMock(
                side_effect=ValueError("No decoded access token found in OAuth context")
            )
        )
        with pytest.raises(ValueError, match="No decoded access token"):
            await dispatcher._is_multi_project()


class TestListToolsRouting:
    @pytest.mark.parametrize(
        "is_multi,expected_tool,called,not_called",
        [
            pytest.param(True, "multi_tool", "multi", "single", id="multi_project"),
            pytest.param(False, "single_tool", "single", "multi", id="single_project"),
        ],
    )
    async def test_routes_based_on_project_mode(
        self, is_multi, expected_tool, called, not_called
    ):
        multi = MagicMock(spec=FastMCP)
        multi.list_tools = AsyncMock(return_value=[_make_tool("multi_tool")])
        single = MagicMock(spec=FastMCP)
        single.list_tools = AsyncMock(return_value=[_make_tool("single_tool")])

        mcps = {"multi": multi, "single": single}
        dispatcher = _make_dispatcher(
            multi_project_mcp=multi, single_project_mcp=single
        )
        with patch.object(
            dispatcher, "_is_multi_project", AsyncMock(return_value=is_multi)
        ):
            tools = await dispatcher.list_tools()

        assert [t.name for t in tools] == [expected_tool]
        mcps[called].list_tools.assert_awaited_once()
        mcps[not_called].list_tools.assert_not_awaited()

    async def test_list_tools_bypasses_eliciting_provider(self):
        """list_tools() must use inner_provider only — never block on elicitation.

        Regression test for: v1.17.x shows 'No tools' in Cursor for dbt Core
        users because ElicitingCredentialsProvider.get_credentials() was called
        during ListToolsRequest, causing a timeout via the elicitation prompt.
        """
        single = MagicMock(spec=FastMCP)
        single.list_tools = AsyncMock(return_value=[_make_tool("single_tool")])
        dispatcher = _make_dispatcher(single_project_mcp=single)

        # Inner provider raises MissingHostError → is_multi = False → single-project
        dispatcher.config.credentials_provider.inner_provider.get_credentials = (
            AsyncMock(
                side_effect=MissingHostError(
                    "DBT_HOST is a required environment variable"
                )
            )
        )

        tools = await dispatcher.list_tools()

        assert [t.name for t in tools] == ["single_tool"]
        # The eliciting provider must never be called from list_tools
        dispatcher.config.credentials_provider.get_credentials.assert_not_awaited()
        dispatcher.config.credentials_provider.inner_provider.get_credentials.assert_awaited_once()


class TestCallToolRouting:
    @pytest.mark.parametrize(
        "is_multi,expected_text,called,not_called",
        [
            pytest.param(True, "multi result", "multi", "single", id="multi_project"),
            pytest.param(
                False, "single result", "single", "multi", id="single_project"
            ),
        ],
    )
    async def test_routes_based_on_project_mode(
        self, is_multi, expected_text, called, not_called
    ):
        multi = MagicMock(spec=FastMCP)
        multi.call_tool = AsyncMock(
            return_value=[TextContent(type="text", text="multi result")]
        )
        single = MagicMock(spec=FastMCP)
        single.call_tool = AsyncMock(
            return_value=[TextContent(type="text", text="single result")]
        )

        mcps = {"multi": multi, "single": single}
        dispatcher = _make_dispatcher(
            multi_project_mcp=multi, single_project_mcp=single
        )
        with patch.object(
            dispatcher, "_is_multi_project", AsyncMock(return_value=is_multi)
        ):
            result = await dispatcher.call_tool("some_tool", {"arg": "val"})

        assert result == [TextContent(type="text", text=expected_text)]
        mcps[called].call_tool.assert_awaited_once()
        mcps[not_called].call_tool.assert_not_awaited()

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

    async def test_tracking_failure_does_not_crash_call_tool(self):
        single = MagicMock(spec=FastMCP)
        single.call_tool = AsyncMock(
            side_effect=MissingHostError("DBT_HOST is a required environment variable")
        )

        dispatcher = _make_dispatcher(single_project_mcp=single)
        dispatcher.usage_tracker.emit_tool_called_event = AsyncMock(
            side_effect=MissingHostError("tracking credentials missing")
        )
        with patch.object(
            dispatcher, "_is_multi_project", AsyncMock(return_value=False)
        ):
            result = await dispatcher.call_tool("some_tool", {})

        # Tool error is returned cleanly despite tracking also failing
        assert len(result) == 1
        assert isinstance(result[0], TextContent)
        assert "DBT_HOST" in result[0].text
        # Tracking was attempted (and failed, but didn't crash)
        dispatcher.usage_tracker.emit_tool_called_event.assert_awaited_once()


class TestAppLifespanLogging:
    async def test_lifespan_logs_exception_with_traceback(self, caplog):
        """Regression: app_lifespan used logger.error() without exc_info=True, so an
        AssertionError with no message logged as 'Error in MCP server:' with no traceback —
        making the crash completely undiagnosable. Should use logger.exception() instead."""
        from dbt_mcp.mcp.server import app_lifespan

        server = _make_dispatcher()
        server.config.proxied_tool_config_provider = MagicMock()
        server.config.lsp_config = None
        server.config.disable_tools = []
        server.config.enable_tools = None
        server.config.enabled_toolsets = set()
        server.config.disabled_toolsets = set()
        server._is_multi_project = AsyncMock(return_value=False)

        with patch(
            "dbt_mcp.mcp.server.register_proxied_tools", side_effect=AssertionError()
        ):
            with patch(
                "dbt_mcp.mcp.server.ProxiedToolsManager.close", new_callable=AsyncMock
            ):
                with patch("dbt_mcp.mcp.server.shutdown"):
                    with caplog.at_level(logging.ERROR, logger="dbt_mcp.mcp.server"):
                        with pytest.raises(AssertionError):
                            async with app_lifespan(server):
                                pass

        error_records = [
            r
            for r in caplog.records
            if r.levelno == logging.ERROR and "Error in MCP server" in r.message
        ]
        assert error_records, "Expected an ERROR log for 'Error in MCP server'"
        assert any(r.exc_info is not None for r in error_records), (
            "Expected exc_info to be set so the full traceback appears in the log, "
            "not just the (empty) exception message"
        )


class TestArgLogging:
    async def test_sensitive_args_not_logged(self, caplog):
        single = MagicMock(spec=FastMCP)
        single.call_tool = AsyncMock(return_value=[TextContent(type="text", text="ok")])
        dispatcher = _make_dispatcher(single_project_mcp=single)

        with (
            patch.object(
                dispatcher, "_is_multi_project", AsyncMock(return_value=False)
            ),
            caplog.at_level(logging.INFO, logger="dbt_mcp.mcp.server"),
        ):
            await dispatcher.call_tool(
                "show",
                {"sql_query": "SELECT id FROM my_model", "limit": 5},
            )

        assert "SELECT id FROM my_model" not in caplog.text
        assert "***" in caplog.text
        assert "limit" in caplog.text
