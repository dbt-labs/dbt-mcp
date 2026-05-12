"""Unit tests for tool definition infrastructure."""

from enum import Enum
from typing import Any

from dbt_mcp.tools.definitions import GenericToolDefinition, generic_dbt_mcp_tool
from dbt_mcp.tools.register import generic_register_tools
from dbt_mcp.tools.toolsets import Toolset


class FakeToolName(Enum):
    MY_TOOL = "my_tool"


def _make_tool(
    meta: dict[str, Any] | None = None,
) -> GenericToolDefinition[FakeToolName]:
    """Helper to create a tool definition with optional meta."""

    @generic_dbt_mcp_tool(
        description="test tool",
        name_enum=FakeToolName,
        title="Test Tool",
        read_only_hint=True,
        meta=meta,
    )
    async def my_tool() -> str:
        return "ok"

    return my_tool


class TestMetaPassthrough:
    """Test that the meta field is preserved through all operations."""

    def test_decorator_sets_meta(self):
        meta = {"ui": {"resourceUri": "ui://test/app.html"}}
        tool = _make_tool(meta=meta)
        assert tool.meta == meta

    def test_decorator_meta_defaults_to_none(self):
        tool = _make_tool()
        assert tool.meta is None

    def test_adapt_context_preserves_meta(self):
        meta = {"ui": {"resourceUri": "ui://test/app.html"}}
        tool = _make_tool(meta=meta)

        def mapper() -> None:
            return None

        adapted = tool.adapt_context(mapper)
        assert adapted.meta == meta

    def test_to_fastmcp_internal_tool_passes_meta(self):
        meta = {"ui": {"resourceUri": "ui://test/app.html"}}
        tool = _make_tool(meta=meta)

        internal = tool.to_fastmcp_internal_tool()
        assert internal.meta == meta

    def test_to_fastmcp_internal_tool_none_meta(self):
        tool = _make_tool()

        internal = tool.to_fastmcp_internal_tool()
        assert internal.meta is None

    def test_register_tools_passes_meta(self, mock_fastmcp):
        mock_mcp, _ = mock_fastmcp
        meta = {"ui": {"resourceUri": "ui://test/app.html"}}
        tool = _make_tool(meta=meta)

        generic_register_tools(
            mock_mcp,
            [tool],
            disabled_tools=set(),
            enabled_tools=None,
            enabled_toolsets=set(),
            disabled_toolsets=set(),
            tool_to_toolset={FakeToolName.MY_TOOL: Toolset.DISCOVERY},
        )

        assert mock_mcp.tool_kwargs["my_tool"]["meta"] == meta

    def test_register_tools_passes_none_meta(self, mock_fastmcp):
        mock_mcp, _ = mock_fastmcp
        tool = _make_tool()

        generic_register_tools(
            mock_mcp,
            [tool],
            disabled_tools=set(),
            enabled_tools=None,
            enabled_toolsets=set(),
            disabled_toolsets=set(),
            tool_to_toolset={FakeToolName.MY_TOOL: Toolset.DISCOVERY},
        )

        assert mock_mcp.tool_kwargs["my_tool"]["meta"] is None
