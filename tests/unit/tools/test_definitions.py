"""Unit tests for tool definition infrastructure."""

from enum import Enum
from inspect import signature
from typing import Annotated, Any

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import Field

from dbt_mcp.tools.targets import Permission, ProjectTarget
from dbt_mcp.tools.injection import AdaptError, ContextInput

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
        requirements=(),
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


async def test_explicit_selector_is_consumed_by_context_building():
    @generic_dbt_mcp_tool(
        description="test",
        title="test",
        name_enum=FakeToolName,
    )
    async def my_tool(
        context: str,
        query: str,
        project_id: Annotated[
            int, ProjectTarget(requires=Permission.METADATA_READ), ContextInput()
        ] = Field(description="Project ID."),
    ) -> str:
        return f"{context}:{query}"

    def build_context(project_id: int) -> str:
        return f"project {project_id}"

    adapted = my_tool.adapt_with_mappers(context=build_context)
    assert await adapted.fn(project_id=42, query="orders") == "project 42:orders"
    schema = adapted.to_fastmcp_internal_tool().parameters
    assert schema["properties"]["project_id"]["type"] == "integer"
    assert "project_id" in schema["required"]
    assert my_tool.targets["project_id"].requires == Permission.METADATA_READ
    assert "project_id" in signature(my_tool.fn).parameters
    assert "project_id" in adapted.input_signature.parameters

    def build_query(term: str) -> str:
        return term.upper()

    adapted_again = adapted.adapt_with_mappers(query=build_query)
    assert await adapted_again.fn(project_id=42, term="orders") == "project 42:ORDERS"
    assert adapted_again.targets == my_tool.targets

    def wrong_context(project_id: str) -> str:
        return project_id

    with pytest.raises(AdaptError, match="cannot accept declared input"):
        my_tool.adapt_with_mappers(context=wrong_context)


async def test_configured_context_consumes_no_selector_and_injects_no_placeholder():
    @generic_dbt_mcp_tool(
        description="test",
        title="test",
        name_enum=FakeToolName,
    )
    async def my_tool(
        context: str,
        project_id: Annotated[
            int, ProjectTarget(requires=Permission.METADATA_READ), ContextInput()
        ] = Field(description="Project ID."),
    ) -> str:
        return context

    def build_context() -> str:
        return "configured environment"

    adapted = my_tool.adapt_with_mappers(context=build_context)
    assert await adapted.fn() == "configured environment"
    assert "project_id" not in signature(adapted.fn).parameters
    assert adapted.targets == my_tool.targets


async def test_selector_declaration_stays_required_and_non_nullable():
    @generic_dbt_mcp_tool(
        description="test",
        title="test",
        name_enum=FakeToolName,
    )
    async def my_tool(
        context: str,
        project_id: Annotated[
            int, ProjectTarget(requires=Permission.METADATA_READ), ContextInput()
        ] = Field(description="Project ID."),
    ) -> str:
        return context

    def build_context(project_id: int | None = None) -> str:
        return f"project {project_id}" if project_id else "configured environment"

    adapted = my_tool.adapt_with_mappers(context=build_context)
    internal = adapted.to_fastmcp_internal_tool()
    assert internal.parameters["properties"]["project_id"]["type"] == "integer"
    assert "project_id" in internal.parameters["required"]
    assert await internal.run({"project_id": 42}) == "project 42"
    with pytest.raises(ToolError, match="project_id"):
        await internal.run({"project_id": None})
    assert await adapted.fn() == "configured environment"
