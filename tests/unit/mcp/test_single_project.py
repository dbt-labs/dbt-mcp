"""Tests for single-project mode: strip at registration, inject in call_tool."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from dbt_mcp.mcp.server import DbtMCP


def _make_dbt_mcp(*, static_project_id: int | None = None) -> DbtMCP:
    """Create a DbtMCP instance with minimal mocks for testing."""
    mock_config = Mock()
    mock_config.disable_tools = []
    mock_config.enable_tools = None
    mock_tracker = Mock()
    mock_tracker.emit_tool_called_event = AsyncMock()
    return DbtMCP(
        config=mock_config,
        usage_tracker=mock_tracker,
        lifespan=None,
        static_project_id=static_project_id,
        name="test",
    )


def _register_tool(server: DbtMCP, name: str, has_project_id: bool = True) -> None:
    """Register a dummy tool directly into the tool manager."""
    properties: dict = {"query": {"type": "string"}}
    required = ["query"]
    if has_project_id:
        properties["project_id"] = {"type": "integer"}
        required.append("project_id")

    from mcp.server.fastmcp.tools.base import Tool as InternalTool
    from mcp.server.fastmcp.utilities.func_metadata import ArgModelBase, FuncMetadata
    from pydantic import create_model

    async def dummy_fn(**kwargs):
        return kwargs

    server._tool_manager._tools[name] = InternalTool(
        fn=dummy_fn,
        name=name,
        title=name,
        description="test tool",
        parameters={"type": "object", "properties": properties, "required": required},
        fn_metadata=FuncMetadata(
            arg_model=create_model(f"{name}Args", __base__=ArgModelBase),
        ),
        is_async=True,
        context_kwarg=None,
    )


def test_strip_project_id_from_tools():
    server = _make_dbt_mcp()
    _register_tool(server, "get_models", has_project_id=True)
    _register_tool(server, "search_docs", has_project_id=False)

    server.strip_project_id_from_tools()

    models_params = server._tool_manager._tools["get_models"].parameters
    assert "project_id" not in models_params["properties"]
    assert "project_id" not in models_params.get("required", [])
    assert "query" in models_params["properties"]

    docs_params = server._tool_manager._tools["search_docs"].parameters
    assert "query" in docs_params["properties"]


def test_strip_removes_empty_required():
    server = _make_dbt_mcp()

    from mcp.server.fastmcp.tools.base import Tool as InternalTool
    from mcp.server.fastmcp.utilities.func_metadata import ArgModelBase, FuncMetadata
    from pydantic import create_model

    async def dummy_fn(**kwargs):
        return kwargs

    server._tool_manager._tools["only_pid"] = InternalTool(
        fn=dummy_fn,
        name="only_pid",
        title="only_pid",
        description="test",
        parameters={
            "type": "object",
            "properties": {"project_id": {"type": "integer"}},
            "required": ["project_id"],
        },
        fn_metadata=FuncMetadata(
            arg_model=create_model("OnlyPidArgs", __base__=ArgModelBase),
        ),
        is_async=True,
        context_kwarg=None,
    )

    server.strip_project_id_from_tools()

    params = server._tool_manager._tools["only_pid"].parameters
    assert params["properties"] == {}
    assert "required" not in params


async def test_call_tool_injects_project_id():
    server = _make_dbt_mcp(static_project_id=42)
    _register_tool(server, "get_models", has_project_id=True)

    # Mock super().call_tool to capture what arguments are passed
    with patch.object(
        type(server).__bases__[0], "call_tool", new_callable=AsyncMock
    ) as mock_super_call:
        mock_super_call.return_value = [{"type": "text", "text": "ok"}]
        await server.call_tool("get_models", {"query": "select 1"})

        mock_super_call.assert_called_once_with(
            "get_models", {"query": "select 1", "project_id": 42}
        )


async def test_call_tool_no_injection_when_no_static_project():
    server = _make_dbt_mcp(static_project_id=None)
    _register_tool(server, "get_models", has_project_id=True)

    with patch.object(
        type(server).__bases__[0], "call_tool", new_callable=AsyncMock
    ) as mock_super_call:
        mock_super_call.return_value = [{"type": "text", "text": "ok"}]
        await server.call_tool("get_models", {"query": "select 1"})

        mock_super_call.assert_called_once_with(
            "get_models", {"query": "select 1"}
        )
