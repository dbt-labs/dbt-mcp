"""Unit tests for code mode tools registration and list_tools override."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp.types import TextContent

from dbt_mcp.code_mode.tools import (
    CODE_MODE_TOOL_NAMES,
    _dispatch_search_query,
    register_code_mode_tools,
)
from dbt_mcp.code_mode.spec import ToolSpec
from dbt_mcp.config.config import load_config
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.lsp.lsp_binary_manager import LspBinaryInfo
from dbt_mcp.mcp.server import create_dbt_mcp


def test_code_mode_tool_names() -> None:
    assert CODE_MODE_TOOL_NAMES == {"codemode_search", "codemode_execute"}


def test_register_code_mode_tools_adds_two_tools() -> None:
    mock_mcp = MagicMock()
    register_code_mode_tools(mock_mcp)
    assert mock_mcp.add_tool.call_count == 2
    names = {call.kwargs["name"] for call in mock_mcp.add_tool.call_args_list}
    assert names == CODE_MODE_TOOL_NAMES


class TestDispatchSearchQuery:
    @pytest.fixture()
    def spec(self) -> ToolSpec:
        tools = {
            "get_all_models": SimpleNamespace(
                name="get_all_models",
                description="Retrieves name and description of all models.",
                parameters={
                    "properties": {
                        "environment_id": {"type": "integer"},
                    },
                    "required": ["environment_id"],
                },
            ),
            "build": SimpleNamespace(
                name="build",
                description="Executes models, tests, snapshots, and seeds.",
                parameters={"properties": {"select": {"type": "string"}}},
            ),
        }
        s = ToolSpec()
        s.build_from_internal_tools(tools)
        return s

    def test_categories_query(self, spec: ToolSpec) -> None:
        result = _dispatch_search_query(spec, "categories")
        assert isinstance(result, list)
        cat_names = {c["category"] for c in result}
        assert "discovery" in cat_names

    def test_tools_query(self, spec: ToolSpec) -> None:
        result = _dispatch_search_query(spec, "tools")
        names = {t["name"] for t in result}
        assert "get_all_models" in names
        assert "build" in names

    def test_tools_category_query(self, spec: ToolSpec) -> None:
        result = _dispatch_search_query(spec, "tools:dbt_cli")
        names = {t["name"] for t in result}
        assert names == {"build"}

    def test_detail_query(self, spec: ToolSpec) -> None:
        result = _dispatch_search_query(spec, "detail:get_all_models")
        assert result["name"] == "get_all_models"
        assert any(p["name"] == "environment_id" for p in result["params"])

    def test_detail_unknown_tool(self, spec: ToolSpec) -> None:
        result = _dispatch_search_query(spec, "detail:nonexistent")
        assert "error" in result

    def test_guide_query(self, spec: ToolSpec) -> None:
        result = _dispatch_search_query(spec, "guide:get_all_models")
        assert isinstance(result, str)

    def test_guide_unknown_tool(self, spec: ToolSpec) -> None:
        result = _dispatch_search_query(spec, "guide:nonexistent")
        assert "error" in result

    def test_unknown_query_format(self, spec: ToolSpec) -> None:
        result = _dispatch_search_query(spec, "foobar")
        assert "error" in result


@pytest.mark.asyncio
async def test_codemode_search_returns_categories() -> None:
    mock_mcp = MagicMock()
    mock_mcp._tool_manager = SimpleNamespace(
        _tools={
            "get_all_models": SimpleNamespace(
                name="get_all_models",
                description="Retrieves name and description of all models.",
                parameters={"properties": {"environment_id": {"type": "integer"}}, "required": ["environment_id"]},
            ),
        }
    )
    register_code_mode_tools(mock_mcp)
    handlers = {c.kwargs["name"]: c.args[0] for c in mock_mcp.add_tool.call_args_list}

    result = await handlers["codemode_search"](query="categories")
    payload = json.loads(result[0].text)
    assert isinstance(payload, list)
    assert any(c["category"] == "discovery" for c in payload)


@pytest.mark.asyncio
async def test_codemode_search_returns_tool_detail() -> None:
    mock_mcp = MagicMock()
    mock_mcp._tool_manager = SimpleNamespace(
        _tools={
            "get_all_models": SimpleNamespace(
                name="get_all_models",
                description="Retrieves name and description of all models.",
                parameters={"properties": {"environment_id": {"type": "integer"}}, "required": ["environment_id"]},
            ),
        }
    )
    register_code_mode_tools(mock_mcp)
    handlers = {c.kwargs["name"]: c.args[0] for c in mock_mcp.add_tool.call_args_list}

    result = await handlers["codemode_search"](query="detail:get_all_models")
    payload = json.loads(result[0].text)
    assert payload["name"] == "get_all_models"
    assert any(p["name"] == "environment_id" for p in payload["params"])


@pytest.mark.asyncio
async def test_codemode_execute_normalizes_json_text_tool_output() -> None:
    mock_mcp = MagicMock()
    mock_mcp.call_tool = AsyncMock(
        return_value=[TextContent(type="text", text='{"models":["m1","m2"]}')]
    )
    mock_mcp._tool_manager = SimpleNamespace(
        _tools={
            "list_models": SimpleNamespace(
                name="list_models",
                description="List models",
                inputSchema={"properties": {}},
            )
        }
    )
    register_code_mode_tools(mock_mcp)
    handlers = {c.kwargs["name"]: c.args[0] for c in mock_mcp.add_tool.call_args_list}

    result = await handlers["codemode_execute"](
        code='response = await dbt.list_models()\nreturn response["models"][0]'
    )
    assert result[0].text == "m1"


@pytest.mark.asyncio
async def test_codemode_execute_blocks_recursive_codemode_calls() -> None:
    mock_mcp = MagicMock()
    mock_mcp.call_tool = AsyncMock(return_value=[TextContent(type="text", text="[]")])
    mock_mcp._tool_manager = SimpleNamespace(_tools={})
    register_code_mode_tools(mock_mcp)
    handlers = {c.kwargs["name"]: c.args[0] for c in mock_mcp.add_tool.call_args_list}

    result = await handlers["codemode_execute"](
        code='return await dbt.codemode_search(query="categories")'
    )
    payload = json.loads(result[0].text)
    assert "cannot call other code mode tools" in payload["error"]


@pytest.mark.asyncio
async def test_list_tools_returns_only_code_mode_tools_when_enabled(env_setup) -> None:
    with (
        env_setup(
            env_vars={
                "DBT_MCP_ENABLE_CODE_MODE": "true",
                "DISABLE_DBT_CODEGEN": "false",
                "DISABLE_MCP_SERVER_METADATA": "false",
            }
        ),
        patch(
            "dbt_mcp.config.config.detect_binary_type",
            return_value=BinaryType.DBT_CORE,
        ),
        patch(
            "dbt_mcp.config.config.dbt_lsp_binary_info",
            return_value=LspBinaryInfo(path="/path/to/lsp", version="1.0.0"),
        ),
        patch("dbt_mcp.mcp.server.register_proxied_tools", AsyncMock()),
    ):
        config = load_config(enable_proxied_tools=False)
        assert config.enable_code_mode is True
        dbt_mcp = await create_dbt_mcp(config)
        tools_result = await dbt_mcp.list_tools()
        tools = getattr(tools_result, "tools", tools_result)
        tool_names = {getattr(t, "name", t) for t in tools}
        assert tool_names == CODE_MODE_TOOL_NAMES
