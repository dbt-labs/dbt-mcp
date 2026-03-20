"""Code mode MCP tools: codemode_search and codemode_execute."""

import json
import logging
from collections.abc import Sequence
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ContentBlock, TextContent

from dbt_mcp.code_mode.catalog import build_catalog_from_tools
from dbt_mcp.code_mode.executor import execute_code, run_search_code

logger = logging.getLogger(__name__)

CODE_MODE_TOOL_NAMES = frozenset({"codemode_search", "codemode_execute"})

CODEMODE_SEARCH_DESCRIPTION = """Search or filter the dbt MCP tool catalog without loading full schemas.

You receive a variable `catalog`: a list of objects with keys:
- name: tool name (e.g. "list_models", "query_metrics")
- description: short description
- param_names: list of parameter names for the tool

Write Python code that uses `catalog` and returns a value (e.g. filter to tools
whose name or description matches a topic). Example:
  return [t for t in catalog if "model" in t["name"] or "model" in t["description"]]
"""

CODEMODE_EXECUTE_DESCRIPTION = """Execute Python code that calls dbt MCP tools via the `dbt` proxy.

You receive a variable `dbt`. Call tools as async methods with keyword arguments:
  result = await dbt.list_models()
  metrics = await dbt.query_metrics(query={"metrics": [...], "dimensions": [...]})

Chain multiple calls and return a value. Example:
  models = await dbt.get_mart_models()
  return [m["name"] for m in models[:5]]
"""


def _result_to_content(value: Any) -> list[TextContent]:
    """Convert a Python value to MCP text content."""
    if isinstance(value, (list, dict, str, int, float, bool)) or value is None:
        text = json.dumps(value) if not isinstance(value, str) else value
    else:
        text = json.dumps(str(value))
    return [TextContent(type="text", text=text)]


def _normalize_content_blocks(content: Sequence[ContentBlock]) -> Any:
    normalized: list[Any] = []
    for block in content:
        if isinstance(block, TextContent):
            text = block.text
            try:
                normalized.append(json.loads(text))
            except json.JSONDecodeError:
                normalized.append(text)
            continue
        normalized.append(str(block))
    if len(normalized) == 1:
        return normalized[0]
    return normalized


def register_code_mode_tools(dbt_mcp: FastMCP) -> None:
    """Register codemode_search and codemode_execute on the server.

    When code mode is enabled, list_tools is overridden to expose only these two,
    reducing token usage while keeping full tool capability via execute().
    """

    def _make_search_handler(server: FastMCP) -> Any:
        async def codemode_search(code: str) -> list[TextContent]:
            tools_dict = getattr(server, "_tool_manager", None)
            if tools_dict is None:
                return _result_to_content({"error": "tool manager not available"})
            tools = getattr(tools_dict, "_tools", {})
            tool_list = [
                t for name, t in tools.items() if name not in CODE_MODE_TOOL_NAMES
            ]
            catalog = build_catalog_from_tools(tool_list)
            try:
                result = run_search_code(code, catalog)
                return _result_to_content(result)
            except ValueError as e:
                return _result_to_content({"error": str(e)})
            except Exception as e:
                logger.exception("Code mode search failed")
                return _result_to_content({"error": str(e)})

        return codemode_search

    def _make_execute_handler(server: FastMCP) -> Any:
        async def codemode_execute(code: str) -> list[TextContent]:
            async def call_tool(name: str, arguments: dict[str, Any]) -> Any:
                if name in CODE_MODE_TOOL_NAMES:
                    raise ValueError("Code mode tools cannot call other code mode tools")
                result = await server.call_tool(name, arguments)
                if isinstance(result, dict):
                    return result
                if isinstance(result, Sequence) and not isinstance(result, str):
                    return _normalize_content_blocks(result)
                return result

            try:
                result = await execute_code(code, call_tool)
                return _result_to_content(result)
            except ValueError as e:
                return _result_to_content({"error": str(e)})
            except Exception as e:
                logger.exception("Code mode execute failed")
                return _result_to_content({"error": str(e)})

        return codemode_execute

    search_fn = _make_search_handler(dbt_mcp)
    execute_fn = _make_execute_handler(dbt_mcp)

    dbt_mcp.add_tool(
        search_fn,
        name="codemode_search",
        description=CODEMODE_SEARCH_DESCRIPTION,
    )
    dbt_mcp.add_tool(
        execute_fn,
        name="codemode_execute",
        description=CODEMODE_EXECUTE_DESCRIPTION,
    )
