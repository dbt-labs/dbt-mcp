"""Code mode MCP tools: codemode_search and codemode_execute."""

import json
import logging
from collections.abc import Sequence
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ContentBlock, TextContent

from dbt_mcp.code_mode.executor import execute_code
from dbt_mcp.code_mode.spec import ToolSpec

logger = logging.getLogger(__name__)

CODE_MODE_TOOL_NAMES = frozenset({"codemode_search", "codemode_execute"})

CODEMODE_SEARCH_DESCRIPTION = """Discover dbt MCP tools progressively. Use the `query` parameter to control what is returned.

**Query modes** (pass one of these as the `query` string):

1. `"categories"` — list all tool categories with tool names (cheapest, start here)
2. `"tools"` — list all tools with one-line summaries
3. `"tools:<category>"` — list tools in a specific category (e.g. "tools:discovery")
4. `"detail:<tool_name>"` — get full parameter schema for a tool (e.g. "detail:get_lineage")
5. `"guide:<tool_name>"` — get the rich usage guide with examples (e.g. "guide:get_lineage")

**Recommended workflow:**
1. Start with `"categories"` to see what's available
2. Use `"tools:<category>"` to find the right tool
3. Use `"detail:<tool_name>"` to get exact parameter names and types before calling
4. Optionally use `"guide:<tool_name>"` for complex tools that need usage examples

This progressive approach loads only the information you need, saving tokens.
"""

CODEMODE_EXECUTE_DESCRIPTION = """Execute Python code that calls dbt MCP tools via the `dbt` proxy.

You receive a variable `dbt`. Call tools as async methods with keyword arguments:
  result = await dbt.list_models()
  metrics = await dbt.query_metrics(query={"metrics": [...], "dimensions": [...]})

Chain multiple calls and return a value. Example:
  models = await dbt.get_mart_models()
  return [m["name"] for m in models[:5]]

Use codemode_search first to discover tool names and parameters before executing.
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


def _build_spec(server: FastMCP) -> ToolSpec:
    """Build the ToolSpec from the server's registered tools."""
    spec = ToolSpec()
    tool_manager = getattr(server, "_tool_manager", None)
    if tool_manager is None:
        return spec
    tools = getattr(tool_manager, "_tools", {})
    filtered = {n: t for n, t in tools.items() if n not in CODE_MODE_TOOL_NAMES}
    spec.build_from_internal_tools(filtered)
    return spec


def _dispatch_search_query(spec: ToolSpec, query: str) -> Any:
    """Route a codemode_search query string to the appropriate spec method."""
    q = query.strip()
    if q == "categories":
        return spec.list_categories()
    if q == "tools":
        return spec.list_tools()
    if q.startswith("tools:"):
        category = q[len("tools:") :].strip()
        return spec.list_tools(category=category)
    if q.startswith("detail:"):
        tool_name = q[len("detail:") :].strip()
        detail = spec.get_tool_detail(tool_name)
        if detail is None:
            return {"error": f"Unknown tool: {tool_name}"}
        return detail
    if q.startswith("guide:"):
        tool_name = q[len("guide:") :].strip()
        guide = spec.get_tool_guide(tool_name)
        if guide is None:
            return {"error": f"No guide available for: {tool_name}"}
        return guide
    return {"error": f"Unknown query format: {q!r}. Use categories, tools, tools:<cat>, detail:<name>, or guide:<name>."}


def register_code_mode_tools(dbt_mcp: FastMCP) -> None:
    """Register codemode_search and codemode_execute on the server.

    When code mode is enabled, list_tools is overridden to expose only these two,
    reducing token usage while keeping full tool capability via execute().
    """

    def _make_search_handler(server: FastMCP) -> Any:
        spec: ToolSpec | None = None

        async def codemode_search(query: str) -> list[TextContent]:
            nonlocal spec
            if spec is None:
                spec = _build_spec(server)
            try:
                result = _dispatch_search_query(spec, query)
                return _result_to_content(result)
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
