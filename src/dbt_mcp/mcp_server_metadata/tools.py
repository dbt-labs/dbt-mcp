"""MCP Server tools for server information and management."""

import logging
from importlib.metadata import version

from mcp.server.fastmcp import FastMCP

from dbt_mcp.tools.definitions import ToolDefinition, dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)


def _get_server_version() -> str:
    """Get the dbt-mcp server version from package metadata."""
    try:
        return version("dbt-mcp")
    except Exception:
        return "unknown"


@dbt_mcp_tool(
    description="Get the version of the dbt MCP server. Use this to check what version of the dbt MCP server is running.",
    title="Get MCP Server Version",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
    open_world_hint=False,
)
def get_mcp_server_version() -> str:
    """Returns the current version of the dbt MCP server."""
    return _get_server_version()


MCP_SERVER_TOOLS: list[ToolDefinition] = [
    get_mcp_server_version,
]


def register_mcp_server_tools(
    dbt_mcp: FastMCP,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName],
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    """Register MCP server tools."""
    register_tools(
        dbt_mcp,
        tool_definitions=MCP_SERVER_TOOLS,
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
