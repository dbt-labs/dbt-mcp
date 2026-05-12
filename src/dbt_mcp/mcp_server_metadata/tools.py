"""MCP Server tools for server information and management."""

import logging
import subprocess
from importlib.metadata import version
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)

_THIS_DIR = Path(__file__).resolve().parent


def _get_server_version() -> str:
    """Get the dbt-mcp server version from package metadata."""
    try:
        return version("dbt-mcp")
    except Exception:
        return "unknown"


def _get_git_branch() -> str:
    """git rev-parse walks up from cwd to find the enclosing .git directory."""
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=_THIS_DIR,
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
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


@dbt_mcp_tool(
    description="Get the current git branch of the running dbt MCP server. Useful when running a local development build to identify which branch is active.",
    title="Get MCP Server Branch",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
    open_world_hint=False,
)
def get_mcp_server_branch() -> str:
    """Returns the current git branch of the dbt MCP server."""
    return _get_git_branch()


MCP_SERVER_TOOLS = [
    get_mcp_server_version,
    get_mcp_server_branch,
]


def register_mcp_server_tools(
    dbt_mcp: FastMCP,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
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
