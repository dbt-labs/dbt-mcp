import logging
from collections.abc import Sequence
from enum import Enum

from mcp.server.fastmcp import FastMCP

from dbt_mcp.tools.definitions import GenericToolDefinition
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import TOOL_TO_TOOLSET, Toolset

logger = logging.getLogger(__name__)


def should_register_tool[NameEnum: Enum](
    tool_name: NameEnum,
    enabled_tools: set[NameEnum],
    disabled_tools: set[NameEnum],
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
    tool_to_toolset: dict[NameEnum, Toolset],
) -> bool:
    """Determine if a tool should be registered based on precedence rules.

    Precedence order (highest to lowest):
    1. Individual tool enable - if tool in enabled_tools, return True
    2. Individual tool disable - if tool in disabled_tools, return False
    3. Toolset enable - if tool's toolset in enabled_toolsets, return True
    4. Toolset disable - if tool's toolset in disabled_toolsets, return False
    5. Default - if any explicit enables are set, return False; otherwise return True

    Args:
        tool_name: The tool name as a ToolName enum
        enabled_tools: Set of explicitly enabled tools (highest precedence)
        disabled_tools: Set of explicitly disabled tools
        enabled_toolsets: Set of enabled toolsets
        disabled_toolsets: Set of disabled toolsets

    Returns:
        True if tool should be registered, False otherwise
    """
    # Precedence 1: Individual tool enable (highest)
    if tool_name in enabled_tools:
        return True

    # Precedence 2: Individual tool disable
    if tool_name in disabled_tools:
        return False

    # Get the tool's toolset
    tool_toolset = tool_to_toolset.get(tool_name)

    # Precedence 3 & 4: Toolset checks
    if tool_toolset:
        if tool_toolset in enabled_toolsets:
            return True
        if tool_toolset in disabled_toolsets:
            return False

    # Precedence 5: Fallback behavior (only when rules 1-4 don't apply)
    # This is the key mechanism that switches between denylist and allowlist modes:
    # - If NO enable configuration exists → return True (original default: enable all tools)
    # - If ANY enable configuration exists → return False (allowlist mode: disable unless explicitly allowed)
    # This preserves backward compatibility while enabling opt-in allowlist functionality
    return not bool(enabled_tools or enabled_toolsets)


def register_tools(
    dbt_mcp: FastMCP,
    tool_definitions: Sequence[GenericToolDefinition[ToolName]],
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName],
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    return generic_register_tools(
        dbt_mcp,
        tool_definitions,
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
        tool_to_toolset=TOOL_TO_TOOLSET,
    )


def generic_register_tools[NameEnum: Enum](
    dbt_mcp: FastMCP,
    tool_definitions: Sequence[GenericToolDefinition[NameEnum]],
    *,
    disabled_tools: set[NameEnum],
    enabled_tools: set[NameEnum],
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
    tool_to_toolset: dict[NameEnum, Toolset],
) -> None:
    """Register tools with the MCP server using precedence-based enablement logic.

    Args:
        dbt_mcp: The FastMCP server instance
        tool_definitions: List of tool definitions to register
        exclude_tools: Tools to exclude (backward compatibility - converted to disabled_tools)
        enabled_tools: Set of explicitly enabled tools (precedence 1)
        enabled_toolsets: Set of enabled toolsets (precedence 3)
        disabled_toolsets: Set of disabled toolsets (precedence 4)
    """
    for tool_definition in tool_definitions:
        tool_name = tool_definition.get_name()
        if not should_register_tool(
            tool_name=tool_name,
            enabled_tools=enabled_tools,
            disabled_tools=disabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
            tool_to_toolset=tool_to_toolset,
        ):
            continue
        dbt_mcp.add_tool(
            fn=tool_definition.fn,
            name=tool_name.value,
            title=tool_definition.title,
            description=tool_definition.description,
            annotations=tool_definition.annotations,
            structured_output=tool_definition.structured_output,
        )
