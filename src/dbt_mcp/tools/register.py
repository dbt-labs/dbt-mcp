from collections.abc import Sequence

from mcp.server.fastmcp import FastMCP

from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.tool_names import ToolName


def register_tools(
    dbt_mcp: FastMCP,
    tool_definitions: list[ToolDefinition],
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    for tool_definition in tool_definitions:
        if tool_definition.get_name().lower() in [
            tool.value.lower() for tool in exclude_tools
        ]:
            continue
        dbt_mcp.tool(
            name=tool_definition.get_name(),
            title=tool_definition.title,
            description=tool_definition.description,
            annotations=tool_definition.annotations,
            structured_output=tool_definition.structured_output,
        )(tool_definition.fn)
