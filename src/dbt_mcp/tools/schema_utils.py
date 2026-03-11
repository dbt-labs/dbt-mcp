"""Utilities for manipulating MCP tool schemas."""

from mcp.types import Tool as MCPTool


def strip_project_id_param(tool: MCPTool) -> MCPTool:
    """Remove project_id from a tool's input schema.

    Used in single-project mode so the agent never sees project_id as a parameter.
    Returns a new Tool instance; the original is not mutated.
    """
    schema = tool.inputSchema
    properties = schema.get("properties", {})
    if "project_id" not in properties:
        return tool

    new_properties = {k: v for k, v in properties.items() if k != "project_id"}
    new_required = [r for r in schema.get("required", []) if r != "project_id"]

    new_schema = {**schema, "properties": new_properties}
    if new_required:
        new_schema["required"] = new_required
    else:
        new_schema.pop("required", None)

    return tool.model_copy(update={"inputSchema": new_schema})
