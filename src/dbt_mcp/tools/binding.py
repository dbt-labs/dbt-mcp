from collections.abc import Mapping
from typing import Any

from mcp.types import Tool


def merge_bound_arguments(
    arguments: Mapping[str, Any], bindings: Mapping[str, Any]
) -> dict[str, Any]:
    merged = dict(arguments)
    for name, value in bindings.items():
        if name in merged and merged[name] is not None and merged[name] != value:
            raise ValueError(f"{name} conflicts with its bound value")
        merged[name] = value
    return merged


def bind_schema(tool: Tool, bindings: Mapping[str, Any]) -> Tool:
    schema = dict(tool.inputSchema)
    schema["properties"] = {
        name: value
        for name, value in schema.get("properties", {}).items()
        if name not in bindings
    }
    if "required" in schema:
        schema["required"] = [
            name for name in schema["required"] if name not in bindings
        ]
    return tool.model_copy(update={"inputSchema": schema})
