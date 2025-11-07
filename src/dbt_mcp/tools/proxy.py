from dataclasses import dataclass
from typing import (
    Any,
    Literal,
)

from mcp.server.fastmcp.utilities.func_metadata import (
    ArgModelBase,
    FuncMetadata,
)
from pydantic import create_model
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined


@dataclass
class ProxyTool:
    name: str
    input_schema: dict[str, Any]


def json_schema_type_to_python_type(schema: dict[str, Any]) -> Any:  # noqa: PLR0911, PLR0912
    """
    Convert a JSON schema type definition to a Python type annotation.

    Handles basic types, arrays, objects, enums, and nullable types.
    """
    schema_type = schema.get("type")

    # Handle enum types
    if "enum" in schema:
        enum_values = schema["enum"]
        if len(enum_values) == 1:
            return Literal[enum_values[0]]  # type: ignore
        return Literal[tuple(enum_values)]  # type: ignore

    # Handle array of types (e.g., ["string", "null"])
    if isinstance(schema_type, list):
        # Handle nullable types: ["string", "null"] -> str | None
        types = []
        has_null = False
        for t in schema_type:
            if t == "null":
                has_null = True
            else:
                types.append(json_schema_type_to_python_type({"type": t}))

        if len(types) == 1 and has_null:
            return types[0] | None  # type: ignore
        if len(types) > 1:
            result = types[0]
            for t in types[1:]:
                result = result | t  # type: ignore
            if has_null:
                result = result | None  # type: ignore
            return result
        return Any

    # Handle basic types
    type_mapping = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "null": type(None),
    }

    if schema_type in type_mapping:
        return type_mapping[schema_type]

    # Handle array types
    if schema_type == "array":
        items_schema = schema.get("items", {})
        if items_schema:
            item_type = json_schema_type_to_python_type(items_schema)
            return list[item_type]  # type: ignore
        return list[Any]

    # Handle object types
    if schema_type == "object":
        return dict[str, Any]

    # Default fallback
    return Any


# Based on this: https://github.com/modelcontextprotocol/python-sdk/blob/9ae4df85fbab97bf476ddd160b766ca4c208cd13/src/mcp/server/fastmcp/utilities/func_metadata.py#L105
def get_proxy_tool_fn_metadata(proxy_tool: ProxyTool) -> FuncMetadata:
    """
    Create FuncMetadata for a proxy tool by converting JSON schema to Pydantic.

    Args:
        proxy_tool: The proxy tool with its JSON schema definition

    Returns:
        FuncMetadata with properly typed argument model
    """
    dynamic_pydantic_model_params: dict[str, Any] = {}
    properties = proxy_tool.input_schema.get("properties", {})
    required_fields = set(proxy_tool.input_schema.get("required", []))

    for key, property_schema in properties.items():
        # Convert JSON schema type to Python type annotation
        python_type = json_schema_type_to_python_type(property_schema)

        # If field is not required and not already nullable, make it optional
        if key not in required_fields:
            # Check if already nullable (has None in union)
            origin = getattr(python_type, "__origin__", None)
            if origin is not type(None) and not (
                hasattr(origin, "__args__")
                and type(None) in getattr(python_type, "__args__", [])
            ):
                python_type = python_type | None  # type: ignore
            default = None
        else:
            default = PydanticUndefined

        field_info = FieldInfo.from_annotated_attribute(
            annotation=python_type,
            default=default,
        )
        dynamic_pydantic_model_params[key] = (
            field_info.annotation,
            field_info,
        )
    return FuncMetadata(
        arg_model=create_model(
            f"{proxy_tool.name}Arguments",
            **dynamic_pydantic_model_params,
            __base__=ArgModelBase,
        )
    )
