from dataclasses import dataclass
from typing import (
    Any,
    Literal,
    Union,
    get_args,
    get_origin,
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


def _is_optional_type(python_type: Any) -> bool:
    """
    Check if a Python type annotation is already optional (includes None).

    Args:
        python_type: Python type annotation to check

    Returns:
        True if the type is optional (Union with None), False otherwise
    """
    # Handle None type directly
    if python_type is type(None):
        return True

    # Check if it's a Union type
    origin = get_origin(python_type)
    if origin is Union:
        args = get_args(python_type)
        return type(None) in args

    return False


def json_schema_type_to_python_type(schema: dict[str, Any]) -> Any:  # noqa: PLR0911, PLR0912
    """
    Convert a JSON schema type definition to a Python type annotation.

    Handles basic types, arrays, objects, enums, nullable types, anyOf,
    oneOf, and allOf.

    Args:
        schema: JSON schema definition

    Returns:
        Python type annotation suitable for Pydantic
    """
    # Handle empty schema or schemas with no type (treat as Any)
    if not schema:
        return Any

    schema_type = schema.get("type")

    # Handle anyOf (union of types)
    if "anyOf" in schema:
        types = [json_schema_type_to_python_type(s) for s in schema["anyOf"]]
        if not types:
            return Any
        # Combine into Union
        result = types[0]
        for t in types[1:]:
            result = result | t  # type: ignore
        return result

    # Handle oneOf (similar to anyOf for type purposes)
    if "oneOf" in schema:
        types = [json_schema_type_to_python_type(s) for s in schema["oneOf"]]
        if not types:
            return Any
        # Combine into Union
        result = types[0]
        for t in types[1:]:
            result = result | t  # type: ignore
        return result

    # Handle allOf (intersection - use the most specific type)
    # For simplicity, we take the first non-empty schema
    if "allOf" in schema:
        for sub_schema in schema["allOf"]:
            python_type = json_schema_type_to_python_type(sub_schema)
            if python_type is not Any:
                return python_type
        return Any

    # Handle enum types (must come before type checking)
    if "enum" in schema:
        enum_values = schema["enum"]
        if not enum_values:
            return Any
        if len(enum_values) == 1:
            return Literal[enum_values[0]]  # type: ignore
        # Convert to tuple for Literal
        return Literal[tuple(enum_values)]  # type: ignore

    # Handle const (single constant value)
    if "const" in schema:
        return Literal[schema["const"]]  # type: ignore

    # Handle array of types (e.g., ["string", "null"])
    if isinstance(schema_type, list):
        if not schema_type:
            return Any
        # Handle nullable types: ["string", "null"] -> str | None
        types = []
        has_null = False
        for t in schema_type:
            if t == "null":
                has_null = True
            else:
                types.append(json_schema_type_to_python_type({"type": t}))

        if not types:
            return type(None) if has_null else Any

        if len(types) == 1:
            return types[0] | type(None) if has_null else types[0]  # type: ignore

        # Multiple types
        result = types[0]
        for t in types[1:]:
            result = result | t  # type: ignore
        if has_null:
            result = result | type(None)  # type: ignore
        return result

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
        items_schema = schema.get("items")
        if items_schema:
            # Handle tuple-like arrays with different item types
            if isinstance(items_schema, list):
                item_types = [
                    json_schema_type_to_python_type(item) for item in items_schema
                ]
                # For simplicity, use list[Union[...]] for mixed types
                if item_types:
                    union_type = item_types[0]
                    for t in item_types[1:]:
                        union_type = union_type | t  # type: ignore
                    return list[union_type]  # type: ignore
                return list[Any]
            else:
                item_type = json_schema_type_to_python_type(items_schema)
                return list[item_type]  # type: ignore
        return list[Any]

    # Handle object types with properties (nested objects)
    if schema_type == "object":
        # Could potentially create nested Pydantic models here, but dict is safer
        return dict[str, Any]

    # Handle no type specified but has properties (implicit object)
    if "properties" in schema and not schema_type:
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
            # Check if already nullable using proper typing utilities
            if not _is_optional_type(python_type):
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
