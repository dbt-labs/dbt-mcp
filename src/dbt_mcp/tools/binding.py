import inspect
from collections.abc import Callable, Mapping
from functools import wraps
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


def bind_arguments(
    fn: Callable[..., Any], bindings: Mapping[str, Any]
) -> Callable[..., Any]:
    original = inspect.signature(fn)
    unknown = bindings.keys() - original.parameters.keys()
    if unknown:
        raise ValueError(f"Cannot bind unknown arguments: {', '.join(sorted(unknown))}")
    exposed = original.replace(
        parameters=[
            p for name, p in original.parameters.items() if name not in bindings
        ]
    )

    def arguments(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
        merged = merge_bound_arguments(kwargs, bindings)
        bound = exposed.bind(
            *args, **{k: v for k, v in merged.items() if k not in bindings}
        )
        bound.apply_defaults()
        return merge_bound_arguments(bound.arguments, bindings)

    if inspect.iscoroutinefunction(fn):

        @wraps(fn)
        async def invoke(*args: Any, **kwargs: Any) -> Any:
            return await fn(**arguments(args, kwargs))
    else:

        @wraps(fn)
        def invoke(*args: Any, **kwargs: Any) -> Any:
            return fn(**arguments(args, kwargs))

    invoke.__signature__ = exposed  # type: ignore[attr-defined]
    invoke.__annotations__ = {
        "return": exposed.return_annotation,
        **{name: p.annotation for name, p in exposed.parameters.items()},
    }
    return invoke
