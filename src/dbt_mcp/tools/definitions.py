from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import Enum
from functools import cached_property, partial, wraps
from inspect import BoundArguments, Parameter, isawaitable, unwrap
from typing import Any

from mcp.server.fastmcp.tools.base import Tool
from mcp.types import ToolAnnotations

from dbt_mcp.tools.binding import InputBinding, configure_argument_validation
from dbt_mcp.tools.injection import (
    AdaptError,
    _accepts,
    _signature,
    _with_signature,
    adapt_with_mappers,
)
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.targets import Target, target_parameters


@dataclass
class GenericToolDefinition[NameEnum: Enum]:
    fn: Callable[..., Any]
    title: str  # Human-friendly title for the tool
    description: str
    name_enum: type[NameEnum]
    name: str | None = None  # Machine-friendly name for the tool
    annotations: ToolAnnotations | None = None
    structured_output: bool = True
    meta: dict[str, Any] | None = None
    requirements: tuple[Target | Enum, ...] | None = None

    def __post_init__(self) -> None:
        # Adapted/bound signatures may hide every target. The canonical function
        # must still declare access explicitly before any adaptation takes place.
        if self.requirements is None and not self.targets:
            raise ValueError("Tools must declare access requirements")

    @property
    def targets(self) -> dict[str, Target]:
        # Access declarations survive context adaptation even when no selector
        # is exposed by the configured mapper.
        return target_parameters(unwrap(self.fn))

    def get_name(self) -> NameEnum:
        return self.name_enum((self.name or self.fn.__name__).lower())

    @cached_property
    def fastmcp_tool(self) -> Tool:
        tool = Tool.from_function(
            fn=self.fn,
            name=self.name,
            title=self.title,
            description=self.description,
            annotations=self.annotations,
            structured_output=self.structured_output,
            meta=self.meta,
        )
        configure_argument_validation(tool)
        return tool

    def bind_inputs(self, binding: InputBinding) -> "GenericToolDefinition[NameEnum]":
        """Return a request-specific interface without changing the original tool."""
        return replace(self, fn=binding.bind_callable(self.fn))

    def adapt_with_mappers(
        self, **parameter_mappers: Callable[..., Any]
    ) -> "GenericToolDefinition[NameEnum]":
        """Inject parameters by name, including context and resolved selectors."""
        return replace(self, fn=adapt_with_mappers(self.fn, **parameter_mappers))

    def with_call_hook(
        self, hook: Callable[[dict[str, Any]], Any], *, inject: str
    ) -> "GenericToolDefinition[NameEnum]":
        """Run a hook on complete call inputs and inject its result internally.

        Bind inputs after installing the hook so it sees selected values too.
        FastMCP validates the public inputs before invoking this callable.
        """
        declaration = _signature(self.fn)
        if inject not in declaration.parameters:
            raise AdaptError(f"Unknown hook destination: {inject}")
        result_type = _signature(hook).return_annotation
        if result_type is Parameter.empty or not _accepts(
            declaration.parameters[inject].annotation, result_type
        ):
            raise AdaptError(
                f"{inject}: hook return type {result_type!r} is incompatible"
            )
        exposed = declaration.replace(
            parameters=[
                p for name, p in declaration.parameters.items() if name != inject
            ]
        )

        @wraps(self.fn)
        async def invoke(*args: Any, **kwargs: Any) -> Any:
            inputs = dict(exposed.bind(*args, **kwargs).arguments)
            prepared = hook(dict(inputs))
            if isawaitable(prepared):
                prepared = await prepared
            arguments = BoundArguments(
                declaration, OrderedDict(inputs | {inject: prepared})
            )
            result = self.fn(*arguments.args, **arguments.kwargs)
            return await result if isawaitable(result) else result

        return replace(self, fn=_with_signature(invoke, exposed))


@dataclass
class ToolDefinition(GenericToolDefinition[ToolName]):
    name_enum: type[ToolName] = ToolName


def generic_dbt_mcp_tool[NameEnum: Enum](
    *,
    description: str,
    title: str,
    name_enum: type[NameEnum],
    name: str | None = None,
    read_only_hint: bool = False,
    destructive_hint: bool = True,
    idempotent_hint: bool = False,
    open_world_hint: bool = True,
    structured_output: bool = True,
    meta: dict[str, Any] | None = None,
    requirements: tuple[Target | Enum, ...] | None = None,
) -> Callable[[Callable], GenericToolDefinition[NameEnum]]:
    """Decorator to define a tool definition for dbt MCP"""

    def decorator(fn: Callable) -> GenericToolDefinition[NameEnum]:
        return GenericToolDefinition(
            fn=fn,
            description=description,
            name_enum=name_enum,
            name=name,
            title=title,
            annotations=ToolAnnotations(
                title=title,
                readOnlyHint=read_only_hint,
                destructiveHint=destructive_hint,
                idempotentHint=idempotent_hint,
                openWorldHint=open_world_hint,
            ),
            structured_output=structured_output,
            meta=meta,
            requirements=requirements,
        )

    return decorator


# Wrapper with ToolName pre-supplied for the common case
dbt_mcp_tool = partial(generic_dbt_mcp_tool, name_enum=ToolName)
