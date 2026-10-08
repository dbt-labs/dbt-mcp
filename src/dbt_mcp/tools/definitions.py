from collections.abc import Callable
from dataclasses import dataclass, field, replace
from enum import Enum
from functools import cached_property, partial
from inspect import Parameter, Signature, signature, unwrap
from typing import Any

from mcp.server.fastmcp.tools.base import Tool
from mcp.types import ToolAnnotations

from dbt_mcp.tools.binding import CallScope, InputBinding, configure_argument_validation
from dbt_mcp.tools.injection import (
    AdaptError,
    _accepts,
    _signature,
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
    _input_signature: Signature | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        # Adapted/bound signatures may hide every target. The canonical function
        # must still declare access explicitly before any adaptation takes place.
        if self.requirements is None and not self.targets:
            raise ValueError("Tools must declare access requirements")

    @property
    def input_signature(self) -> Signature:
        """Declared model inputs, independent of context mapper defaults."""
        return self._input_signature or _signature(self.fn)

    @property
    def targets(self) -> dict[str, Target]:
        # Access declarations survive context adaptation even when no selector
        # is exposed by the configured mapper.
        return target_parameters(unwrap(self.fn))

    def get_name(self) -> NameEnum:
        return self.name_enum((self.name or self.fn.__name__).lower())

    @cached_property
    def _internal_tool(self) -> Tool:
        tool = Tool.from_function(
            fn=(
                self.fn
                if signature(self.fn) == self.input_signature
                else InputBinding().bind_callable(
                    self.fn, declaration=self.input_signature
                )
            ),
            name=self.name,
            title=self.title,
            description=self.description,
            annotations=self.annotations,
            structured_output=self.structured_output,
            meta=self.meta,
        )
        configure_argument_validation(tool)
        return tool

    def to_fastmcp_internal_tool(self) -> Tool:
        return self._internal_tool

    def bind_inputs(
        self, binding: InputBinding, *, call_scope: CallScope | None = None
    ) -> "GenericToolDefinition[NameEnum]":
        """Return a request-specific interface without changing the original tool."""
        fn = binding.bind_callable(
            self.fn, declaration=self.input_signature, call_scope=call_scope
        )
        return replace(self, fn=fn, _input_signature=signature(fn))

    def adapt_with_mappers(
        self, **parameter_mappers: Callable[..., Any]
    ) -> "GenericToolDefinition[NameEnum]":
        """Inject parameters by name, including context and resolved selectors."""
        fn = adapt_with_mappers(self.fn, **parameter_mappers)
        exposed = signature(fn)
        declarations = self.input_signature.parameters
        for name, declaration in declarations.items():
            parameter = exposed.parameters.get(name)
            if parameter is not None and not _accepts(
                parameter.annotation, declaration.annotation
            ):
                raise AdaptError(
                    f"{name}: context mapper cannot accept declared input {declaration.annotation!r}"
                )
        parameters = [
            declarations.get(name, parameter)
            for name, parameter in exposed.parameters.items()
        ]
        parameters.sort(key=lambda p: (p.kind, p.default is not Parameter.empty))
        contract = exposed.replace(parameters=parameters)
        return replace(self, fn=fn, _input_signature=contract)


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
