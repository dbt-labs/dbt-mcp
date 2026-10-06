from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import Enum
from functools import partial, wraps
from inspect import Parameter, Signature, isawaitable, signature, unwrap
from typing import Annotated, Any, get_args, get_origin

from mcp.server.fastmcp.tools.base import Tool
from mcp.types import ToolAnnotations
from pydantic.fields import FieldInfo

from dbt_mcp.tools.injection import (
    AdaptError,
    ContextInput,
    _accepts,
    adapt_with_mapper,
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
    def input_signature(self) -> Signature:
        """Complete unbound model contract, including context-only selectors."""
        parameters = dict(signature(self.fn).parameters)
        for name, parameter in self._context_inputs.items():
            parameters[name] = parameter.replace(
                kind=Parameter.KEYWORD_ONLY, default=Parameter.empty
            )
        ordered = sorted(
            parameters.values(),
            key=lambda p: (p.kind, p.default is not Parameter.empty),
        )
        return signature(self.fn).replace(parameters=ordered)

    @property
    def _context_inputs(self) -> dict[str, Parameter]:
        """Schema inputs declared on the canonical function and consumed by context."""
        return {
            name: parameter.replace(
                annotation=Annotated[*get_args(parameter.annotation), parameter.default]
            )
            if isinstance(parameter.default, FieldInfo)
            else parameter
            for name, parameter in signature(unwrap(self.fn)).parameters.items()
            if get_origin(parameter.annotation) is Annotated
            and any(
                isinstance(item, ContextInput)
                for item in get_args(parameter.annotation)[1:]
            )
        }

    @property
    def targets(self) -> dict[str, Target]:
        # Access declarations survive context adaptation even when no selector
        # is exposed by the configured mapper.
        return target_parameters(unwrap(self.fn))

    def invocation_function(self) -> Callable[..., Any]:
        """Apply declared input types to selectors consumed by context mappers.

        Configured mappers expose no selector. Project-aware mappers can accept
        omission internally to use a configured environment, but the model's
        declaration stays non-nullable and required whenever it is exposed.
        """
        implementation = signature(self.fn)
        declarations = self._context_inputs
        if not declarations.keys() & implementation.parameters.keys():
            return self.fn
        invocation = implementation.replace(
            parameters=[
                p.replace(annotation=declarations[p.name].annotation)
                if p.name in declarations
                else p
                for p in implementation.parameters.values()
            ]
        )

        @wraps(self.fn)
        async def invoke(*args: Any, **kwargs: Any) -> Any:
            result = self.fn(*args, **kwargs)
            return await result if isawaitable(result) else result

        invoke.__signature__ = invocation  # type: ignore[attr-defined]
        return invoke

    def get_name(self) -> NameEnum:
        return self.name_enum((self.name or self.fn.__name__).lower())

    def to_fastmcp_internal_tool(self) -> Tool:
        tool = Tool.from_function(
            fn=self.invocation_function(),
            name=self.name,
            title=self.title,
            description=self.description,
            annotations=self.annotations,
            structured_output=self.structured_output,
            meta=self.meta,
        )
        # Context input defaults declare schemas, rather than runtime values.
        # Transport omission support must not make exposed selectors optional.
        for name in self._context_inputs.keys() & tool.parameters["properties"].keys():
            tool.parameters["properties"][name].pop("default", None)
            tool.parameters.setdefault("required", []).append(name)
        if "required" in tool.parameters:
            tool.parameters["required"] = list(
                dict.fromkeys(tool.parameters["required"])
            )
        return tool

    def adapt_context(
        self,
        context_mapper: Callable[..., Any],
        **parameter_mappers: Callable[..., Any],
    ) -> "GenericToolDefinition[NameEnum]":
        """
        Adapt the tool definition to accept a different context object.
        """
        return self._adapted(
            adapt_with_mapper(self.fn, context_mapper, **parameter_mappers)
        )

    def adapt_with_mappers(
        self, **parameter_mappers: Callable[..., Any]
    ) -> "GenericToolDefinition[NameEnum]":
        """Inject parameters by name, including context and resolved selectors."""
        return self._adapted(adapt_with_mappers(self.fn, **parameter_mappers))

    def _adapted(self, fn: Callable[..., Any]) -> "GenericToolDefinition[NameEnum]":
        exposed = signature(fn)
        for name, declaration in self._context_inputs.items():
            parameter = exposed.parameters.get(name)
            if parameter is not None and not _accepts(
                parameter.annotation, declaration.annotation
            ):
                raise AdaptError(
                    f"{name}: context mapper cannot accept declared input {declaration.annotation!r}"
                )
        return replace(self, fn=fn)


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
