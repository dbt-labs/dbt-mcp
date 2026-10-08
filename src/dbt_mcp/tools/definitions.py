from collections.abc import Callable
from dataclasses import dataclass, field, replace
from enum import Enum
from functools import cached_property, lru_cache, partial, wraps
from inspect import Parameter, Signature, isawaitable, signature, unwrap
from typing import Any

from mcp.server.fastmcp.tools.base import Tool
from mcp.server.fastmcp.utilities.func_metadata import FuncMetadata
from mcp.types import ToolAnnotations

from dbt_mcp.tools.binding import InputBinding
from dbt_mcp.tools.injection import (
    AdaptError,
    _accepts,
    _signature,
    adapt_with_mappers,
)
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.targets import Target, target_parameters


@lru_cache(maxsize=256)
def _input_metadata(contract: Signature, name: str) -> FuncMetadata:
    """Cache validation by input shape, independent of request values/choices."""

    def inputs() -> None:
        pass

    inputs.__signature__ = contract  # type: ignore[attr-defined]
    inputs.__name__ = name
    inputs.__annotations__ = {
        name: parameter.annotation for name, parameter in contract.parameters.items()
    }
    return Tool.from_function(inputs, structured_output=False).fn_metadata


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
    _binding: InputBinding = field(default_factory=InputBinding, repr=False)

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
            fn=self.fn,
            name=self.name,
            title=self.title,
            description=self.description,
            annotations=self.annotations,
            structured_output=self.structured_output,
            meta=self.meta,
        )
        tool.parameters = self.input_schema
        return tool

    def to_fastmcp_internal_tool(self) -> Tool:
        return self._internal_tool

    @cached_property
    def input_schema(self) -> dict[str, Any]:
        schema = _input_metadata(
            self.input_signature, self.fn.__name__
        ).arg_model.model_json_schema()
        return self._binding.input_schema(schema)

    def validate_and_bind(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Validate the public inputs and supply the host's hidden selections."""
        bound = self._binding.bind_arguments(
            arguments, parameters=set(self.input_schema["properties"])
        )
        metadata = _input_metadata(self.input_signature, self.fn.__name__)
        parsed = metadata.arg_model.model_validate(metadata.pre_parse_json(bound))
        return parsed.model_dump_one_level() | self._binding.values

    def bind_inputs(self, binding: InputBinding) -> "GenericToolDefinition[NameEnum]":
        """Return a request-specific interface without changing the original tool."""
        declaration = self.input_signature
        unknown = (
            binding.hidden_parameters | binding.choices.keys()
        ) - declaration.parameters.keys()
        if unknown:
            raise AdaptError(f"Unknown input bindings: {', '.join(sorted(unknown))}")
        if binding.hidden_parameters & binding.choices.keys():
            raise AdaptError("An input cannot be both bound and selectable")
        exposed = declaration.replace(
            parameters=[
                p
                for name, p in declaration.parameters.items()
                if name not in binding.hidden_parameters
            ]
        )

        @wraps(self.fn)
        async def invoke(*args: Any, **kwargs: Any) -> Any:
            inputs = exposed.bind(*args, **kwargs)
            values = binding.bind_arguments(
                inputs.arguments, parameters=set(exposed.parameters)
            )
            result = self.fn(**values)
            return await result if isawaitable(result) else result

        invoke.__signature__ = exposed  # type: ignore[attr-defined]
        return replace(self, fn=invoke, _input_signature=exposed, _binding=binding)

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
