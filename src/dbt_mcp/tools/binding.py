from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, nullcontext
from dataclasses import dataclass, field
from functools import partial, wraps
from inspect import Parameter, isawaitable
from typing import Annotated, Any

from mcp.server.fastmcp.tools.base import Tool
from pydantic import BeforeValidator, Field
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from dbt_mcp.tools.injection import AdaptError, _signature, _with_signature

type CallScope = Callable[[dict[str, Any]], AbstractAsyncContextManager[dict[str, Any]]]


def configure_argument_validation(tool: Tool) -> None:
    # FastMCP doesn't expose model_config in Tool.from_function. Configure its
    # generated model locally, and publish the schema from that same model.
    model = tool.fn_metadata.arg_model
    model.model_config = {**model.model_config, "extra": "forbid"}
    model.model_rebuild(force=True)
    tool.parameters = model.model_json_schema(by_alias=True)


def _check_choice(value: Any, *, choices: tuple[Any, ...]) -> Any:
    if not any(type(value) is type(choice) and value == choice for choice in choices):
        raise ValueError(f"choose one of {list(choices)}")
    return value


@dataclass(frozen=True)
class InputBinding:
    """A host's input selection, applied to the callable FastMCP validates."""

    values: dict[str, Any] = field(default_factory=dict)
    hidden: frozenset[str] = frozenset()
    choices: dict[str, tuple[Any, ...]] = field(default_factory=dict)

    @property
    def hidden_parameters(self) -> frozenset[str]:
        return self.hidden | self.values.keys()

    def bind_callable(
        self,
        fn: Callable[..., Any],
        *,
        call_scope: CallScope | None = None,
    ) -> Callable[..., Any]:
        declaration = _signature(fn)
        unknown = (
            self.hidden_parameters | self.choices.keys()
        ) - declaration.parameters.keys()
        if unknown:
            raise AdaptError(f"Unknown input bindings: {', '.join(sorted(unknown))}")
        if self.hidden_parameters & self.choices.keys():
            raise AdaptError("An input cannot be both bound and selectable")
        parameters = []
        for name, parameter in declaration.parameters.items():
            if name in self.hidden_parameters:
                continue
            if name in self.choices:
                # Choice constraints are part of FastMCP's argument model too.
                # Preserve declared field constraints while making it required.
                declared_field = (
                    FieldInfo.merge_field_infos(
                        parameter.default, default=PydanticUndefined
                    )
                    if isinstance(parameter.default, FieldInfo)
                    else Field()
                )
                parameter = parameter.replace(
                    default=Parameter.empty,
                    annotation=Annotated[
                        parameter.annotation,
                        declared_field,
                        BeforeValidator(
                            partial(_check_choice, choices=self.choices[name])
                        ),
                        Field(
                            json_schema_extra={"enum": list(self.choices[name])},
                        ),
                    ],
                )
            parameters.append(parameter)
        parameters.sort(key=lambda p: (p.kind, p.default is not Parameter.empty))
        exposed = declaration.replace(parameters=parameters)

        @wraps(fn)
        async def invoke(*args: Any, **kwargs: Any) -> Any:
            inputs = dict(exposed.bind(*args, **kwargs).arguments) | self.values
            scope = call_scope(inputs) if call_scope else nullcontext(inputs)
            async with scope as resolved:
                result = fn(**resolved)
                return await result if isawaitable(result) else result

        return _with_signature(invoke, exposed)

    def bind_tool(self, tool: Tool, *, call_scope: CallScope | None = None) -> Tool:
        """Return a per-request tool; never mutate the registered callable/model."""
        bound = Tool.from_function(
            self.bind_callable(tool.fn, call_scope=call_scope),
            name=tool.name,
            title=tool.title,
            description=tool.description,
            annotations=tool.annotations,
            icons=tool.icons,
            meta=tool.meta,
            structured_output=tool.output_schema is not None,
        )
        configure_argument_validation(bound)
        return bound
