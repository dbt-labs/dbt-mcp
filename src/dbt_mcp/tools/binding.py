from collections.abc import Callable
from dataclasses import dataclass, field
from functools import wraps
from inspect import isawaitable
from typing import Any

from mcp.server.fastmcp.tools.base import Tool

from dbt_mcp.tools.injection import AdaptError, _signature, _with_signature


def configure_argument_validation(tool: Tool) -> None:
    # FastMCP doesn't expose model_config in Tool.from_function. Configure its
    # generated model locally, and publish the schema from that same model.
    model = tool.fn_metadata.arg_model
    model.model_config = {**model.model_config, "extra": "forbid"}
    model.model_rebuild(force=True)
    tool.parameters = model.model_json_schema(by_alias=True)


@dataclass(frozen=True)
class InputBinding:
    """A host's input selection, applied to the callable FastMCP validates."""

    values: dict[str, Any] = field(default_factory=dict)
    hidden: frozenset[str] = frozenset()

    @property
    def hidden_parameters(self) -> frozenset[str]:
        return self.hidden | self.values.keys()

    def bind_callable(
        self,
        fn: Callable[..., Any],
    ) -> Callable[..., Any]:
        declaration = _signature(fn)
        unknown = self.hidden_parameters - declaration.parameters.keys()
        if unknown:
            raise AdaptError(f"Unknown input bindings: {', '.join(sorted(unknown))}")
        exposed = declaration.replace(
            parameters=[
                parameter
                for name, parameter in declaration.parameters.items()
                if name not in self.hidden_parameters
            ]
        )

        @wraps(fn)
        async def invoke(*args: Any, **kwargs: Any) -> Any:
            inputs = dict(exposed.bind(*args, **kwargs).arguments) | self.values
            result = fn(**inputs)
            return await result if isawaitable(result) else result

        return _with_signature(invoke, exposed)

    def bind_tool(self, tool: Tool) -> Tool:
        """Return a per-request tool; never mutate the registered callable/model."""
        bound = Tool.from_function(
            self.bind_callable(tool.fn),
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
