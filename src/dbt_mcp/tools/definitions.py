from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from mcp.types import ToolAnnotations

from dbt_mcp.tools.injection import adapt_with_mapper
from dbt_mcp.tools.tool_names import ToolName


@dataclass
class ToolDefinition:
    fn: Callable
    description: str
    name: str | None = None
    title: str | None = None
    annotations: ToolAnnotations | None = None
    # We haven't strictly defined our tool contracts yet.
    # So we're setting this to False by default for now.
    structured_output: bool | None = False

    def get_name(self) -> ToolName:
        return ToolName((self.name or self.fn.__name__).lower())

    def adapt_context(self, context_mapper: Callable[..., Any]) -> "ToolDefinition":
        """
        Adapt the tool definition to accept a different context object.
        """
        return ToolDefinition(
            fn=adapt_with_mapper(self.fn, context_mapper),
            description=self.description,
            name=self.name,
            title=self.title,
            annotations=self.annotations,
            structured_output=self.structured_output,
        )


def dbt_mcp_tool(
    description: str,
    name: str | None = None,
    title: str | None = None,
    read_only_hint: bool = False,
    destructive_hint: bool = True,
    idempotent_hint: bool = False,
    open_world_hint: bool = True,
    structured_output: bool | None = False,
) -> Callable[[Callable], ToolDefinition]:
    """Decorator to define a tool definition for dbt MCP"""

    def decorator(fn: Callable) -> ToolDefinition:
        return ToolDefinition(
            fn=fn,
            description=description,
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
        )

    return decorator
