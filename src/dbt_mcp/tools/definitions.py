from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from mcp.types import ToolAnnotations

from dbt_mcp.tools.injection import adapt_with_mapper
from dbt_mcp.tools.tool_names import ToolName


@dataclass
class GenericToolDefinition[NameEnum: Enum]:
    fn: Callable[..., Any]
    description: str
    name_enum: type[NameEnum]
    name: str | None = None
    title: str | None = None
    annotations: ToolAnnotations | None = None
    # We haven't strictly defined our tool contracts yet.
    # So we're setting this to False by default for now.
    structured_output: bool | None = False

    def get_name(self) -> NameEnum:
        return self.name_enum((self.name or self.fn.__name__).lower())

    def adapt_context(
        self, context_mapper: Callable[..., Any]
    ) -> "GenericToolDefinition[NameEnum]":
        """
        Adapt the tool definition to accept a different context object.
        """
        return type(self)(
            fn=adapt_with_mapper(self.fn, context_mapper),
            description=self.description,
            name_enum=self.name_enum,
            name=self.name,
            title=self.title,
            annotations=self.annotations,
            structured_output=self.structured_output,
        )


@dataclass
class ToolDefinition(GenericToolDefinition[ToolName]):
    name_enum: type[ToolName] = ToolName


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
