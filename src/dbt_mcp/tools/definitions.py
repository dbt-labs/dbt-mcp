from collections.abc import Callable
from dataclasses import dataclass

from mcp.types import ToolAnnotations


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

    def get_name(self) -> str:
        return self.name or self.fn.__name__
