from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class ResourceDefinition:
    """Definition for an MCP resource."""

    uri: str
    name: str
    description: str
    mime_type: str
    content_fn: Callable[[], str]

    def get_uri(self) -> str:
        return self.uri

    def get_content(self) -> str:
        """Get the content of the resource by calling the content function."""
        return self.content_fn()


def dbt_mcp_resource(
    uri: str,
    name: str,
    description: str,
    mime_type: str = "text/plain",
) -> Callable[[Callable], ResourceDefinition]:
    """Decorator to define a resource definition for dbt MCP"""

    def decorator(fn: Callable) -> ResourceDefinition:
        return ResourceDefinition(
            uri=uri,
            name=name,
            description=description,
            mime_type=mime_type,
            content_fn=fn,
        )

    return decorator
