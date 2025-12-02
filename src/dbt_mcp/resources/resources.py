import logging

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.resources import FunctionResource

from dbt_mcp.resources.default_styleguide import get_default_styleguide
from dbt_mcp.resources.definitions import ResourceDefinition

logger = logging.getLogger(__name__)

# Re-export for convenience
__all__ = [
    "get_default_styleguide",
    "list_resource_definitions",
    "register_resources",
]


def list_resource_definitions() -> list[ResourceDefinition]:
    """Return a list of all available resource definitions.

    Returns:
        List of resource definitions to register
    """

    return [get_default_styleguide]


def register_resources(
    dbt_mcp: FastMCP,
    resource_definitions: list[ResourceDefinition],
) -> None:
    """Register MCP resources with the FastMCP server.

    Args:
        dbt_mcp: The FastMCP server instance
        resource_definitions: List of resource definitions to register
    """
    for resource_definition in resource_definitions:
        logger.info(f"Registering resource: {resource_definition.uri}")
        resource = FunctionResource.from_function(
            fn=resource_definition.content_fn,
            uri=resource_definition.get_uri(),
            name=resource_definition.name,
            description=resource_definition.description,
            mime_type=resource_definition.mime_type,
        )
        dbt_mcp.add_resource(resource)
