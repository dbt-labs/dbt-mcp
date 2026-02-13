"""Discovery/Metadata API tool errors."""

from dbt_mcp.errors.base import ToolCallError


class DiscoveryToolCallError(ToolCallError):
    """Base exception for Discovery/Metadata API tool errors."""

    pass


class GraphQLError(DiscoveryToolCallError):
    """Exception raised for GraphQL API and query errors."""

    pass
