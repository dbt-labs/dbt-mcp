"""Semantic Layer tool errors."""

from dbt_mcp.errors.base import ToolCallError


class SemanticLayerToolCallError(ToolCallError):
    """Base exception for Semantic Layer tool errors."""

    pass


class SemanticLayerQueryTimeoutError(SemanticLayerToolCallError):
    """Exception raised when a semantic layer query times out with COMPILED status.

    A COMPILED timeout means the query finished SQL compilation and is executing
    against the data platform. In agent contexts this indicates the query is too
    complex or returns too much data, and the client should simplify the request.
    """

    pass
