"""Common errors used across multiple tool types."""

from dbt_mcp.errors.base import ToolCallError


class InvalidParameterError(ToolCallError):
    """Exception raised when invalid or missing parameters are provided.

    This is a cross-cutting error used by multiple tool types.
    """

    pass
