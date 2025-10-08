"""SQL tool errors."""

from dbt_mcp.errors.base import ToolCallError


class SQLToolCallError(ToolCallError):
    """Base exception for SQL tool errors."""

    pass


class RemoteToolError(SQLToolCallError):
    """Exception raised when a remote SQL tool call fails."""

    pass
