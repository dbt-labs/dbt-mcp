"""dbt CLI tool errors."""

from dbt_mcp.errors.base import ToolCallError


class CLIToolCallError(ToolCallError):
    """Base exception for dbt CLI tool errors."""

    pass


class BinaryExecutionError(CLIToolCallError):
    """Exception raised when dbt binary execution fails."""

    pass
