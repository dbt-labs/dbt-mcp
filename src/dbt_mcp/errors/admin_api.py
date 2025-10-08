"""Admin API tool errors."""

from dbt_mcp.errors.base import ToolCallError


class AdminAPIToolCallError(ToolCallError):
    """Base exception for Admin API tool errors."""

    pass


class AdminAPIError(AdminAPIToolCallError):
    """Exception raised for Admin API communication errors."""

    pass


class ArtifactRetrievalError(AdminAPIToolCallError):
    """Exception raised when artifact retrieval fails."""

    pass
