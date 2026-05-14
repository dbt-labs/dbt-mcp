from dbt_mcp.errors.base import ToolCallError


class ArtifactSearchError(ToolCallError):
    """Base exception for server-side artifact store failures (load, query, validation)."""


class ArtifactLoadError(ArtifactSearchError):
    """Raised when fetching or parsing an artifact from the Admin API fails."""


class ArtifactQueryError(ArtifactSearchError):
    """Raised when a SQL query against the artifact store fails."""


class ArtifactValidationError(ArtifactSearchError):
    """Raised when Pydantic validation of raw artifact JSON fails."""


class ArtifactNotLoadedError(ToolCallError):
    """Raised when querying before any artifacts have been loaded (client error)."""
