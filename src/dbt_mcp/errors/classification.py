from dbt_mcp.errors.admin_api import (
    AdminAPIError,
    AdminAPIToolCallError,
    ArtifactRetrievalError,
)
from dbt_mcp.errors.artifact_search import (
    ArtifactNotLoadedError,
    ArtifactSearchError,
)
from dbt_mcp.errors.cli import BinaryExecutionError, CLIToolCallError
from dbt_mcp.errors.common import (
    ConfigurationError,
    InvalidParameterError,
    NotFoundError,
)
from dbt_mcp.errors.discovery import DiscoveryToolCallError, GraphQLError
from dbt_mcp.errors.semantic_layer import (
    SemanticLayerQueryTimeoutError,
    SemanticLayerToolCallError,
)
from dbt_mcp.errors.sql import RemoteToolError, SQLToolCallError

ClientToolCallError = (
    InvalidParameterError
    | NotFoundError
    | SemanticLayerQueryTimeoutError
    | GraphQLError
    | ArtifactNotLoadedError
)

ServerToolCallError = (
    SemanticLayerToolCallError
    | CLIToolCallError
    | BinaryExecutionError
    | SQLToolCallError
    | RemoteToolError
    | DiscoveryToolCallError
    | AdminAPIToolCallError
    | AdminAPIError
    | ArtifactRetrievalError
    | ConfigurationError
    | ArtifactSearchError
)
