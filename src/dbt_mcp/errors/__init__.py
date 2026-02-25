from dbt_mcp.errors.admin_api import (
    AdminAPIError,
    AdminAPIToolCallError,
    ArtifactRetrievalError,
)
from dbt_mcp.errors.base import ToolCallError
from dbt_mcp.errors.cli import BinaryExecutionError, CLIToolCallError
from dbt_mcp.errors.common import InvalidParameterError, NotFoundError
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
)

__all__ = [
    "AdminAPIError",
    "AdminAPIToolCallError",
    "ArtifactRetrievalError",
    "BinaryExecutionError",
    "CLIToolCallError",
    "ClientToolCallError",
    "DiscoveryToolCallError",
    "GraphQLError",
    "InvalidParameterError",
    "NotFoundError",
    "RemoteToolError",
    "SQLToolCallError",
    "SemanticLayerQueryTimeoutError",
    "SemanticLayerToolCallError",
    "ServerToolCallError",
    "ToolCallError",
]
