from mcp import ServerSession
from mcp.server.fastmcp import Context, FastMCP
from mcp.shared.context import RequestContext
from starlette.requests import Request

from dbt_mcp.config.config import (
    AdminApiConfig,
    DbtCliConfig,
    DiscoveryConfig,
    SemanticLayerConfig,
)
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.semantic_layer.client import SemanticLayerClientProtocol


class DbtMcpContext(Context[ServerSession, object, Request]):
    """Custom context for the MCP server"""

    _semantic_layer_config: SemanticLayerConfig | None = None
    _semantic_layer_client: SemanticLayerClientProtocol | None = None
    _discovery_config: DiscoveryConfig | None = None
    _dbt_cli_config: DbtCliConfig | None = None
    _admin_api_config: AdminApiConfig | None = None
    _admin_api_client: DbtAdminAPIClient | None = None

    def __init__(
        self,
        request_context: RequestContext[ServerSession, object, Request] | None = None,
        fastmcp: FastMCP | None = None,
        semantic_layer_config: SemanticLayerConfig | None = None,
        semantic_layer_client: SemanticLayerClientProtocol | None = None,
        discovery_config: DiscoveryConfig | None = None,
        dbt_cli_config: DbtCliConfig | None = None,
        admin_api_config: AdminApiConfig | None = None,
        admin_api_client: DbtAdminAPIClient | None = None,
    ):
        super().__init__(request_context=request_context, fastmcp=fastmcp)
        self._semantic_layer_config = semantic_layer_config
        self._semantic_layer_client = semantic_layer_client
        self._discovery_config = discovery_config
        self._dbt_cli_config = dbt_cli_config
        self._admin_api_config = admin_api_config
        self._admin_api_client = admin_api_client

    def get_semantic_layer_config(self) -> SemanticLayerConfig:
        if self._semantic_layer_config is None:
            raise ValueError("Semantic layer config is not set")
        return self._semantic_layer_config

    def get_semantic_layer_client(self) -> SemanticLayerClientProtocol:
        if self._semantic_layer_client is None:
            raise ValueError("Semantic layer client is not set")
        return self._semantic_layer_client

    def get_discovery_config(self) -> DiscoveryConfig:
        if self._discovery_config is None:
            raise ValueError("Discovery config is not set")
        return self._discovery_config

    def get_dbt_cli_config(self) -> DbtCliConfig:
        if self._dbt_cli_config is None:
            raise ValueError("Dbt cli config is not set")
        return self._dbt_cli_config

    def get_admin_api_config(self) -> AdminApiConfig:
        if self._admin_api_config is None:
            raise ValueError("Admin api config is not set")
        return self._admin_api_config

    def get_admin_api_client(self) -> DbtAdminAPIClient:
        if self._admin_api_client is None:
            raise ValueError("Admin api client is not set")
        return self._admin_api_client
