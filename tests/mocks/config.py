from dbt_mcp.config.config import (
    Config,
    DbtCliConfig,
    DbtCodegenConfig,
    LspConfig,
)
from dbt_mcp.config.elicitation import ElicitingCredentialsProvider
from dbt_mcp.config.config_providers import (
    AdminApiConfig,
    DiscoveryConfig,
    MultiProjectDiscoveryConfigProvider,
    ProxiedToolConfig,
    SemanticLayerConfig,
)
from dbt_mcp.config.config_providers.admin_api import DefaultAdminApiConfigProvider
from dbt_mcp.config.config_providers.discovery import DefaultDiscoveryConfigProvider
from dbt_mcp.config.config_providers.proxied_tool import (
    DefaultProxiedToolConfigProvider,
)
from dbt_mcp.config.config_providers.semantic_layer import (
    DefaultSemanticLayerConfigProvider,
    MultiProjectSemanticLayerConfigProvider,
)
from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.headers import (
    AdminApiHeadersProvider,
    DiscoveryHeadersProvider,
    ProxiedToolHeadersProvider,
    SemanticLayerHeadersProvider,
)
from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.lsp.lsp_binary_manager import LspBinaryInfo
from dbt_mcp.lsp.providers.local_lsp_client_provider import LocalLSPClientProvider
from dbt_mcp.lsp.providers.local_lsp_connection_provider import (
    LocalLSPConnectionProvider,
)
from dbt_mcp.oauth.token_provider import StaticTokenProvider

mock_settings = DbtMcpSettings.model_construct()

mock_proxied_tool_config = ProxiedToolConfig(
    url="http://localhost:8000",
    prod_environment_id=1,
    dev_environment_id=1,
    user_id=1,
    headers_provider=ProxiedToolHeadersProvider(
        token_provider=StaticTokenProvider(token="token")
    ),
)

mock_dbt_cli_config = DbtCliConfig(
    project_dir="/test/project",
    dbt_path="/path/to/dbt",
    dbt_cli_timeout=10,
    binary_type=BinaryType.DBT_CORE,
)

mock_dbt_codegen_config = DbtCodegenConfig(
    project_dir="/test/project",
    dbt_path="/path/to/dbt",
    dbt_cli_timeout=10,
    binary_type=BinaryType.DBT_CORE,
)

mock_local_lsp_connection_provider = LocalLSPConnectionProvider(
    lsp_binary_info=LspBinaryInfo(
        cmd=["/path/to/lsp"],
        version="1.0.0",
    ),
    project_dir="/test/project",
)

mock_lsp_config = LspConfig(
    local_lsp_connection_provider=mock_local_lsp_connection_provider,
    lsp_client_provider=LocalLSPClientProvider(
        lsp_connection_provider=mock_local_lsp_connection_provider,
    ),
)


mock_discovery_config = DiscoveryConfig(
    url="http://localhost:8000",
    headers_provider=DiscoveryHeadersProvider(
        token_provider=StaticTokenProvider(token="token")
    ),
    environment_id=1,
)

_mock_sl_token_provider = StaticTokenProvider(token="token")

mock_semantic_layer_config = SemanticLayerConfig(
    host="localhost",
    token_provider=_mock_sl_token_provider,
    url="http://localhost:8000",
    headers_provider=SemanticLayerHeadersProvider(
        token_provider=_mock_sl_token_provider
    ),
    prod_environment_id=1,
)

mock_admin_api_config = AdminApiConfig(
    url="http://localhost:8000",
    headers_provider=AdminApiHeadersProvider(
        token_provider=StaticTokenProvider(token="token")
    ),
    account_id=12345,
)


# Create mock config providers
class MockProxiedToolConfigProvider(DefaultProxiedToolConfigProvider):
    def __init__(self):
        pass  # Skip the base class __init__

    async def get_config(self):
        return mock_proxied_tool_config


class MockMultiProjectDiscoveryConfigProvider(MultiProjectDiscoveryConfigProvider):
    def __init__(self):
        pass  # Skip the base class __init__

    async def get_config(self, project_id: int):
        return mock_discovery_config


class MockDiscoveryConfigProvider(DefaultDiscoveryConfigProvider):
    def __init__(self):
        pass  # Skip the base class __init__

    async def get_config(self):
        return mock_discovery_config


class MockSemanticLayerConfigProvider(DefaultSemanticLayerConfigProvider):
    def __init__(self):
        pass  # Skip the base class __init__

    async def get_config(self):
        return mock_semantic_layer_config


class MockMultiProjectSemanticLayerConfigProvider(
    MultiProjectSemanticLayerConfigProvider
):
    def __init__(self):
        pass  # Skip the base class __init__

    async def get_config(self, project_id: int):
        return mock_semantic_layer_config


class MockAdminApiConfigProvider(DefaultAdminApiConfigProvider):
    def __init__(self):
        pass  # Skip the base class __init__

    async def get_config(self):
        return mock_admin_api_config


class MockCredentialsProvider(CredentialsProvider):
    def __init__(self, settings: DbtMcpSettings | None = None):
        super().__init__(settings or mock_settings)
        self.token_provider = StaticTokenProvider(token=self.settings.dbt_token)

    async def get_credentials(self):
        return self.settings, self.token_provider


mock_config = Config(
    proxied_tool_config_provider=MockProxiedToolConfigProvider(),
    dbt_cli_config=mock_dbt_cli_config,
    dbt_codegen_config=mock_dbt_codegen_config,
    multi_project_discovery_config_provider=MockMultiProjectDiscoveryConfigProvider(),
    discovery_config_provider=MockDiscoveryConfigProvider(),
    multi_project_semantic_layer_config_provider=MockMultiProjectSemanticLayerConfigProvider(),
    semantic_layer_config_provider=MockSemanticLayerConfigProvider(),
    admin_api_config_provider=MockAdminApiConfigProvider(),
    lsp_config=mock_lsp_config,
    disable_tools=[],
    enable_tools=None,  # None means not set, [] would mean allowlist mode
    disabled_toolsets=set(),
    enabled_toolsets=set(),
    credentials_provider=ElicitingCredentialsProvider(
        inner=MockCredentialsProvider(),
    ),
)
