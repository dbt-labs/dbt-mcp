from dbt_mcp.config.config import (
    Config,
    DbtCliConfig,
    DbtCodegenConfig,
    LspConfig,
)
from dbt_mcp.config.config_providers import (
    AdminApiConfig,
    DefaultAdminApiConfigProvider,
    DefaultDiscoveryConfigProvider,
    DefaultSemanticLayerConfigProvider,
    DefaultSqlConfigProvider,
    DiscoveryConfig,
    SemanticLayerConfig,
    SqlConfig,
)
from dbt_mcp.config.headers import (
    AdminApiHeadersProvider,
    DiscoveryHeadersProvider,
    SemanticLayerHeadersProvider,
    SqlHeadersProvider,
)
from dbt_mcp.config.settings import CredentialsProvider, DbtMcpSettings
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.oauth.token_provider import StaticTokenProvider

mock_settings = DbtMcpSettings.model_construct()

mock_sql_config = SqlConfig(
    url="http://localhost:8000",
    prod_environment_id=1,
    dev_environment_id=1,
    user_id=1,
    headers_provider=SqlHeadersProvider(
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

mock_lsp_config = LspConfig(
    project_dir="/test/project",
    lsp_path="/path/to/lsp",
)

mock_discovery_config = DiscoveryConfig(
    url="http://localhost:8000",
    headers_provider=DiscoveryHeadersProvider(
        token_provider=StaticTokenProvider(token="token")
    ),
    environment_id=1,
)

mock_semantic_layer_config = SemanticLayerConfig(
    host="localhost",
    token="token",
    url="http://localhost:8000",
    headers_provider=SemanticLayerHeadersProvider(
        token_provider=StaticTokenProvider(token="token")
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
class MockSqlConfigProvider(DefaultSqlConfigProvider):
    def __init__(self):
        pass  # Skip the base class __init__

    async def get_config(self):
        return mock_sql_config


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
    sql_config_provider=MockSqlConfigProvider(),
    dbt_cli_config=mock_dbt_cli_config,
    dbt_codegen_config=mock_dbt_codegen_config,
    discovery_config_provider=MockDiscoveryConfigProvider(),
    semantic_layer_config_provider=MockSemanticLayerConfigProvider(),
    admin_api_config_provider=MockAdminApiConfigProvider(),
    lsp_config=mock_lsp_config,
    disable_tools=[],
    credentials_provider=MockCredentialsProvider(),
)
