from dbt_mcp.config.config import (
    AdminApiConfig,
    Config,
    DbtCliConfig,
    DiscoveryConfig,
    SemanticLayerConfig,
    SqlConfig,
    TrackingConfig,
)
from dbt_mcp.config.headers import (
    AdminApiHeadersProvider,
    DiscoveryHeadersProvider,
    SemanticLayerHeadersProvider,
    SqlHeadersProvider,
)
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.oauth.token_provider import StaticTokenProvider

mock_tracking_config = TrackingConfig(
    host="http://localhost:8000",
    host_prefix="test",
    prod_environment_id=1,
    dev_environment_id=1,
    dbt_cloud_user_id=1,
    local_user_id="1",
)

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

mock_discovery_config = DiscoveryConfig(
    url="http://localhost:8000",
    headers_provider=DiscoveryHeadersProvider(
        token_provider=StaticTokenProvider(token="token")
    ),
    environment_id=1,
)

mock_semantic_layer_config = SemanticLayerConfig(
    host="localhost",
    service_token="token",
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

mock_config = Config(
    tracking_config=mock_tracking_config,
    sql_config=mock_sql_config,
    dbt_cli_config=mock_dbt_cli_config,
    discovery_config=mock_discovery_config,
    semantic_layer_config=mock_semantic_layer_config,
    admin_api_config=mock_admin_api_config,
    disable_tools=[],
    token_provider=StaticTokenProvider(token="token"),
)
