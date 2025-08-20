from dbt_mcp.config.config import (
    Config,
    DbtCliConfig,
    DiscoveryConfig,
    SemanticLayerConfig,
    SqlConfig,
    TrackingConfig,
)

mock_tracking_config = TrackingConfig(
    host="http://localhost:8000",
    multicell_account_prefix="test",
    prod_environment_id=1,
    dev_environment_id=1,
    dbt_cloud_user_id=1,
    local_user_id="1",
)

mock_sql_config = SqlConfig(
    multicell_account_prefix=None,
    prod_environment_id=1,
    dev_environment_id=1,
    user_id=1,
    token="token",
    host="http://localhost/mcp",
)

mock_dbt_cli_config = DbtCliConfig(
    project_dir="/test/project",
    dbt_path="/path/to/dbt",
    dbt_cli_timeout=10,
)

mock_discovery_config = DiscoveryConfig(
    url="http://localhost:8000",
    headers={
        "Authorization": "Bearer token",
        "Content-Type": "application/json",
    },
    environment_id=1,
)

mock_semantic_layer_config = SemanticLayerConfig(
    host="localhost",
    service_token="token",
    url="http://localhost:8000",
    headers={
        "Authorization": "Bearer token",
        "Content-Type": "application/json",
    },
    prod_environment_id=1,
)

mock_config = Config(
    tracking_config=mock_tracking_config,
    sql_config=mock_sql_config,
    dbt_cli_config=mock_dbt_cli_config,
    discovery_config=mock_discovery_config,
    semantic_layer_config=mock_semantic_layer_config,
    disable_tools=[],
)
