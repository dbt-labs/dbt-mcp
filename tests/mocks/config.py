from dbt_mcp.config.config import (
    Config,
    DbtCliConfig,
    DiscoveryConfig,
    RemoteConfig,
    SemanticLayerConfig,
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

mock_remote_config = RemoteConfig(
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
)

mock_discovery_config = DiscoveryConfig(
    host="http://localhost:8000",
    token="token",
    multicell_account_prefix=None,
    environment_id=1,
)

mock_semantic_layer_config = SemanticLayerConfig(
    host="localhost",
    service_token="token",
    multicell_account_prefix=None,
    prod_environment_id=1,
)

mock_config = Config(
    tracking_config=mock_tracking_config,
    remote_config=mock_remote_config,
    dbt_cli_config=mock_dbt_cli_config,
    discovery_config=mock_discovery_config,
    semantic_layer_config=mock_semantic_layer_config,
)
