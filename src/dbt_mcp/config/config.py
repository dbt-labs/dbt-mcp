import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from dbt_mcp.config.config_providers import (
    AdminApiConfigProvider,
    DiscoveryConfigProvider,
    SemanticLayerConfigProvider,
)
from dbt_mcp.config.headers import (
    HeadersProvider,
    SqlHeadersProvider,
)
from dbt_mcp.config.settings import CredentialsProvider, DbtMcpSettings
from dbt_mcp.dbt_cli.binary_type import BinaryType, detect_binary_type
from dbt_mcp.tools.tool_names import ToolName


@dataclass
class TrackingConfig:
    host: str | None = None
    host_prefix: str | None = None
    prod_environment_id: int | None = None
    dev_environment_id: int | None = None
    dbt_cloud_user_id: int | None = None
    local_user_id: str | None = None


@dataclass
class DbtCliConfig:
    project_dir: str
    dbt_path: str
    dbt_cli_timeout: int
    binary_type: BinaryType


@dataclass
class SqlConfig:
    user_id: int
    dev_environment_id: int
    prod_environment_id: int
    url: str
    headers_provider: HeadersProvider


@dataclass
class Config:
    tracking_config: TrackingConfig
    disable_tools: list[ToolName]
    sql_config: SqlConfig | None
    dbt_cli_config: DbtCliConfig | None
    discovery_config_provider: DiscoveryConfigProvider | None
    semantic_layer_config_provider: SemanticLayerConfigProvider | None
    admin_api_config_provider: AdminApiConfigProvider | None


def load_config() -> Config:
    settings = DbtMcpSettings()  # type: ignore
    credentials_provider = CredentialsProvider(settings)

    # Set default warn error options if not provided
    if settings.dbt_warn_error_options is None:
        warn_error_options = '{"error": ["NoNodesForSelectionCriteria"]}'
        os.environ["DBT_WARN_ERROR_OPTIONS"] = warn_error_options

    # Build configurations
    sql_config = None
    if (
        not settings.actual_disable_sql
        and settings.dbt_user_id
        and settings.dbt_token
        and settings.dbt_dev_env_id
        and settings.actual_prod_environment_id
        and settings.actual_host
    ):
        _, token_provider = credentials_provider.get_credentials()
        is_local = settings.actual_host and settings.actual_host.startswith("localhost")
        path = "/v1/mcp/" if is_local else "/api/ai/v1/mcp/"
        scheme = "http://" if is_local else "https://"
        host_prefix = (
            f"{settings.actual_host_prefix}." if settings.actual_host_prefix else ""
        )
        url = f"{scheme}{host_prefix}{settings.actual_host}{path}"
        sql_config = SqlConfig(
            user_id=settings.dbt_user_id,
            dev_environment_id=settings.dbt_dev_env_id,
            prod_environment_id=settings.actual_prod_environment_id,
            url=url,
            headers_provider=SqlHeadersProvider(token_provider=token_provider),
        )

    admin_api_config_provider = None
    if not settings.disable_admin_api:
        admin_api_config_provider = AdminApiConfigProvider(
            credentials_provider=credentials_provider,
        )

    dbt_cli_config = None
    if not settings.disable_dbt_cli and settings.dbt_project_dir and settings.dbt_path:
        binary_type = detect_binary_type(settings.dbt_path)
        dbt_cli_config = DbtCliConfig(
            project_dir=settings.dbt_project_dir,
            dbt_path=settings.dbt_path,
            dbt_cli_timeout=settings.dbt_cli_timeout,
            binary_type=binary_type,
        )

    discovery_config_provider = None
    if not settings.disable_discovery:
        discovery_config_provider = DiscoveryConfigProvider(
            credentials_provider=credentials_provider,
        )

    semantic_layer_config_provider = None
    if not settings.disable_semantic_layer:
        semantic_layer_config_provider = SemanticLayerConfigProvider(
            credentials_provider=credentials_provider,
        )

    # Load local user ID from dbt profile
    local_user_id = None
    try:
        home = os.environ.get("HOME")
        user_path = Path(f"{home}/.dbt/.user.yml")
        if home and user_path.exists():
            with open(user_path) as f:
                local_user_id = yaml.safe_load(f).get("id")
    except Exception:
        pass

    return Config(
        tracking_config=TrackingConfig(
            host=settings.actual_host,
            host_prefix=settings.actual_host_prefix,
            prod_environment_id=settings.actual_prod_environment_id,
            dev_environment_id=settings.dbt_dev_env_id,
            dbt_cloud_user_id=settings.dbt_user_id,
            local_user_id=local_user_id,
        ),
        disable_tools=settings.disable_tools or [],
        sql_config=sql_config,
        dbt_cli_config=dbt_cli_config,
        discovery_config_provider=discovery_config_provider,
        semantic_layer_config_provider=semantic_layer_config_provider,
        admin_api_config_provider=admin_api_config_provider,
    )
