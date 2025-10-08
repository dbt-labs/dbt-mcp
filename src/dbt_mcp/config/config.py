import os
from dataclasses import dataclass

from dbt_mcp.config.config_providers import (
    DefaultAdminApiConfigProvider,
    DefaultDiscoveryConfigProvider,
    DefaultSemanticLayerConfigProvider,
    DefaultSqlConfigProvider,
)
from dbt_mcp.config.settings import (
    CredentialsProvider,
    DbtMcpSettings,
)
from dbt_mcp.dbt_cli.binary_type import BinaryType, detect_binary_type
from dbt_mcp.telemetry.logging import configure_logging
from dbt_mcp.tools.tool_names import ToolName

PACKAGE_NAME = "dbt-mcp"


@dataclass
class DbtCliConfig:
    project_dir: str
    dbt_path: str
    dbt_cli_timeout: int
    binary_type: BinaryType


@dataclass
class DbtCodegenConfig:
    project_dir: str
    dbt_path: str
    dbt_cli_timeout: int
    binary_type: BinaryType


@dataclass
class Config:
    disable_tools: list[ToolName]
    sql_config_provider: DefaultSqlConfigProvider | None
    dbt_cli_config: DbtCliConfig | None
    dbt_codegen_config: DbtCodegenConfig | None
    discovery_config_provider: DefaultDiscoveryConfigProvider | None
    semantic_layer_config_provider: DefaultSemanticLayerConfigProvider | None
    admin_api_config_provider: DefaultAdminApiConfigProvider | None
    credentials_provider: CredentialsProvider


def load_config() -> Config:
    settings = DbtMcpSettings()  # type: ignore
    configure_logging(settings.file_logging)
    credentials_provider = CredentialsProvider(settings)

    # Set default warn error options if not provided
    if settings.dbt_warn_error_options is None:
        warn_error_options = '{"error": ["NoNodesForSelectionCriteria"]}'
        os.environ["DBT_WARN_ERROR_OPTIONS"] = warn_error_options

    # Build configurations
    sql_config_provider = None
    if not settings.actual_disable_sql:
        sql_config_provider = DefaultSqlConfigProvider(
            credentials_provider=credentials_provider,
        )

    admin_api_config_provider = None
    if not settings.disable_admin_api:
        admin_api_config_provider = DefaultAdminApiConfigProvider(
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

    dbt_codegen_config = None
    if (
        not settings.disable_dbt_codegen
        and settings.dbt_project_dir
        and settings.dbt_path
    ):
        binary_type = detect_binary_type(settings.dbt_path)
        dbt_codegen_config = DbtCodegenConfig(
            project_dir=settings.dbt_project_dir,
            dbt_path=settings.dbt_path,
            dbt_cli_timeout=settings.dbt_cli_timeout,
            binary_type=binary_type,
        )

    discovery_config_provider = None
    if not settings.disable_discovery:
        discovery_config_provider = DefaultDiscoveryConfigProvider(
            credentials_provider=credentials_provider,
        )

    semantic_layer_config_provider = None
    if not settings.disable_semantic_layer:
        semantic_layer_config_provider = DefaultSemanticLayerConfigProvider(
            credentials_provider=credentials_provider,
        )

    return Config(
        disable_tools=settings.disable_tools or [],
        sql_config_provider=sql_config_provider,
        dbt_cli_config=dbt_cli_config,
        dbt_codegen_config=dbt_codegen_config,
        discovery_config_provider=discovery_config_provider,
        semantic_layer_config_provider=semantic_layer_config_provider,
        admin_api_config_provider=admin_api_config_provider,
        credentials_provider=credentials_provider,
    )
