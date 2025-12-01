import os
from dataclasses import dataclass

from dbt_mcp.config.config_providers import (
    DefaultAdminApiConfigProvider,
    DefaultDiscoveryConfigProvider,
    DefaultProxiedToolConfigProvider,
    DefaultSemanticLayerConfigProvider,
)
from dbt_mcp.config.settings import (
    CredentialsProvider,
    DbtMcpSettings,
)
from dbt_mcp.dbt_cli.binary_type import BinaryType, detect_binary_type
from dbt_mcp.lsp.lsp_binary_manager import LspBinaryInfo, dbt_lsp_binary_info
from dbt_mcp.telemetry.logging import configure_logging
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

PACKAGE_NAME = "dbt-mcp"

TOOLSET_TO_DISABLE_ATTR = {
    Toolset.SEMANTIC_LAYER: "disable_semantic_layer",
    Toolset.ADMIN_API: "disable_admin_api",
    Toolset.DBT_CLI: "disable_dbt_cli",
    Toolset.DBT_CODEGEN: "disable_dbt_codegen",
    Toolset.DISCOVERY: "disable_discovery",
    Toolset.DBT_LSP: "disable_lsp",
    Toolset.SQL: "actual_disable_sql",
}

TOOLSET_TO_ENABLE_ATTR = {
    Toolset.SEMANTIC_LAYER: "enable_semantic_layer",
    Toolset.ADMIN_API: "enable_admin_api",
    Toolset.DBT_CLI: "enable_dbt_cli",
    Toolset.DBT_CODEGEN: "enable_dbt_codegen",
    Toolset.DISCOVERY: "enable_discovery",
    Toolset.DBT_LSP: "enable_lsp",
    Toolset.SQL: "enable_sql",
}


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
class LspConfig:
    project_dir: str
    lsp_binary_info: LspBinaryInfo | None


@dataclass
class Config:
    disable_tools: list[ToolName]
    enable_tools: list[ToolName]
    disabled_toolsets: set[Toolset]
    enabled_toolsets: set[Toolset]
    proxied_tool_config_provider: DefaultProxiedToolConfigProvider | None
    dbt_cli_config: DbtCliConfig | None
    dbt_codegen_config: DbtCodegenConfig | None
    discovery_config_provider: DefaultDiscoveryConfigProvider | None
    semantic_layer_config_provider: DefaultSemanticLayerConfigProvider | None
    admin_api_config_provider: DefaultAdminApiConfigProvider | None
    credentials_provider: CredentialsProvider
    lsp_config: LspConfig | None


def load_config(enable_proxied_tools: bool = True) -> Config:
    settings = DbtMcpSettings()  # type: ignore
    configure_logging(file_logging=settings.file_logging, log_level=settings.log_level)
    credentials_provider = CredentialsProvider(settings)

    # Set default warn error options if not provided
    if settings.dbt_warn_error_options is None:
        warn_error_options = '{"error": ["NoNodesForSelectionCriteria"]}'
        os.environ["DBT_WARN_ERROR_OPTIONS"] = warn_error_options

    # Build configurations
    enabled_toolsets: set[Toolset] = {
        toolset
        for toolset, attr_name in TOOLSET_TO_ENABLE_ATTR.items()
        if getattr(settings, attr_name, False)
    }

    disabled_toolsets: set[Toolset] = {
        toolset
        for toolset, attr_name in TOOLSET_TO_DISABLE_ATTR.items()
        if getattr(settings, attr_name, False)
    }

    proxied_tool_config_provider = None
    if enable_proxied_tools and settings.actual_host:
        proxied_tool_config_provider = DefaultProxiedToolConfigProvider(
            credentials_provider=credentials_provider
        )

    admin_api_config_provider = None
    if settings.actual_host:
        admin_api_config_provider = DefaultAdminApiConfigProvider(
            credentials_provider=credentials_provider,
        )

    dbt_cli_config = None
    if settings.dbt_project_dir and settings.dbt_path:
        binary_type = detect_binary_type(settings.dbt_path)
        dbt_cli_config = DbtCliConfig(
            project_dir=settings.dbt_project_dir,
            dbt_path=settings.dbt_path,
            dbt_cli_timeout=settings.dbt_cli_timeout,
            binary_type=binary_type,
        )

    dbt_codegen_config = None
    if settings.dbt_project_dir and settings.dbt_path:
        binary_type = detect_binary_type(settings.dbt_path)
        dbt_codegen_config = DbtCodegenConfig(
            project_dir=settings.dbt_project_dir,
            dbt_path=settings.dbt_path,
            dbt_cli_timeout=settings.dbt_cli_timeout,
            binary_type=binary_type,
        )

    discovery_config_provider = None
    if settings.actual_host:
        discovery_config_provider = DefaultDiscoveryConfigProvider(
            credentials_provider=credentials_provider,
        )

    semantic_layer_config_provider = None
    if settings.actual_host:
        semantic_layer_config_provider = DefaultSemanticLayerConfigProvider(
            credentials_provider=credentials_provider,
        )

    lsp_config = None
    if settings.dbt_project_dir:
        lsp_binary_info = dbt_lsp_binary_info(settings.dbt_lsp_path)
        lsp_config = LspConfig(
            project_dir=settings.dbt_project_dir,
            lsp_binary_info=lsp_binary_info,
        )

    return Config(
        disable_tools=settings.disable_tools or [],
        enable_tools=settings.enable_tools or [],
        disabled_toolsets=disabled_toolsets,
        enabled_toolsets=enabled_toolsets,
        proxied_tool_config_provider=proxied_tool_config_provider,
        dbt_cli_config=dbt_cli_config,
        dbt_codegen_config=dbt_codegen_config,
        discovery_config_provider=discovery_config_provider,
        semantic_layer_config_provider=semantic_layer_config_provider,
        admin_api_config_provider=admin_api_config_provider,
        credentials_provider=credentials_provider,
        lsp_config=lsp_config,
    )
