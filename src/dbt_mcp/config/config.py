import os
from dataclasses import dataclass

from dbt_mcp.config.config_providers.admin_api import DefaultAdminApiConfigProvider
from dbt_mcp.config.config_providers.discovery import (
    DefaultDiscoveryConfigProvider,
    MultiProjectDiscoveryConfigProvider,
)
from dbt_mcp.config.config_providers.proxied_tool import (
    DefaultProxiedToolConfigProvider,
)
from dbt_mcp.config.config_providers.semantic_layer import (
    DefaultSemanticLayerConfigProvider,
    MultiProjectSemanticLayerConfigProvider,
)
from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.elicitation import ConfigPersistence, ElicitingCredentialsProvider
from dbt_mcp.config.settings import (
    DbtMcpLogSettings,
    DbtMcpSettings,
)
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.dbt_cli.binary_type import BinaryType, detect_binary_type
from dbt_mcp.lsp.lsp_binary_manager import dbt_lsp_binary_info
from dbt_mcp.lsp.providers.local_lsp_client_provider import LocalLSPClientProvider
from dbt_mcp.lsp.providers.local_lsp_connection_provider import (
    LocalLSPConnectionProvider,
)
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
    Toolset.PRODUCT_DOCS: "disable_product_docs",
    Toolset.MCP_SERVER_METADATA: "disable_mcp_server_metadata",
}

TOOLSET_TO_ENABLE_ATTR = {
    Toolset.SEMANTIC_LAYER: "enable_semantic_layer",
    Toolset.ADMIN_API: "enable_admin_api",
    Toolset.DBT_CLI: "enable_dbt_cli",
    Toolset.DBT_CODEGEN: "enable_dbt_codegen",
    Toolset.DISCOVERY: "enable_discovery",
    Toolset.DBT_LSP: "enable_lsp",
    Toolset.SQL: "enable_sql",
    Toolset.PRODUCT_DOCS: "enable_product_docs",
    Toolset.MCP_SERVER_METADATA: "enable_mcp_server_metadata",
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
    local_lsp_connection_provider: LocalLSPConnectionProvider
    lsp_client_provider: LocalLSPClientProvider


@dataclass
class Config:
    disable_tools: list[ToolName]
    enable_tools: list[ToolName] | None
    disabled_toolsets: set[Toolset]
    enabled_toolsets: set[Toolset]
    proxied_tool_config_provider: DefaultProxiedToolConfigProvider | None
    dbt_cli_config: DbtCliConfig | None
    dbt_codegen_config: DbtCodegenConfig | None
    multi_project_discovery_config_provider: MultiProjectDiscoveryConfigProvider
    discovery_config_provider: DefaultDiscoveryConfigProvider
    multi_project_semantic_layer_config_provider: (
        MultiProjectSemanticLayerConfigProvider
    )
    semantic_layer_config_provider: DefaultSemanticLayerConfigProvider
    admin_api_config_provider: DefaultAdminApiConfigProvider
    credentials_provider: CredentialsProvider
    eliciting_credentials_provider: ElicitingCredentialsProvider
    lsp_config: LspConfig | None


def load_config(enable_proxied_tools: bool = True) -> Config:
    log_settings = DbtMcpLogSettings()  # type: ignore
    configure_logging(
        file_logging=log_settings.file_logging, log_level=log_settings.log_level
    )
    settings = DbtMcpSettings()  # type: ignore

    # Apply persisted config from previous elicitation (env vars still win)
    persistence = ConfigPersistence()
    persisted = persistence.read()
    if settings.dbt_host is None and "dbt_host" in persisted:
        settings.dbt_host = persisted["dbt_host"]

    inner_credentials = CredentialsProvider(settings)
    credentials_provider = ElicitingCredentialsProvider(inner_credentials, persistence)

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

    # Proxied tools still gated on explicit opt-in flag
    proxied_tool_config_provider = None
    if enable_proxied_tools:
        proxied_tool_config_provider = DefaultProxiedToolConfigProvider(
            credentials_provider=inner_credentials
        )

    # Platform providers — always created (they resolve lazily via get_credentials).
    # Providers hold the raw CredentialsProvider (not the wrapper) because:
    # 1. They type-expect CredentialsProvider, not the wrapper
    # 2. _is_multi_project() calls the wrapper first, which elicits + populates
    #    settings, so by the time a provider's get_config() runs, the inner
    #    CredentialsProvider already has valid settings cached.
    admin_api_config_provider = DefaultAdminApiConfigProvider(
        credentials_provider=inner_credentials,
    )
    admin_client = DbtAdminAPIClient(admin_api_config_provider)
    multi_project_discovery_config_provider = MultiProjectDiscoveryConfigProvider(
        credentials_provider=inner_credentials,
        admin_client=admin_client,
    )
    multi_project_semantic_layer_config_provider = (
        MultiProjectSemanticLayerConfigProvider(
            credentials_provider=inner_credentials,
            admin_client=admin_client,
            metrics_related_max=settings.sl_metrics_related_max,
        )
    )
    discovery_config_provider = DefaultDiscoveryConfigProvider(
        credentials_provider=inner_credentials,
    )
    semantic_layer_config_provider = DefaultSemanticLayerConfigProvider(
        credentials_provider=inner_credentials,
        metrics_related_max=settings.sl_metrics_related_max,
    )

    # CLI/codegen/LSP — still conditional (need concrete paths at registration time)
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

    lsp_config = None
    if settings.dbt_project_dir:
        lsp_binary_info = dbt_lsp_binary_info(settings.dbt_lsp_path)
        if lsp_binary_info:
            local_lsp_connection_provider = LocalLSPConnectionProvider(
                lsp_binary_info=lsp_binary_info,
                project_dir=settings.dbt_project_dir,
            )
            lsp_client_provider = LocalLSPClientProvider(
                lsp_connection_provider=local_lsp_connection_provider,
            )
            lsp_config = LspConfig(
                local_lsp_connection_provider=local_lsp_connection_provider,
                lsp_client_provider=lsp_client_provider,
            )

    return Config(
        disable_tools=settings.disable_tools or [],
        enable_tools=settings.enable_tools,
        disabled_toolsets=disabled_toolsets,
        enabled_toolsets=enabled_toolsets,
        proxied_tool_config_provider=proxied_tool_config_provider,
        dbt_cli_config=dbt_cli_config,
        dbt_codegen_config=dbt_codegen_config,
        multi_project_discovery_config_provider=multi_project_discovery_config_provider,
        discovery_config_provider=discovery_config_provider,
        multi_project_semantic_layer_config_provider=multi_project_semantic_layer_config_provider,
        semantic_layer_config_provider=semantic_layer_config_provider,
        admin_api_config_provider=admin_api_config_provider,
        credentials_provider=inner_credentials,
        eliciting_credentials_provider=credentials_provider,
        lsp_config=lsp_config,
    )
