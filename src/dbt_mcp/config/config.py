import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field

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
from dbt_mcp.config.settings import (
    DbtMcpLogSettings,
    DbtMcpSettings,
)
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.dbt_cli.binary_type import BinaryType, detect_binary_type, get_dbt_version
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
class AppsConfig:
    cdn_base: str


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
    lsp_config: LspConfig | None
    apps_config: AppsConfig
    # Lazy: invoking the provider runs `dbt --version`, which can take several
    # seconds when many adapters are installed. Resolution is deferred until a
    # product_docs tool actually needs the version, then cached for the
    # lifetime of the closure.
    dbt_version_provider: Callable[[], str | None] = field(
        default_factory=lambda: lambda: None
    )


def _make_dbt_version_provider(
    *,
    dbt_path: str | None,
    binary_type: BinaryType | None,
) -> Callable[[], str | None]:
    """Return a callable that resolves the dbt version on first invocation
    and caches the result for the lifetime of the closure.

    The subprocess is *not* run until the provider is called, so importing
    this module and constructing a ``Config`` never blocks on
    ``dbt --version`` -- which can take several seconds on installs with
    many adapters.
    """
    if not dbt_path or binary_type is None or binary_type == BinaryType.DBT_CLOUD_CLI:
        return lambda: None

    lock = threading.Lock()
    cache: dict[str, str | None] = {}

    def provider() -> str | None:
        with lock:
            if "value" not in cache:
                cache["value"] = get_dbt_version(dbt_path, binary_type)
            return cache["value"]

    return provider


def load_config(enable_proxied_tools: bool = True) -> Config:
    log_settings = DbtMcpLogSettings()  # type: ignore
    configure_logging(
        file_logging=log_settings.file_logging, log_level=log_settings.log_level
    )
    settings = DbtMcpSettings()  # type: ignore

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

    # Proxied tools still gated on explicit opt-in flag
    proxied_tool_config_provider = None
    if enable_proxied_tools:
        proxied_tool_config_provider = DefaultProxiedToolConfigProvider(
            credentials_provider=credentials_provider
        )

    admin_api_config_provider = DefaultAdminApiConfigProvider(
        credentials_provider=credentials_provider,
    )
    admin_client = DbtAdminAPIClient(admin_api_config_provider)
    multi_project_discovery_config_provider = MultiProjectDiscoveryConfigProvider(
        credentials_provider=credentials_provider,
        admin_client=admin_client,
    )
    multi_project_semantic_layer_config_provider = (
        MultiProjectSemanticLayerConfigProvider(
            credentials_provider=credentials_provider,
            admin_client=admin_client,
            metrics_related_max=settings.sl_metrics_related_max,
            max_response_chars=settings.sl_metrics_max_response_chars,
        )
    )
    discovery_config_provider = DefaultDiscoveryConfigProvider(
        credentials_provider=credentials_provider,
    )
    semantic_layer_config_provider = DefaultSemanticLayerConfigProvider(
        credentials_provider=credentials_provider,
        metrics_related_max=settings.sl_metrics_related_max,
        max_response_chars=settings.sl_metrics_max_response_chars,
    )

    # Detect binary type once (needed for CLI, codegen, and version detection)
    binary_type: BinaryType | None = None
    if settings.dbt_project_dir and settings.dbt_path:
        binary_type = detect_binary_type(settings.dbt_path)

    # CLI/codegen/LSP — still conditional (need concrete paths at registration time)
    dbt_cli_config = None
    if settings.dbt_project_dir and settings.dbt_path and binary_type is not None:
        dbt_cli_config = DbtCliConfig(
            project_dir=settings.dbt_project_dir,
            dbt_path=settings.dbt_path,
            dbt_cli_timeout=settings.dbt_cli_timeout,
            binary_type=binary_type,
        )

    dbt_codegen_config = None
    if settings.dbt_project_dir and settings.dbt_path and binary_type is not None:
        dbt_codegen_config = DbtCodegenConfig(
            project_dir=settings.dbt_project_dir,
            dbt_path=settings.dbt_path,
            dbt_cli_timeout=settings.dbt_cli_timeout,
            binary_type=binary_type,
        )

    lsp_config = None
    if settings.dbt_project_dir:
        lsp_binary_info = dbt_lsp_binary_info(
            lsp_path=settings.dbt_lsp_path, dbt_path=settings.dbt_path
        )
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

    # Version detection requires both a project dir and a dbt binary, and
    # excludes dbt Cloud CLI because its --version reports the Go wrapper
    # version. The provider defers the actual `dbt --version` subprocess
    # until the first product_docs tool call so startup is never blocked.
    dbt_version_provider = _make_dbt_version_provider(
        dbt_path=settings.dbt_path if settings.dbt_project_dir else None,
        binary_type=binary_type,
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
        credentials_provider=credentials_provider,
        lsp_config=lsp_config,
        apps_config=AppsConfig(cdn_base=settings.cdn_base),
        dbt_version_provider=dbt_version_provider,
    )
