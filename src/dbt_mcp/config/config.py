import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import yaml
from filelock import FileLock
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from dbt_mcp.config.headers import (
    AdminApiHeadersProvider,
    DiscoveryHeadersProvider,
    HeadersProvider,
    SemanticLayerHeadersProvider,
    SqlHeadersProvider,
    TokenProvider,
)
from dbt_mcp.dbt_cli.binary_type import BinaryType, detect_binary_type
from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.dbt_platform import DbtPlatformContext
from dbt_mcp.oauth.login import login
from dbt_mcp.oauth.token_provider import OAuthTokenProvider, StaticTokenProvider
from dbt_mcp.tools.tool_names import ToolName

OAUTH_REDIRECT_STARTING_PORT = 6785


@dataclass
class TrackingConfig:
    host: str | None = None
    host_prefix: str | None = None
    prod_environment_id: int | None = None
    dev_environment_id: int | None = None
    dbt_cloud_user_id: int | None = None
    local_user_id: str | None = None


@dataclass
class SemanticLayerConfig:
    url: str
    host: str
    prod_environment_id: int
    service_token: str
    headers_provider: HeadersProvider


@dataclass
class DiscoveryConfig:
    url: str
    headers_provider: HeadersProvider
    environment_id: int


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
class AdminApiConfig:
    url: str
    headers_provider: HeadersProvider
    account_id: int
    prod_environment_id: int | None = None


class DbtMcpSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # dbt Platform settings
    dbt_host: str | None = Field(None, alias="DBT_HOST")
    dbt_mcp_host: str | None = Field(None, alias="DBT_MCP_HOST")
    dbt_prod_env_id: int | None = Field(None, alias="DBT_PROD_ENV_ID")
    dbt_env_id: int | None = Field(None, alias="DBT_ENV_ID")  # legacy support
    dbt_dev_env_id: int | None = Field(None, alias="DBT_DEV_ENV_ID")
    dbt_user_id: int | None = Field(None, alias="DBT_USER_ID")
    dbt_account_id: int | None = Field(None, alias="DBT_ACCOUNT_ID")
    dbt_token: str | None = Field(None, alias="DBT_TOKEN")
    multicell_account_prefix: str | None = Field(None, alias="MULTICELL_ACCOUNT_PREFIX")
    host_prefix: str | None = Field(None, alias="DBT_HOST_PREFIX")

    # dbt CLI settings
    dbt_project_dir: str | None = Field(None, alias="DBT_PROJECT_DIR")
    dbt_path: str = Field("dbt", alias="DBT_PATH")
    dbt_cli_timeout: int = Field(10, alias="DBT_CLI_TIMEOUT")
    dbt_warn_error_options: str | None = Field(None, alias="DBT_WARN_ERROR_OPTIONS")
    dbt_profiles_dir: str | None = Field(None, alias="DBT_PROFILES_DIR")

    # Disable tool settings
    disable_dbt_cli: bool = Field(False, alias="DISABLE_DBT_CLI")
    disable_semantic_layer: bool = Field(False, alias="DISABLE_SEMANTIC_LAYER")
    disable_discovery: bool = Field(False, alias="DISABLE_DISCOVERY")
    disable_remote: bool | None = Field(None, alias="DISABLE_REMOTE")
    disable_admin_api: bool | None = Field(False, alias="DISABLE_ADMIN_API")
    disable_sql: bool | None = Field(None, alias="DISABLE_SQL")
    disable_tools: Annotated[list[ToolName] | None, NoDecode] = Field(
        None, alias="DISABLE_TOOLS"
    )

    @property
    def actual_host(self) -> str | None:
        host = self.dbt_host or self.dbt_mcp_host
        if host is None:
            return None
        return host.rstrip("/")

    @property
    def actual_prod_environment_id(self) -> int | None:
        return self.dbt_prod_env_id or self.dbt_env_id

    @property
    def actual_disable_sql(self) -> bool:
        if self.disable_sql is not None:
            return self.disable_sql
        if self.disable_remote is not None:
            return self.disable_remote
        return True

    @property
    def actual_host_prefix(self) -> str | None:
        if self.host_prefix is not None:
            return self.host_prefix
        if self.multicell_account_prefix is not None:
            return self.multicell_account_prefix
        return None

    @field_validator("disable_tools", mode="before")
    @classmethod
    def parse_disable_tools(cls, env_var: str | None) -> list[ToolName]:
        if not env_var:
            return []
        errors: list[str] = []
        tool_names: list[ToolName] = []
        for tool_name in env_var.split(","):
            tool_name_stripped = tool_name.strip()
            if tool_name_stripped == "":
                continue
            try:
                tool_names.append(ToolName(tool_name_stripped))
            except ValueError:
                errors.append(
                    f"Invalid tool name in DISABLE_TOOLS: {tool_name_stripped}."
                    + " Must be a valid tool name."
                )
        if errors:
            raise ValueError("\n".join(errors))
        return tool_names


@dataclass
class Config:
    tracking_config: TrackingConfig
    disable_tools: list[ToolName]
    sql_config: SqlConfig | None
    dbt_cli_config: DbtCliConfig | None
    discovery_config: DiscoveryConfig | None
    semantic_layer_config: SemanticLayerConfig | None
    admin_api_config: AdminApiConfig | None
    token_provider: TokenProvider


def _get_dbt_user_dir(dbt_profiles_dir: str | None = None) -> Path:
    # Respect DBT_PROFILES_DIR if set; otherwise default to ~/.dbt/mcp.yml
    if dbt_profiles_dir:
        return Path(dbt_profiles_dir).expanduser()
    else:
        return Path.home() / ".dbt"


def _find_available_port(*, start_port: int, max_attempts: int = 20) -> int:
    """
    Return the first available port on 127.0.0.1 starting at start_port.

    Raises RuntimeError if no port is found within the attempted range.
    """
    for candidate_port in range(start_port, start_port + max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", candidate_port))
            except OSError:
                continue
            return candidate_port
    raise RuntimeError(
        "No available port found starting at "
        f"{start_port} after {max_attempts} attempts."
    )


def validate_settings(settings: DbtMcpSettings) -> list[str]:
    errors: list[str] = []
    errors.extend(validate_dbt_platform_settings(settings))
    errors.extend(validate_dbt_cli_settings(settings))
    return errors


def validate_dbt_platform_settings(
    settings: DbtMcpSettings,
) -> list[str]:
    errors: list[str] = []
    if (
        not settings.disable_semantic_layer
        or not settings.disable_discovery
        or not settings.actual_disable_sql
        or not settings.disable_admin_api
    ):
        if not settings.actual_host:
            errors.append(
                "DBT_HOST environment variable is required when semantic layer, discovery, SQL or admin API tools are enabled."
            )
        if not settings.actual_prod_environment_id:
            errors.append(
                "DBT_PROD_ENV_ID environment variable is required when semantic layer, discovery, SQL or admin API tools are enabled."
            )
        if not settings.dbt_token:
            errors.append(
                "DBT_TOKEN environment variable is required when semantic layer, discovery, SQL or admin API tools are enabled."
            )
        if settings.actual_host and (
            settings.actual_host.startswith("metadata")
            or settings.actual_host.startswith("semantic-layer")
        ):
            errors.append(
                "DBT_HOST must not start with 'metadata' or 'semantic-layer'."
            )
    if (
        not settings.actual_disable_sql
        and ToolName.TEXT_TO_SQL not in (settings.disable_tools or [])
        and not settings.actual_prod_environment_id
    ):
        errors.append(
            "DBT_PROD_ENV_ID environment variable is required when text_to_sql is enabled."
        )
    if not settings.actual_disable_sql and ToolName.EXECUTE_SQL not in (
        settings.disable_tools or []
    ):
        if not settings.dbt_dev_env_id:
            errors.append(
                "DBT_DEV_ENV_ID environment variable is required when execute_sql is enabled."
            )
        if not settings.dbt_user_id:
            errors.append(
                "DBT_USER_ID environment variable is required when execute_sql is enabled."
            )
    return errors


def validate_dbt_cli_settings(settings: DbtMcpSettings) -> list[str]:
    errors: list[str] = []
    if not settings.disable_dbt_cli:
        if not settings.dbt_project_dir:
            errors.append(
                "DBT_PROJECT_DIR environment variable is required when dbt CLI tools are enabled."
            )
        if not settings.dbt_path:
            errors.append(
                "DBT_PATH environment variable is required when dbt CLI tools are enabled."
            )
    return errors


def get_dbt_platform_context(
    *,
    dbt_user_dir: Path,
    dbt_platform_url: str,
    dbt_platform_context_manager: DbtPlatformContextManager,
) -> DbtPlatformContext:
    # Some MCP hosts (Claude Desktop) tend to run multiple MCP servers instances.
    # We need to lock so that only one can run the oauth flow.
    with FileLock(dbt_user_dir / "mcp.lock"):
        if (
            (dbt_ctx := dbt_platform_context_manager.read_context())
            and dbt_ctx.decoded_access_token
            and dbt_ctx.decoded_access_token.access_token_response.expires_at
            > time.time() + 120  # 2 minutes buffer
        ):
            return dbt_ctx
        # Find an available port for the local OAuth redirect server
        selected_port = _find_available_port(start_port=OAUTH_REDIRECT_STARTING_PORT)
        return login(
            dbt_platform_url=dbt_platform_url,
            port=selected_port,
            dbt_platform_context_manager=dbt_platform_context_manager,
        )


def get_dbt_host(
    settings: DbtMcpSettings, dbt_platform_context: DbtPlatformContext
) -> str:
    actual_host = settings.actual_host
    if not actual_host:
        raise ValueError("DBT_HOST is a required environment variable")
    host_prefix_with_period = f"{dbt_platform_context.host_prefix}."
    if not actual_host.startswith(host_prefix_with_period):
        raise ValueError(
            f"The DBT_HOST environment variable is expected to start with the {dbt_platform_context.host_prefix} custom subdomain."
        )
    # We have to remove the custom subdomain prefix
    # so that the metadata and semantic-layer URLs can be constructed correctly.
    return actual_host.removeprefix(host_prefix_with_period)


def load_config() -> Config:
    # Load settings from environment variables using pydantic_settings
    settings = DbtMcpSettings()  # type: ignore[call-arg]
    dbt_platform_errors = validate_dbt_platform_settings(settings)
    token_provider: TokenProvider
    # Oauth is exerimental but secure, so you shouldn't use it,
    # but there are no security concerns if you do.
    enable_oauth = os.environ.get("ENABLE_EXPERIMENAL_SECURE_OAUTH") == "true"
    if enable_oauth and dbt_platform_errors:
        dbt_user_dir = _get_dbt_user_dir(dbt_profiles_dir=settings.dbt_profiles_dir)
        config_location = dbt_user_dir / "mcp.yml"
        dbt_platform_url = f"https://{settings.actual_host}"
        dbt_platform_context_manager = DbtPlatformContextManager(config_location)
        dbt_platform_context = get_dbt_platform_context(
            dbt_platform_context_manager=dbt_platform_context_manager,
            dbt_user_dir=dbt_user_dir,
            dbt_platform_url=dbt_platform_url,
        )

        # Override settings with settings attained from login or mcp.yml
        settings.dbt_user_id = dbt_platform_context.user_id
        settings.dbt_dev_env_id = (
            dbt_platform_context.dev_environment.id
            if dbt_platform_context.dev_environment
            else None
        )
        settings.dbt_prod_env_id = (
            dbt_platform_context.prod_environment.id
            if dbt_platform_context.prod_environment
            else None
        )
        settings.host_prefix = dbt_platform_context.host_prefix
        settings.dbt_host = get_dbt_host(settings, dbt_platform_context)
        if not dbt_platform_context.decoded_access_token:
            raise ValueError("No decoded access token found in OAuth context")
        settings.dbt_token = (
            dbt_platform_context.decoded_access_token.access_token_response.access_token
        )

        token_provider = OAuthTokenProvider(
            access_token_response=dbt_platform_context.decoded_access_token.access_token_response,
            dbt_platform_url=dbt_platform_url,
            context_manager=dbt_platform_context_manager,
        )
    else:
        token_provider = StaticTokenProvider(token=settings.dbt_token)
    return create_config(settings, token_provider)


def create_config(settings: DbtMcpSettings, token_provider: TokenProvider) -> Config:
    # Set default warn error options if not provided
    if settings.dbt_warn_error_options is None:
        warn_error_options = '{"error": ["NoNodesForSelectionCriteria"]}'
        os.environ["DBT_WARN_ERROR_OPTIONS"] = warn_error_options

    errors = validate_settings(settings)
    if errors:
        raise ValueError("Errors found in configuration:\n\n" + "\n".join(errors))

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

    # For admin API tools, we need token, host, and account_id
    admin_api_config = None
    if (
        not settings.disable_admin_api
        and settings.dbt_token
        and settings.actual_host
        and settings.dbt_account_id
    ):
        if settings.actual_host_prefix:
            url = f"https://{settings.actual_host_prefix}.{settings.actual_host}"
        else:
            url = f"https://{settings.actual_host}"
        admin_api_config = AdminApiConfig(
            url=url,
            headers_provider=AdminApiHeadersProvider(token_provider=token_provider),
            account_id=settings.dbt_account_id,
            prod_environment_id=settings.actual_prod_environment_id,
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

    discovery_config = None
    if (
        not settings.disable_discovery
        and settings.actual_host
        and settings.actual_prod_environment_id
        and settings.dbt_token
    ):
        if settings.actual_host_prefix:
            url = f"https://{settings.actual_host_prefix}.metadata.{settings.actual_host}/graphql"
        else:
            url = f"https://metadata.{settings.actual_host}/graphql"
        discovery_config = DiscoveryConfig(
            url=url,
            headers_provider=DiscoveryHeadersProvider(token_provider=token_provider),
            environment_id=settings.actual_prod_environment_id,
        )

    semantic_layer_config = None
    if (
        not settings.disable_semantic_layer
        and settings.actual_host
        and settings.actual_prod_environment_id
        and settings.dbt_token
    ):
        is_local = settings.actual_host and settings.actual_host.startswith("localhost")
        if is_local:
            host = settings.actual_host
        elif settings.actual_host_prefix:
            host = (
                f"{settings.actual_host_prefix}.semantic-layer.{settings.actual_host}"
            )
        else:
            host = f"semantic-layer.{settings.actual_host}"
        assert host is not None

        semantic_layer_config = SemanticLayerConfig(
            url=f"http://{host}" if is_local else f"https://{host}" + "/api/graphql",
            host=host,
            prod_environment_id=settings.actual_prod_environment_id,
            service_token=settings.dbt_token,
            headers_provider=SemanticLayerHeadersProvider(
                token_provider=token_provider
            ),
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
        discovery_config=discovery_config,
        semantic_layer_config=semantic_layer_config,
        admin_api_config=admin_api_config,
        token_provider=token_provider,
    )
