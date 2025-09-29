import os
import socket
import time
from pathlib import Path
from typing import Annotated

from filelock import FileLock
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from dbt_mcp.config.dbt_project import DbtProjectYaml
from dbt_mcp.config.headers import (
    TokenProvider,
)
from dbt_mcp.config.yaml import try_read_yaml
from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.dbt_platform import DbtPlatformContext
from dbt_mcp.oauth.login import login
from dbt_mcp.oauth.token_provider import (
    OAuthTokenProvider,
    StaticTokenProvider,
)
from dbt_mcp.tools.tool_names import ToolName

OAUTH_REDIRECT_STARTING_PORT = 6785
DEFAULT_DBT_CLI_TIMEOUT = 60


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
    dbt_cli_timeout: int = Field(DEFAULT_DBT_CLI_TIMEOUT, alias="DBT_CLI_TIMEOUT")
    dbt_warn_error_options: str | None = Field(None, alias="DBT_WARN_ERROR_OPTIONS")
    dbt_profiles_dir: str | None = Field(None, alias="DBT_PROFILES_DIR")

    # Disable tool settings
    disable_dbt_cli: bool = Field(False, alias="DISABLE_DBT_CLI")
    disable_dbt_codegen: bool = Field(True, alias="DISABLE_DBT_CODEGEN")
    disable_semantic_layer: bool = Field(False, alias="DISABLE_SEMANTIC_LAYER")
    disable_discovery: bool = Field(False, alias="DISABLE_DISCOVERY")
    disable_remote: bool | None = Field(None, alias="DISABLE_REMOTE")
    disable_admin_api: bool | None = Field(False, alias="DISABLE_ADMIN_API")
    disable_sql: bool | None = Field(None, alias="DISABLE_SQL")
    disable_tools: Annotated[list[ToolName] | None, NoDecode] = Field(
        None, alias="DISABLE_TOOLS"
    )

    # Tracking settings
    do_not_track: str | None = Field(None, alias="DO_NOT_TRACK")
    send_anonymous_usage_data: str | None = Field(
        None, alias="DBT_SEND_ANONYMOUS_USAGE_STATS"
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

    @property
    def dbt_project_yml(self) -> DbtProjectYaml | None:
        if not self.dbt_project_dir:
            return None
        dbt_project_yml = try_read_yaml(Path(self.dbt_project_dir) / "dbt_project.yml")
        if dbt_project_yml is None:
            return None
        return DbtProjectYaml.model_validate(dbt_project_yml)

    @property
    def usage_tracking_enabled(self) -> bool:
        # dbt environment variables take precedence over dbt_project.yml
        if (
            self.send_anonymous_usage_data is not None
            and (
                self.send_anonymous_usage_data.lower() == "false"
                or self.send_anonymous_usage_data == "0"
            )
        ) or (
            self.do_not_track is not None
            and (self.do_not_track.lower() == "true" or self.do_not_track == "1")
        ):
            return False
        dbt_project_yml = self.dbt_project_yml
        if (
            dbt_project_yml
            and dbt_project_yml.flags
            and dbt_project_yml.flags.send_anonymous_usage_stats is not None
        ):
            return dbt_project_yml.flags.send_anonymous_usage_stats
        return True

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


def get_dbt_profiles_path(dbt_profiles_dir: str | None = None) -> Path:
    # Respect DBT_PROFILES_DIR if set; otherwise default to ~/.dbt/mcp.yml
    if dbt_profiles_dir:
        return Path(dbt_profiles_dir).expanduser()
    else:
        return Path.home() / ".dbt"


async def get_dbt_platform_context(
    *,
    dbt_user_dir: Path,
    dbt_platform_url: str,
    dbt_platform_context_manager: DbtPlatformContextManager,
) -> DbtPlatformContext:
    # Some MCP hosts (Claude Desktop) tend to run multiple MCP servers instances.
    # We need to lock so that only one can run the oauth flow.
    with FileLock(dbt_user_dir / "mcp.lock"):
        dbt_ctx = dbt_platform_context_manager.read_context()
        if (
            dbt_ctx
            and dbt_ctx.account_id
            and dbt_ctx.host_prefix
            and dbt_ctx.dev_environment
            and dbt_ctx.prod_environment
            and dbt_ctx.decoded_access_token
            and dbt_ctx.decoded_access_token.access_token_response.expires_at
            > time.time() + 120  # 2 minutes buffer
        ):
            return dbt_ctx
        # Find an available port for the local OAuth redirect server
        selected_port = _find_available_port(start_port=OAUTH_REDIRECT_STARTING_PORT)
        return await login(
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


def validate_settings(settings: DbtMcpSettings):
    errors: list[str] = []
    errors.extend(validate_dbt_platform_settings(settings))
    errors.extend(validate_dbt_cli_settings(settings))
    if errors:
        raise ValueError("Errors found in configuration:\n\n" + "\n".join(errors))


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


class CredentialsProvider:
    def __init__(self, settings: DbtMcpSettings):
        self.settings = settings
        self.token_provider: TokenProvider | None = None

    async def get_credentials(self) -> tuple[DbtMcpSettings, TokenProvider]:
        if self.token_provider is not None:
            # If token provider is already set, just return the cached values
            return self.settings, self.token_provider
        # Load settings from environment variables using pydantic_settings
        dbt_platform_errors = validate_dbt_platform_settings(self.settings)
        # Oauth is exerimental but secure, so you shouldn't use it,
        # but there are no security concerns if you do.
        enable_oauth = os.environ.get("ENABLE_EXPERIMENAL_SECURE_OAUTH") == "true"
        if enable_oauth and dbt_platform_errors:
            dbt_user_dir = get_dbt_profiles_path(
                dbt_profiles_dir=self.settings.dbt_profiles_dir
            )
            config_location = dbt_user_dir / "mcp.yml"
            dbt_platform_url = f"https://{self.settings.actual_host}"
            dbt_platform_context_manager = DbtPlatformContextManager(config_location)
            dbt_platform_context = await get_dbt_platform_context(
                dbt_platform_context_manager=dbt_platform_context_manager,
                dbt_user_dir=dbt_user_dir,
                dbt_platform_url=dbt_platform_url,
            )

            # Override settings with settings attained from login or mcp.yml
            self.settings.dbt_user_id = dbt_platform_context.user_id
            self.settings.dbt_dev_env_id = (
                dbt_platform_context.dev_environment.id
                if dbt_platform_context.dev_environment
                else None
            )
            self.settings.dbt_prod_env_id = (
                dbt_platform_context.prod_environment.id
                if dbt_platform_context.prod_environment
                else None
            )
            self.settings.dbt_account_id = dbt_platform_context.account_id
            self.settings.host_prefix = dbt_platform_context.host_prefix
            self.settings.dbt_host = get_dbt_host(self.settings, dbt_platform_context)
            if not dbt_platform_context.decoded_access_token:
                raise ValueError("No decoded access token found in OAuth context")
            self.settings.dbt_token = dbt_platform_context.decoded_access_token.access_token_response.access_token

            self.token_provider = OAuthTokenProvider(
                access_token_response=dbt_platform_context.decoded_access_token.access_token_response,
                dbt_platform_url=dbt_platform_url,
                context_manager=dbt_platform_context_manager,
            )
            validate_settings(self.settings)
            return self.settings, self.token_provider
        self.token_provider = StaticTokenProvider(token=self.settings.dbt_token)
        validate_settings(self.settings)
        return self.settings, self.token_provider
