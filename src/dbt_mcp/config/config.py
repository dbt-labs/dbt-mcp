import os
import socket
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from dbt_mcp.oauth.login import login
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.dbt_cli.binary_type import BinaryType, detect_binary_type

OAUTH_REDIRECT_STARTING_PORT = 6785
OAUTH_CLIENT_ID = "34ec61e834cdffd9dd90a32231937821"


class TrackingConfig(BaseModel):
    host: str | None = None
    host_prefix: str | None = None
    prod_environment_id: int | None = None
    dev_environment_id: int | None = None
    dbt_cloud_user_id: int | None = None
    local_user_id: str | None = None


class SemanticLayerConfig(BaseModel):
    url: str
    host: str
    prod_environment_id: int
    service_token: str
    headers: dict[str, str]


class DiscoveryConfig(BaseModel):
    url: str
    headers: dict[str, str]
    environment_id: int


class DbtCliConfig(BaseModel):
    project_dir: str
    dbt_path: str
    dbt_cli_timeout: int
    binary_type: BinaryType


class SqlConfig(BaseModel):
    host_prefix: str | None = None
    host: str
    user_id: int
    dev_environment_id: int
    prod_environment_id: int
    token: str


class AdminApiConfig(BaseModel):
    url: str
    headers: dict[str, str]
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


class Config(BaseModel):
    tracking_config: TrackingConfig
    sql_config: SqlConfig | None = None
    dbt_cli_config: DbtCliConfig | None = None
    discovery_config: DiscoveryConfig | None = None
    semantic_layer_config: SemanticLayerConfig | None = None
    admin_api_config: AdminApiConfig | None = None
    disable_tools: list[ToolName]


def _create_mcp_yml(dbt_profiles_dir: str | None = None) -> Path:
    # Respect DBT_PROFILES_DIR if set; otherwise default to ~/.dbt/mcp.yml
    if dbt_profiles_dir:
        config_dir = Path(dbt_profiles_dir).expanduser()
    else:
        config_dir = Path.home() / ".dbt"
    config_location = config_dir / "mcp.yml"
    config_location.parent.mkdir(parents=True, exist_ok=True)
    config_location.touch()
    return config_location


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


def load_config() -> Config:
    # Load settings from environment variables using pydantic_settings
    settings = DbtMcpSettings()  # type: ignore[call-arg]
    dbt_platform_errors = validate_dbt_platform_settings(settings)
    # Oauth is exerimental but secure, so you shouldn't use it,
    # but there are no security concerns if you do.
    enable_oauth = os.environ.get("ENABLE_EXPERIMENAL_SECURE_OAUTH") == "true"
    if enable_oauth and dbt_platform_errors:
        config_location = _create_mcp_yml(dbt_profiles_dir=settings.dbt_profiles_dir)
        actual_host = settings.actual_host
        if not actual_host:
            raise ValueError("DBT_HOST is a required environment variable")

        # Find an available port for the local OAuth redirect server
        selected_port = _find_available_port(start_port=OAUTH_REDIRECT_STARTING_PORT)
        login_result = login(
            dbt_platform_url=f"https://{actual_host}",
            port=selected_port,
            client_id=OAUTH_CLIENT_ID,
            config_location=config_location,
        )

        # Override settings with settings attained from login
        dbt_platform_context = login_result.dbt_platform_context
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
        settings.dbt_token = (
            login_result.decoded_access_token.access_token_response.access_token
        )
        settings.host_prefix = dbt_platform_context.host_prefix
        host_prefix_with_period = f"{dbt_platform_context.host_prefix}."
        if not actual_host.startswith(host_prefix_with_period):
            raise ValueError(
                f"The DBT_HOST environment variable is expected to start with the {dbt_platform_context.host_prefix} custom subdomain."
            )
        # We have to remove the custom subdomain prefix
        # so that the metadata and semantic-layer URLs can be constructed correctly.
        settings.dbt_host = actual_host.removeprefix(host_prefix_with_period)
    return create_config(settings)


def create_config(settings: DbtMcpSettings) -> Config:
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
        sql_config = SqlConfig(
            host_prefix=settings.actual_host_prefix,
            user_id=settings.dbt_user_id,
            token=settings.dbt_token,
            dev_environment_id=settings.dbt_dev_env_id,
            prod_environment_id=settings.actual_prod_environment_id,
            host=settings.actual_host,
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
            headers={"Authorization": f"Bearer {settings.dbt_token}"},
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
            headers={
                "Authorization": f"Bearer {settings.dbt_token}",
                "Content-Type": "application/json",
            },
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
            headers={
                "Authorization": f"Bearer {settings.dbt_token}",
                "x-dbt-partner-source": "dbt-mcp",
            },
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
        sql_config=sql_config,
        dbt_cli_config=dbt_cli_config,
        discovery_config=discovery_config,
        semantic_layer_config=semantic_layer_config,
        admin_api_config=admin_api_config,
        disable_tools=settings.disable_tools or [],
    )
