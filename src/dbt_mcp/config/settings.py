import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

from pydantic import Field, field_validator, model_validator
from pydantic_core.core_schema import ValidationInfo
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from dbt_mcp.config.dbt_project import DbtProjectYaml
from dbt_mcp.config.dbt_yaml import try_read_yaml
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)

DEFAULT_DBT_CLI_TIMEOUT = 60


class DbtMcpLogSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    file_logging: bool = Field(False, alias="DBT_MCP_SERVER_FILE_LOGGING")
    log_level: str | int | None = Field(None, alias="DBT_MCP_LOG_LEVEL")

    def __repr__(self):
        return f"DbtMcpLogSettings(file_logging={self.file_logging}, log_level={self.log_level})"


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
    multicell_account_prefix: str | None = Field(
        None, alias="MULTICELL_ACCOUNT_PREFIX"
    )  # legacy support
    host_prefix: str | None = Field(None, alias="DBT_HOST_PREFIX")
    dbt_lsp_path: str | None = Field(None, alias="DBT_LSP_PATH")
    dbt_project_ids: Annotated[list[int] | None, NoDecode] = Field(
        None, alias="DBT_PROJECT_IDS"
    )

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
    disable_admin_api: bool = Field(False, alias="DISABLE_ADMIN_API")
    disable_sql: bool | None = Field(None, alias="DISABLE_SQL")
    disable_tools: Annotated[list[ToolName] | None, NoDecode] = Field(
        None, alias="DISABLE_TOOLS"
    )
    disable_lsp: bool | None = Field(None, alias="DISABLE_LSP")
    disable_product_docs: bool = Field(False, alias="DISABLE_PRODUCT_DOCS")
    disable_mcp_server_metadata: bool = Field(True, alias="DISABLE_MCP_SERVER_METADATA")

    # Enable tool settings (allowlist)
    enable_tools: Annotated[list[ToolName] | None, NoDecode] = Field(
        None, alias="DBT_MCP_ENABLE_TOOLS"
    )
    enable_semantic_layer: bool = Field(False, alias="DBT_MCP_ENABLE_SEMANTIC_LAYER")
    enable_admin_api: bool = Field(False, alias="DBT_MCP_ENABLE_ADMIN_API")
    enable_dbt_cli: bool = Field(False, alias="DBT_MCP_ENABLE_DBT_CLI")
    enable_dbt_codegen: bool = Field(False, alias="DBT_MCP_ENABLE_DBT_CODEGEN")
    enable_discovery: bool = Field(False, alias="DBT_MCP_ENABLE_DISCOVERY")
    enable_lsp: bool = Field(False, alias="DBT_MCP_ENABLE_LSP")
    enable_sql: bool = Field(False, alias="DBT_MCP_ENABLE_SQL")
    enable_product_docs: bool = Field(False, alias="DBT_MCP_ENABLE_PRODUCT_DOCS")
    enable_mcp_server_metadata: bool = Field(
        False, alias="DBT_MCP_ENABLE_MCP_SERVER_METADATA"
    )

    # Tracking settings
    do_not_track: str | None = Field(None, alias="DO_NOT_TRACK")
    send_anonymous_usage_data: str | None = Field(
        None, alias="DBT_SEND_ANONYMOUS_USAGE_STATS"
    )

    # Semantic layer settings
    sl_metrics_related_max: int = Field(
        10, alias="DBT_MCP_SL_METRICS_RELATED_MAX", ge=0
    )
    sl_metrics_max_response_chars: int = Field(
        16000, alias="DBT_MCP_SL_MAX_RESPONSE_CHARS", ge=0
    )

    def __repr__(self):
        """Custom repr to bring most important settings to front. Redact sensitive info."""
        return (
            #  auto-disable settings
            f"DbtMcpSettings(dbt_host={self.dbt_host}, "
            f"dbt_path={self.dbt_path}, "
            f"dbt_project_dir={self.dbt_project_dir}, "
            # disable settings
            f"disable_dbt_cli={self.disable_dbt_cli}, "
            f"disable_dbt_codegen={self.disable_dbt_codegen}, "
            f"disable_semantic_layer={self.disable_semantic_layer}, "
            f"disable_discovery={self.disable_discovery}, "
            f"disable_admin_api={self.disable_admin_api}, "
            f"disable_sql={self.disable_sql}, "
            f"disable_product_docs={self.disable_product_docs}, "
            f"disable_tools={self.disable_tools}, "
            f"disable_lsp={self.disable_lsp}, "
            # enable settings
            f"enable_tools={self.enable_tools}, "
            f"enable_semantic_layer={self.enable_semantic_layer}, "
            f"enable_admin_api={self.enable_admin_api}, "
            f"enable_dbt_cli={self.enable_dbt_cli}, "
            f"enable_dbt_codegen={self.enable_dbt_codegen}, "
            f"enable_discovery={self.enable_discovery}, "
            f"enable_lsp={self.enable_lsp}, "
            f"enable_product_docs={self.enable_product_docs}, "
            f"enable_sql={self.enable_sql}, "
            # everything else
            f"dbt_prod_env_id={self.dbt_prod_env_id}, "
            f"dbt_dev_env_id={self.dbt_dev_env_id}, "
            f"dbt_user_id={self.dbt_user_id}, "
            f"dbt_account_id={self.dbt_account_id}, "
            f"dbt_token={'***redacted***' if self.dbt_token else None}, "
            f"send_anonymous_usage_data={self.send_anonymous_usage_data})"
        )

    @property
    def actual_host(self) -> str | None:
        host = self.dbt_host or self.dbt_mcp_host
        if host is None:
            return None
        return host.rstrip("/").removeprefix("https://").removeprefix("http://")

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
    def base_host(self) -> str | None:
        """Returns actual_host with the account prefix stripped if it's already embedded.

        Prevents double-prefixing when DBT_HOST already contains the account prefix
        (e.g. 'ab123.us1.dbt.com') and a prefix env var is also set to 'ab123'.
        Returns None if and only if actual_host is None.
        On mismatch (host has a different embedded prefix), logs a warning and returns
        the host unchanged to avoid silently stripping the wrong label.
        """
        host = self.actual_host
        if host is None:
            return None
        result = parse_host_prefix(host, self.actual_host_prefix)
        if result.mismatched_prefix is not None:
            logger.warning(
                f"DBT_HOST ('{host}') appears to contain a different account prefix "
                f"('{result.mismatched_prefix}') than the configured prefix '{self.actual_host_prefix}'. "
                "This may result in incorrect URL construction."
            )
            return host
        return result.base_host

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

    @field_validator("dbt_host", "dbt_mcp_host", mode="after")
    @classmethod
    def validate_host(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Intentionally error on misconfigured host-like env vars (DBT_HOST and DBT_MCP_HOST)."""
        host = (
            v.rstrip("/").removeprefix("https://").removeprefix("http://") if v else v
        )

        if host and (host.startswith("metadata") or host.startswith("semantic-layer")):
            field_name = (
                getattr(info, "field_name", "None") if info is not None else "None"
            ).upper()
            raise ValueError(
                f"{field_name} must not start with 'metadata' or 'semantic-layer': {v}"
            )
        return v

    @field_validator("dbt_path", mode="after")
    @classmethod
    def validate_file_exists(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate a path exists in the system.

        This will only fail if the path is explicitly set to a non-existing path.
        It will auto-disable upon model validation if it can't be found AND it's not $PATH.
        """
        # Allow 'dbt' and 'dbtf' as special cases as they're expected to be on PATH
        if v in ["dbt", "dbtf"]:
            return v
        if v:
            p = Path(v).expanduser()
            if p.exists():
                return str(p)

            field_name = (
                getattr(info, "field_name", "None") if info is not None else "None"
            ).upper()
            raise ValueError(f"{field_name} path does not exist: {v}")
        return v

    @field_validator("dbt_project_dir", "dbt_profiles_dir", mode="after")
    @classmethod
    def validate_dir_exists(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate a directory path exists in the system."""
        if v:
            path = Path(v).expanduser()
            if not path.is_dir():
                field_name = (
                    getattr(info, "field_name", "None") if info is not None else "None"
                ).upper()
                raise ValueError(f"{field_name} directory does not exist: {v}")
            return str(path)
        return v

    @field_validator("disable_tools", mode="before")
    @classmethod
    def parse_disable_tools(cls, env_var: str | None) -> list[ToolName] | None:
        return _parse_tool_list(env_var, "DISABLE_TOOLS")

    @field_validator("enable_tools", mode="before")
    @classmethod
    def parse_enable_tools(cls, env_var: str | None) -> list[ToolName] | None:
        return _parse_tool_list(env_var, "DBT_MCP_ENABLE_TOOLS")

    @field_validator("dbt_project_ids", mode="before")
    @classmethod
    def parse_project_ids(cls, v: str | list | None) -> list[int] | None:
        if v is None:
            return None
        if isinstance(v, list):
            if not v:
                raise ValueError("DBT_PROJECT_IDS must contain at least one project id")
            return [int(i) for i in v]
        project_ids = [int(i.strip()) for i in str(v).split(",") if i.strip()]
        if not project_ids:
            raise ValueError("DBT_PROJECT_IDS must contain at least one project id")
        return project_ids

    @model_validator(mode="after")
    def auto_disable(self) -> "DbtMcpSettings":
        """Auto-disable features based on required settings."""
        # Warn once at startup if DBT_HOST already embeds the account prefix
        host = self.actual_host
        prefix = self.actual_host_prefix
        if host and prefix:
            result = parse_host_prefix(host, prefix)
            if result.prefix_embedded:
                prefix_env_var = (
                    "DBT_HOST_PREFIX"
                    if self.host_prefix is not None
                    else "MULTICELL_ACCOUNT_PREFIX"
                )
                logger.warning(
                    f"DBT_HOST ('{host}') already contains the account prefix '{prefix}'. "
                    "The prefix will be stripped to avoid URL duplication. "
                    f"Consider setting DBT_HOST='{result.base_host}' and keeping {prefix_env_var}='{prefix}'."
                )

        # platform features
        if (
            not self.actual_host
        ):  # host is the only truly required setting for platform features
            # object.__setattr__ is used in case we want to set values on a frozen model
            object.__setattr__(self, "disable_semantic_layer", True)
            object.__setattr__(self, "disable_discovery", True)
            object.__setattr__(self, "disable_admin_api", True)
            object.__setattr__(self, "disable_sql", True)

            logger.warning(
                "Platform features have been automatically disabled due to missing DBT_HOST."
            )

        # CLI features
        cli_errors = validate_dbt_cli_settings(self)
        if cli_errors:
            object.__setattr__(self, "disable_dbt_cli", True)
            object.__setattr__(self, "disable_dbt_codegen", True)
            logger.warning(
                f"CLI features have been automatically disabled due to misconfigurations:\n    {'\n    '.join(cli_errors)}."
            )
        return self


def _parse_tool_list(env_var: str | None, field_name: str) -> list[ToolName] | None:
    """Parse comma-separated tool names from environment variable.

    Invalid tool names are logged as warnings and skipped, allowing the
    application to continue with valid tool names only.

    The distinction between None and empty list is important:
    - None: env var not set -> default behavior (all tools enabled)
    - Empty list: env var set but no valid tools -> allowlist mode with nothing allowed

    Args:
        env_var: Comma-separated tool names
        field_name: Name of the field for error messages

    Returns:
        None if env var not set, otherwise list of validated ToolName enums
        (invalid names are skipped)
    """
    if env_var is None:
        return None
    tool_names: list[ToolName] = []
    for tool_name in env_var.split(","):
        tool_name_stripped = tool_name.strip()
        if not tool_name_stripped:
            continue
        try:
            tool_names.append(ToolName(tool_name_stripped.lower()))
        except ValueError:
            logger.warning(
                f"Ignoring invalid tool name in {field_name}: '{tool_name_stripped}'. "
                "Must be a valid tool name."
            )
    return tool_names


@dataclass(frozen=True)
class HostPrefixResult:
    base_host: str  # Host with first label stripped; matches configured prefix when prefix_embedded=True, or suggested base host on mismatch
    prefix_embedded: bool  # True if configured prefix was at start of host
    mismatched_prefix: (
        str | None
    )  # Set if host has 4+ labels with different first label


def parse_host_prefix(host: str, prefix: str | None) -> HostPrefixResult:
    if prefix and host.startswith(f"{prefix}."):
        return HostPrefixResult(
            base_host=host.removeprefix(f"{prefix}."),
            prefix_embedded=True,
            mismatched_prefix=None,
        )
    labels = host.split(".")
    if prefix and len(labels) >= 4 and labels[0] != prefix:
        return HostPrefixResult(
            base_host=".".join(labels[1:]),
            prefix_embedded=False,
            mismatched_prefix=labels[0],
        )
    return HostPrefixResult(
        base_host=host,
        prefix_embedded=False,
        mismatched_prefix=None,
    )


def _build_dbt_platform_url(actual_host: str, actual_host_prefix: str | None) -> str:
    """Build the dbt Platform base URL, prepending the account prefix when needed.

    Handles three cases:
    - Prefix set, not yet in host: prepend it (e.g. 'us1.dbt.com' + 'ab123' → 'https://ab123.us1.dbt.com')
    - Prefix set, already in host: use host as-is (no double prefix)
    - No prefix: use host as-is
    """
    result = parse_host_prefix(actual_host, actual_host_prefix)
    if result.mismatched_prefix is not None:
        raise ValueError(
            f"DBT_HOST ('{actual_host}') appears to already contain an account prefix "
            f"('{result.mismatched_prefix}') that differs from the configured prefix '{actual_host_prefix}'. "
            f"Set DBT_HOST to the base host (e.g. '{result.base_host}') "
            "or update your prefix env var."
        )
    if actual_host_prefix and not result.prefix_embedded:
        return f"https://{actual_host_prefix}.{actual_host}"
    return f"https://{actual_host}"


def validate_settings(settings: DbtMcpSettings) -> None:
    errors: list[str] = []
    errors.extend(validate_dbt_platform_settings(settings))
    errors.extend(validate_dbt_cli_settings(settings))
    if errors:
        raise ValueError("Errors found in configuration:\n\n" + "\n".join(errors))


def validate_dbt_platform_settings(settings: DbtMcpSettings) -> list[str]:
    """Validate platform settings."""
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
        if (
            settings.actual_prod_environment_id is None
            and settings.dbt_project_ids is None
        ):
            errors.append(
                "DBT_PROD_ENV_ID or DBT_PROJECT_IDS environment variable is required when semantic layer, discovery, SQL or admin API tools are enabled."
            )
        if (
            settings.actual_prod_environment_id is not None
            and settings.dbt_project_ids is not None
        ):
            errors.append(
                "DBT_PROD_ENV_ID and DBT_PROJECT_IDS environment variables cannot be set at the same time."
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
        else:
            dbt_path = Path(settings.dbt_path)
            if not (dbt_path.exists() or shutil.which(dbt_path)):
                errors.append(
                    f"DBT_PATH executable can't be found: {settings.dbt_path}"
                )
    return errors
