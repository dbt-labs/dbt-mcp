import logging
import os
import select
import shutil
import socket
import subprocess
import sys
import time
from enum import Enum
from pathlib import Path
from typing import Annotated

from authlib.integrations.requests_client import OAuth2Session
from filelock import FileLock
from pydantic import Field, field_validator, model_validator
from pydantic_core.core_schema import ValidationInfo
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from dbt_mcp.config.dbt_project import DbtProjectYaml
from dbt_mcp.config.dbt_yaml import try_read_yaml
from dbt_mcp.config.headers import (
    TokenProvider,
)
from dbt_mcp.oauth.client_id import OAUTH_CLIENT_ID
from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.dbt_platform import (
    DbtPlatformContext,
    dbt_platform_context_from_token_response,
)
from dbt_mcp.oauth.login import login
from dbt_mcp.oauth.token_provider import (
    OAuthTokenProvider,
    StaticTokenProvider,
)
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)

OAUTH_REDIRECT_STARTING_PORT = 6785
DEFAULT_DBT_CLI_TIMEOUT = 60


class AuthenticationMethod(Enum):
    OAUTH = "oauth"
    ENV_VAR = "env_var"


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
    dbt_lsp_path: str | None = Field(None, alias="DBT_LSP_PATH")

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
    enable_mcp_server_metadata: bool = Field(
        False, alias="DBT_MCP_ENABLE_MCP_SERVER_METADATA"
    )

    # Tracking settings
    do_not_track: str | None = Field(None, alias="DO_NOT_TRACK")
    send_anonymous_usage_data: str | None = Field(
        None, alias="DBT_SEND_ANONYMOUS_USAGE_STATS"
    )

    # Developer settings
    file_logging: bool = Field(False, alias="DBT_MCP_SERVER_FILE_LOGGING")
    log_level: str | int | None = Field(None, alias="DBT_MCP_LOG_LEVEL")

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
            f"enable_sql={self.enable_sql}, "
            # everything else
            f"dbt_prod_env_id={self.dbt_prod_env_id}, "
            f"dbt_dev_env_id={self.dbt_dev_env_id}, "
            f"dbt_user_id={self.dbt_user_id}, "
            f"dbt_account_id={self.dbt_account_id}, "
            f"dbt_token={'***redacted***' if self.dbt_token else None}, "
            f"send_anonymous_usage_data={self.send_anonymous_usage_data}, "
            f"file_logging={self.file_logging})"
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

    @model_validator(mode="after")
    def apply_cli_defaults(self) -> "DbtMcpSettings":
        """Apply sensible defaults for dbt CLI settings when env vars are not set.

        If DBT_PATH is not set (or is the default "dbt"), attempt to find dbt on PATH.
        If DBT_PROJECT_DIR is not set, attempt to use the current working directory
        if it appears to be a valid dbt project (validated with `dbt parse`).
        """
        # Skip if CLI is explicitly disabled
        if self.disable_dbt_cli:
            return self

        # Resolve dbt_path if it's the default "dbt" or not set
        resolved_dbt_path = self.dbt_path
        if self.dbt_path in ["dbt", "dbtf"] or not self.dbt_path:
            resolved = _resolve_dbt_path_default()
            if resolved:
                resolved_dbt_path = resolved
                object.__setattr__(self, "dbt_path", resolved)
                logger.info(f"Auto-detected dbt executable at: {resolved}")

        # Resolve dbt_project_dir if not set
        if not self.dbt_project_dir:
            resolved_dir = _resolve_project_dir_default(resolved_dbt_path)
            if resolved_dir:
                object.__setattr__(self, "dbt_project_dir", resolved_dir)
                logger.info(f"Auto-detected dbt project directory: {resolved_dir}")

        return self

    @model_validator(mode="after")
    def auto_disable(self) -> "DbtMcpSettings":
        """Auto-disable features based on required settings."""
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


def _is_context_complete(dbt_ctx: DbtPlatformContext | None) -> bool:
    """Check if the context has all required fields (regardless of token expiry).

    Note: dev_environment is optional since not all projects have a development
    environment configured. prod_environment is required for semantic layer
    and other core features.
    """
    return bool(
        dbt_ctx
        and dbt_ctx.account_id
        and dbt_ctx.host_prefix
        and dbt_ctx.prod_environment
        and dbt_ctx.decoded_access_token
    )


def _is_token_valid(dbt_ctx: DbtPlatformContext) -> bool:
    """Check if the access token is still valid (not expired)."""
    if not dbt_ctx.decoded_access_token:
        return False
    expires_at = dbt_ctx.decoded_access_token.access_token_response.expires_at
    return expires_at > time.time() + 120  # 2 minutes buffer


def _try_refresh_token(
    dbt_ctx: DbtPlatformContext,
    dbt_platform_url: str,
    dbt_platform_context_manager: DbtPlatformContextManager,
) -> DbtPlatformContext | None:
    """
    Attempt to refresh the access token using the refresh token.
    Returns the updated context if successful, None otherwise.
    """
    if not dbt_ctx.decoded_access_token:
        return None

    refresh_token = dbt_ctx.decoded_access_token.access_token_response.refresh_token
    if not refresh_token:
        return None

    try:
        logger.info("Access token expired, attempting refresh using refresh token")
        token_url = f"{dbt_platform_url}/oauth/token"
        oauth_client = OAuth2Session(
            client_id=OAUTH_CLIENT_ID,
            token_endpoint=token_url,
        )
        token_response = oauth_client.refresh_token(
            url=token_url,
            refresh_token=refresh_token,
        )
        new_context = dbt_platform_context_from_token_response(
            token_response, dbt_platform_url
        )
        # Merge the new token with the existing context (preserves account/env info)
        updated_context = dbt_ctx.override(new_context)
        dbt_platform_context_manager.write_context_to_file(updated_context)
        logger.info("Successfully refreshed access token at startup")
        return updated_context
    except Exception as e:
        logger.warning(f"Failed to refresh token at startup: {e}")
        return None


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

        # If context is complete, check token validity
        if _is_context_complete(dbt_ctx):
            assert dbt_ctx is not None  # for type checker
            # If token is still valid, use context directly
            if _is_token_valid(dbt_ctx):
                return dbt_ctx
            # Token expired, try to refresh
            refreshed_ctx = _try_refresh_token(
                dbt_ctx, dbt_platform_url, dbt_platform_context_manager
            )
            if refreshed_ctx:
                return refreshed_ctx

        # Fall back to full OAuth login flow
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
        else:
            dbt_path = Path(settings.dbt_path)
            if not (dbt_path.exists() or shutil.which(dbt_path)):
                errors.append(
                    f"DBT_PATH executable can't be found: {settings.dbt_path}"
                )
    return errors


def _validate_dbt_project_with_parse(
    dbt_path: str, project_dir: str, timeout: int = 30
) -> bool:
    """
    Validate that the given directory is a valid dbt project by running `dbt parse`.

    Runs `dbt parse` and checks if the output indicates the directory is not a dbt project.
    The process is terminated early once we can determine success or failure.

    Only returns False for errors that indicate the project directory itself is invalid
    (no dbt_project.yml found, not a dbt project dir). Other errors (e.g., syntax errors
    in models) still indicate a valid project directory.

    Args:
        dbt_path: Path to the dbt executable
        project_dir: Directory to validate as a dbt project
        timeout: Maximum seconds to wait for validation (default 30)

    Returns:
        True if the directory is (or contains) a valid dbt project, False otherwise
    """
    try:
        process = subprocess.Popen(
            [dbt_path, "parse", "--project-dir", project_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            text=True,
            cwd=project_dir,
        )

        # Read output line by line to detect success or failure early
        output_lines: list[str] = []
        start_time = time.time()

        try:
            while True:
                # Check timeout
                if time.time() - start_time > timeout:
                    logger.debug(f"dbt parse validation timed out after {timeout}s")
                    process.terminate()
                    process.wait(timeout=5)
                    # Timeout without "not a project" error - assume valid
                    return True

                # Check if process has ended
                if process.poll() is not None:
                    break

                # Use select on Unix to avoid blocking indefinitely
                if sys.platform != "win32":
                    ready, _, _ = select.select(
                        [process.stdout],
                        [],
                        [],
                        1.0,  # type: ignore[list-item]
                    )
                    if not ready:
                        continue

                line = process.stdout.readline()  # type: ignore[union-attr]
                if not line:
                    break

                output_lines.append(line)
                line_lower = line.lower()

                # Check for specific "not a project" indicators only
                # dbt Core: "No dbt_project.yml found" or similar
                # dbt Fusion: "not a dbt project" or "is not a dbt project dir"
                # These indicate the directory itself is wrong, not just a parsing error
                if (
                    "no dbt_project.yml" in line_lower
                    or "not a dbt project" in line_lower
                    or "is not a dbt project" in line_lower
                ):
                    process.terminate()
                    process.wait(timeout=5)
                    return False

                # Success indicators - dbt found the project and is parsing
                # "found X models" or similar indicates parsing is happening
                # Note: "Running with dbt=" is printed before validation, so don't use it
                if "found" in line_lower and any(
                    word in line_lower
                    for word in ["model", "source", "test", "seed", "snapshot"]
                ):
                    process.terminate()
                    process.wait(timeout=5)
                    return True

        except Exception:
            pass
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()

        # If process completed successfully, it's a valid project
        return_code = process.poll()
        if return_code is not None and return_code == 0:
            return True

        # Check accumulated output for "not a project" indicators
        full_output = "".join(output_lines).lower()
        if (
            "no dbt_project.yml" in full_output
            or "not a dbt project" in full_output
            or "is not a dbt project" in full_output
        ):
            return False

        # If we got output but no "not a project" errors, assume the directory is valid
        # (other errors like syntax errors still mean the project dir is correct)
        if output_lines:
            return True

        return False

    except FileNotFoundError:
        # dbt executable not found
        return False
    except Exception as e:
        logger.debug(f"Error validating dbt project with parse: {e}")
        return False


def _resolve_dbt_path_default() -> str | None:
    """
    Attempt to find a dbt executable on the system PATH.

    Returns:
        The full path to the dbt executable, or None if not found
    """
    # Try common dbt executable names
    for executable in ["dbt", "dbtf"]:
        resolved = shutil.which(executable)
        if resolved:
            logger.debug(f"Resolved dbt executable: {resolved}")
            return resolved
    return None


def _resolve_project_dir_default(dbt_path: str | None) -> str | None:
    """
    Attempt to use the current working directory as the dbt project directory.

    Validates the directory by running `dbt parse`, which will find dbt_project.yml
    even if it's in a parent directory.

    Args:
        dbt_path: Path to the dbt executable (required for validation)

    Returns:
        The current working directory if it's a valid dbt project, None otherwise
    """
    if not dbt_path:
        # Can't validate without dbt
        return None

    cwd = os.getcwd()

    # Validate with dbt parse - this will find dbt_project.yml in parent dirs too
    if _validate_dbt_project_with_parse(dbt_path, cwd):
        logger.debug(f"Validated {cwd} as dbt project directory using dbt parse")
        return cwd
    else:
        logger.debug(f"dbt parse validation failed for {cwd}")
        return None


class CredentialsProvider:
    def __init__(self, settings: DbtMcpSettings):
        self.settings = settings
        self.token_provider: TokenProvider | None = None
        self.authentication_method: AuthenticationMethod | None = None

    def _log_settings(self) -> None:
        settings = self.settings.model_dump()
        if settings.get("dbt_token") is not None:
            settings["dbt_token"] = "***redacted***"
        logger.info(f"Settings: {settings}")

    async def get_credentials(self) -> tuple[DbtMcpSettings, TokenProvider]:
        if self.token_provider is not None:
            # If token provider is already set, just return the cached values
            return self.settings, self.token_provider
        # Load settings from environment variables using pydantic_settings
        dbt_platform_errors = validate_dbt_platform_settings(self.settings)
        if dbt_platform_errors:
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
            self.authentication_method = AuthenticationMethod.OAUTH
            self._log_settings()
            return self.settings, self.token_provider
        self.token_provider = StaticTokenProvider(token=self.settings.dbt_token)
        validate_settings(self.settings)
        self.authentication_method = AuthenticationMethod.ENV_VAR
        self._log_settings()
        return self.settings, self.token_provider
