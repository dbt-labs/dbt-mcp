import asyncio
import logging
import socket
import time
from enum import Enum
from pathlib import Path

from filelock import FileLock

from dbt_mcp.config.config_providers.admin_api import DefaultAdminApiConfigProvider
from dbt_mcp.config.headers import TokenProvider
from dbt_mcp.config.settings import (
    DbtMcpSettings,
    _build_dbt_platform_url,
    validate_dbt_cli_settings,
    validate_dbt_platform_settings,
    validate_settings,
)
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.dbt_platform import DbtPlatformContext
from dbt_mcp.oauth.expiry import STARTUP_EXPIRY_BUFFER_SECONDS
from dbt_mcp.oauth.login import login
from dbt_mcp.oauth.refresh import refresh_oauth_token
from dbt_mcp.oauth.token_provider import (
    OAuthTokenProvider,
    StaticTokenProvider,
)
from dbt_mcp.project.project_resolver import get_account

logger = logging.getLogger(__name__)

OAUTH_REDIRECT_STARTING_PORT = 6785


class AuthenticationMethod(Enum):
    OAUTH = "oauth"
    ENV_VAR = "env_var"


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
    return expires_at > time.time() + STARTUP_EXPIRY_BUFFER_SECONDS


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
        new_context = refresh_oauth_token(
            refresh_token=refresh_token,
            token_url=token_url,
            dbt_platform_url=dbt_platform_url,
        )
        # Merge the new token with the existing context (preserves account/env info)
        updated_context = dbt_ctx.override(new_context)
        dbt_platform_context_manager.write_context_to_file(updated_context)
        logger.info("Successfully refreshed access token at startup")
        return updated_context
    except Exception as e:
        logger.warning(f"Failed to refresh token at startup: {e}")
        return None


async def _fetch_host_prefix_from_platform(
    *,
    dbt_platform_url: str,
    account_id: int,
    token: str,
) -> str | None:
    try:
        account = await get_account(
            dbt_platform_url=dbt_platform_url,
            account_id=account_id,
            headers={
                "Authorization": f"Token {token}",
                "Accept": "application/json",
            },
        )
        return account.host_prefix
    except Exception:
        logger.warning(
            "Failed to fetch host prefix from dbt platform. "
            "If your account requires a prefix, try setting DBT_HOST_PREFIX explicitly."
        )
        return None


async def get_dbt_platform_context(
    *,
    dbt_user_dir: Path,
    dbt_platform_url: str,
    dbt_platform_context_manager: DbtPlatformContextManager,
) -> DbtPlatformContext:
    # Some MCP hosts (Claude Desktop) tend to run multiple MCP servers instances.
    # We need to lock so that only one can run the oauth flow.
    # Resolve the lock file path to handle multi-level symbolic links (see #533).
    lock_path = (dbt_user_dir / "mcp.lock").resolve()
    with FileLock(lock_path):
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


class CredentialsProvider:
    def __init__(self, settings: "DbtMcpSettings") -> None:
        self.settings = settings
        self.token_provider: TokenProvider | None = None
        self.authentication_method: AuthenticationMethod | None = None
        self.account_identifier: str | None = None
        self.user_id: int | None = None

    async def _resolve_account_identifier(self) -> None:
        """Fetch and store the account identifier from the Admin API.

        Fails silently — account_identifier remains None on error.
        """
        if not self.settings.dbt_account_id or not self.settings.actual_host:
            return
        try:
            admin_client = DbtAdminAPIClient(DefaultAdminApiConfigProvider(self))
            account_data = await admin_client.get_account(self.settings.dbt_account_id)
            self.account_identifier = account_data.get("identifier")
        except Exception as e:
            logger.warning(f"Failed to fetch account identifier: {e}")

    async def _resolve_user_id(self) -> None:
        """Fetch and store the current user id.

        Uses settings.dbt_user_id if already populated (e.g. by OAuth JWT);
        otherwise falls back to the Admin API. Fails silently — user_id
        remains None on error.
        """
        if self.settings.dbt_user_id is not None:
            self.user_id = self.settings.dbt_user_id
            return
        if not self.settings.actual_host or not self.token_provider:
            return
        try:
            self.token_provider.get_token()
        except Exception:
            return
        try:
            admin_client = DbtAdminAPIClient(DefaultAdminApiConfigProvider(self))
            user_data = await admin_client.get_current_user()
            id_response = (user_data.get("user") or {}).get("id")
            self.user_id = int(id_response) if id_response is not None else None
        except Exception as e:
            logger.warning(f"Failed to fetch user id: {e}")

    def _log_settings(self) -> None:
        settings = self.settings.model_dump()
        if settings.get("dbt_token") is not None:
            settings["dbt_token"] = "***redacted***"
        logger.info(f"Settings: {settings}")

    async def get_credentials(self) -> "tuple[DbtMcpSettings, TokenProvider]":
        if self.token_provider is not None:
            # If token provider is already set, just return the cached values
            return self.settings, self.token_provider
        # Load settings from environment variables using pydantic_settings
        dbt_platform_errors = validate_dbt_platform_settings(self.settings)
        if dbt_platform_errors:
            if self.settings.dbt_token:
                logger.warning(
                    "DBT_TOKEN is set but will be ignored because platform settings are incomplete. "
                    "Falling back to OAuth authentication. "
                    f"Missing/invalid settings: {'; '.join(dbt_platform_errors)}"
                )
            dbt_user_dir = get_dbt_profiles_path(
                dbt_profiles_dir=self.settings.dbt_profiles_dir
            )
            config_location = dbt_user_dir / "mcp.yml"
            actual_host = self.settings.actual_host
            if not actual_host:
                raise ValueError("DBT_HOST is a required environment variable")
            dbt_platform_url = _build_dbt_platform_url(
                actual_host, self.settings.actual_host_prefix
            )
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
            self.settings.dbt_project_ids = dbt_platform_context.selected_project_ids
            self.settings.dbt_host = self.settings.base_host
            if not dbt_platform_context.decoded_access_token:
                raise ValueError("No decoded access token found in OAuth context")

            token_provider = await OAuthTokenProvider.create(
                access_token_response=dbt_platform_context.decoded_access_token.access_token_response,
                dbt_platform_url=dbt_platform_url,
                context_manager=dbt_platform_context_manager,
            )
            self.token_provider = token_provider
            await asyncio.gather(
                self._resolve_account_identifier(),
                self._resolve_user_id(),
            )

            # Only validate CLI settings here — platform settings were already
            # checked at the top of get_credentials() and the OAuth flow has
            # populated the remaining fields (host, env ids, account id).
            # dbt_token stays None in the OAuth path since the OAuthTokenProvider
            # supplies the token instead.
            cli_errors = validate_dbt_cli_settings(self.settings)
            if cli_errors:
                raise ValueError(
                    "Errors found in configuration:\n\n" + "\n".join(cli_errors)
                )
            self.authentication_method = AuthenticationMethod.OAUTH
            self._log_settings()
            return self.settings, self.token_provider
        self.token_provider = StaticTokenProvider(token=self.settings.dbt_token)
        # Fetch host prefix from the platform when not explicitly configured via env vars
        # Only strip dbt_host to base_host if the fetch succeeds , otherwise
        # preserve what the user set in DBT_HOST. (The OAuth flow always strips dbt_host
        # to base_host since host_prefix is guaranteed to be set after login)
        if (
            not self.settings.actual_host_prefix
            and self.settings.dbt_account_id
            and self.settings.dbt_token
            and self.settings.actual_host
        ):
            dbt_platform_url = _build_dbt_platform_url(self.settings.actual_host, None)
            fetched_prefix = await _fetch_host_prefix_from_platform(
                dbt_platform_url=dbt_platform_url,
                account_id=self.settings.dbt_account_id,
                token=self.settings.dbt_token,
            )
            if fetched_prefix:
                self.settings.host_prefix = fetched_prefix
                self.settings.dbt_host = self.settings.base_host
                logger.info(f"Fetched prefix {fetched_prefix} from dbt Platform.")
        await asyncio.gather(
            self._resolve_account_identifier(),
            self._resolve_user_id(),
        )
        validate_settings(self.settings)
        self.authentication_method = AuthenticationMethod.ENV_VAR
        self._log_settings()
        return self.settings, self.token_provider
