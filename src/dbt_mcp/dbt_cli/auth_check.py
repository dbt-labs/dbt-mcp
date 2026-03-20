import asyncio
import logging
import os
from enum import Enum

from dbt_mcp.config.config import DbtCliConfig
from dbt_mcp.dbt_cli.binary_type import get_color_disable_flag

logger = logging.getLogger(__name__)

AUTH_CHECK_TIMEOUT = 120  # Generous timeout for browser SSO


class AuthStatus(Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    AUTHENTICATED = "authenticated"
    TIMEOUT = "timeout"
    FAILED = "failed"


class WarehouseAuthChecker:
    """Runs `dbt debug` eagerly to trigger and cache warehouse authentication.

    When dbt uses browser-based warehouse auth (e.g., Snowflake externalbrowser),
    the token gets cached in the OS keychain after a successful auth. This checker
    triggers that flow during server startup so subsequent CLI commands work.
    """

    def __init__(self, config: DbtCliConfig) -> None:
        self._config = config
        self._status = AuthStatus.NOT_STARTED
        self._error_message: str | None = None

    @property
    def status(self) -> AuthStatus:
        return self._status

    @property
    def error_message(self) -> str | None:
        return self._error_message

    async def run_auth_check(self) -> None:
        """Run `dbt debug` to trigger warehouse authentication."""
        self._status = AuthStatus.IN_PROGRESS
        logger.info("Starting warehouse auth check via dbt debug")

        try:
            cwd_path = (
                self._config.project_dir
                if os.path.isabs(self._config.project_dir)
                else None
            )
            color_flag = get_color_disable_flag(self._config.binary_type)
            args = [self._config.dbt_path, color_flag, "debug"]

            process = await asyncio.create_subprocess_exec(
                *args,
                cwd=cwd_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            stdout, _ = await asyncio.wait_for(
                process.communicate(),
                timeout=AUTH_CHECK_TIMEOUT,
            )
            output = stdout.decode() if stdout else ""

            if process.returncode == 0:
                self._status = AuthStatus.AUTHENTICATED
                logger.info("Warehouse auth check succeeded")
            else:
                self._status = AuthStatus.FAILED
                self._error_message = output
                logger.warning(f"Warehouse auth check failed: {output}")

        except TimeoutError:
            self._status = AuthStatus.TIMEOUT
            self._error_message = (
                "Warehouse authentication timed out. "
                "If your dbt profile uses browser-based authentication "
                "(e.g., Snowflake externalbrowser), please run "
                "'dbt debug' in your terminal to authenticate, then restart the MCP server."
            )
            logger.warning("Warehouse auth check timed out")
        except Exception as e:
            self._status = AuthStatus.FAILED
            self._error_message = str(e)
            logger.warning(f"Warehouse auth check error: {e}")

    def get_status_message(self) -> str | None:
        """Return an actionable message if auth is not ready, or None if OK."""
        if self._status in (AuthStatus.AUTHENTICATED, AuthStatus.NOT_STARTED):
            return None
        if self._status == AuthStatus.IN_PROGRESS:
            return None  # Let the command proceed; auth may complete during execution
        if self._status == AuthStatus.TIMEOUT:
            return self._error_message
        if self._status == AuthStatus.FAILED:
            return (
                f"Warehouse connection check failed during startup. "
                f"dbt debug output:\n{self._error_message}"
            )
        return None
