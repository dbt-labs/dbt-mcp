import asyncio
import logging
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from pydantic import ValidationError

from dbt_mcp.errors.hints import MULTICELL_HINT, looks_like_ssl_error
from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.expiry import (
    BACKGROUND_REFRESH_BUFFER_SECONDS,
    INLINE_REFRESH_BUFFER_SECONDS,
)
from dbt_mcp.oauth.refresh import is_terminal_refresh_error, refresh_oauth_token
from dbt_mcp.oauth.refresh_strategy import DefaultRefreshStrategy, RefreshStrategy
from dbt_mcp.oauth.token import AccessTokenResponse

logger = logging.getLogger(__name__)

# Consecutive transient background refresh failures tolerated before the worker
# stops. Inline refresh in ``get_token()`` still runs on demand afterwards.
MAX_CONSECUTIVE_REFRESH_FAILURES = 5

# A refresh that returns a token due again within this many seconds past the
# strategy's buffer counts as a failure, so short-lived tokens cannot produce an
# unbounded stream of refresh requests.
MIN_REFRESH_INTERVAL_SECONDS = 60

REAUTHENTICATE_MESSAGE = (
    "Please re-authenticate by restarting the MCP server, which will start a new login."
)

# Re-export for backward compatibility.
TOKEN_EXPIRY_BUFFER_SECONDS = INLINE_REFRESH_BUFFER_SECONDS


def _echoes_input(error: BaseException) -> bool:
    return isinstance(error, ValidationError)


class TokenProvider(Protocol):
    def get_token(self) -> str: ...


class OAuthTokenProvider:
    """
    Token provider for OAuth access token with periodic refresh.

    Use the async ``create()`` factory to construct an instance -- it starts the
    background refresh worker automatically so the token stays fresh even before
    the first ``get_token()`` call.

    As a safety net, ``get_token()`` also validates the token expiry and performs an
    inline (synchronous) refresh when the token is about to expire.
    """

    def __init__(
        self,
        *,
        access_token_response: AccessTokenResponse,
        dbt_platform_url: str,
        context_manager: DbtPlatformContextManager,
        refresh_strategy: RefreshStrategy | None = None,
        max_consecutive_refresh_failures: int = MAX_CONSECUTIVE_REFRESH_FAILURES,
        min_refresh_interval_seconds: float = MIN_REFRESH_INTERVAL_SECONDS,
    ):
        self.min_refresh_interval_seconds = max(min_refresh_interval_seconds, 0)
        self.max_consecutive_refresh_failures = max(max_consecutive_refresh_failures, 1)
        # Serializes refreshes so a rotating refresh token is never spent twice.
        self._refresh_lock = threading.Lock()
        # Set once the server definitively rejects the refresh token. Nothing
        # retries after that, because only a new login can recover.
        self._terminal_error: BaseException | None = None
        # Latest refresh token the server issued. Updated as soon as the token
        # endpoint answers, so a later failure (key fetch, disk write) does not
        # leave us holding a refresh token the server has already rotated.
        self._refresh_token_value = access_token_response.refresh_token
        self.access_token_response = access_token_response
        self.context_manager = context_manager
        self.dbt_platform_url = dbt_platform_url
        self.refresh_strategy = refresh_strategy or DefaultRefreshStrategy()
        self.token_url = f"{self.dbt_platform_url}/oauth/token"

    @classmethod
    async def create(
        cls,
        *,
        access_token_response: AccessTokenResponse,
        dbt_platform_url: str,
        context_manager: DbtPlatformContextManager,
        refresh_strategy: RefreshStrategy | None = None,
    ) -> "OAuthTokenProvider":
        provider = cls(
            access_token_response=access_token_response,
            dbt_platform_url=dbt_platform_url,
            context_manager=context_manager,
            refresh_strategy=refresh_strategy,
        )
        provider.start_background_refresh()
        return provider

    def _is_token_expired(self) -> bool:
        """Check whether the current access token is expired or about to expire."""
        return (
            self.access_token_response.expires_at
            < time.time() + TOKEN_EXPIRY_BUFFER_SECONDS
        )

    def _remember_rotated_refresh_token(
        self, token_response: Mapping[str, Any]
    ) -> None:
        rotated = token_response.get("refresh_token")
        if isinstance(rotated, str) and rotated:
            self._refresh_token_value = rotated

    def _is_due_for_refresh(self) -> bool:
        """True when the worker's own strategy would refresh again right away."""
        buffer = getattr(
            self.refresh_strategy,
            "buffer_seconds",
            BACKGROUND_REFRESH_BUFFER_SECONDS,
        )
        return (
            self.access_token_response.expires_at
            <= time.time() + buffer + MIN_REFRESH_INTERVAL_SECONDS
        )

    def _adopt_refresh_token_from_context(self, *, rejected: str) -> bool:
        """Switch to a different refresh token another instance persisted."""
        try:
            context = self.context_manager.read_context()
            persisted = (
                context.decoded_access_token.access_token_response.refresh_token
                if context and context.decoded_access_token
                else None
            )
        except Exception:
            return False
        if isinstance(persisted, str) and persisted and persisted != rejected:
            self._refresh_token_value = persisted
            return True
        return False

    def _invalidate_persisted_credentials(self) -> None:
        """Make the next startup log in again instead of reusing rejected tokens."""
        try:
            self.context_manager.invalidate_access_token(
                rejected_refresh_token=self._refresh_token_value
            )
        except Exception as e:
            logger.warning("Failed to invalidate rejected OAuth credentials: %s", e)

    def _refresh_token(
        self,
        *,
        stale_refresh_token: str | None = None,
        only_if: Callable[[], bool] | None = None,
    ) -> None:
        """Refresh the OAuth access token using the refresh token.

        Used by both the background worker and the inline safety-net in
        ``get_token()``.  All operations (authlib's ``refresh_token``, context
        persistence) are synchronous, so a plain ``def`` is sufficient.

        Refreshes are serialized. Inside the lock the call returns without a
        request when ``only_if`` says a refresh is no longer needed, or when
        ``stale_refresh_token`` was given and the refresh token has changed
        since, because another caller already refreshed. A definitive
        rejection from the server is remembered and re-raised without any
        further network call.
        """
        with self._refresh_lock:
            if self._terminal_error is not None:
                raise ValueError(str(self._terminal_error)) from self._terminal_error
            if only_if is not None and not only_if():
                return
            if (
                stale_refresh_token is not None
                and self._refresh_token_value != stale_refresh_token
            ):
                return
            logger.info("Refreshing OAuth access token")
            try:
                dbt_platform_context = refresh_oauth_token(
                    refresh_token=self._refresh_token_value,
                    token_url=self.token_url,
                    dbt_platform_url=self.dbt_platform_url,
                    on_token_response=self._remember_rotated_refresh_token,
                )
            except Exception as e:
                if is_terminal_refresh_error(e):
                    if self._adopt_refresh_token_from_context(
                        rejected=self._refresh_token_value
                    ):
                        raise ConnectionError(
                            "Refresh token was rotated by another instance; "
                            "retrying with the persisted one"
                        ) from e
                    self._terminal_error = e
                    self._invalidate_persisted_credentials()
                raise
            if not dbt_platform_context.decoded_access_token:
                raise ValueError("No decoded access token found in context")
            self.access_token_response = (
                dbt_platform_context.decoded_access_token.access_token_response
            )
            self._refresh_token_value = self.access_token_response.refresh_token
            try:
                self.context_manager.update_context(dbt_platform_context)
            except Exception as e:
                # The new token is already in memory and the refresh succeeded.
                logger.error("Failed to persist refreshed OAuth context: %s", e)
            logger.info("OAuth access token refreshed successfully")

    def get_token(self) -> str:
        if self._terminal_error is not None:
            raise ValueError(
                "OAuth refresh token was rejected by the server. "
                + REAUTHENTICATE_MESSAGE
            ) from self._terminal_error
        if self._is_token_expired():
            try:
                self._refresh_token(only_if=self._is_token_expired)
            except Exception as e:
                raise ValueError(
                    "OAuth access token is expired and inline refresh failed. "
                    + REAUTHENTICATE_MESSAGE
                ) from e
        return self.access_token_response.access_token

    def start_background_refresh(self) -> asyncio.Task[None]:
        logger.info("Starting oauth token background refresh")
        return asyncio.create_task(
            self._background_refresh_worker(), name="oauth-token-refresh"
        )

    async def _background_refresh_worker(self) -> None:
        """Background worker that periodically refreshes tokens before expiry.

        Stops on a definitive rejection of the refresh token. Transient
        failures back off exponentially and stop after
        ``max_consecutive_refresh_failures`` in a row. A refresh that succeeds
        but returns a token already due for refresh counts as a failure, so a
        server issuing very short-lived tokens cannot cause a request loop.
        """
        logger.info("Background token refresh worker started")
        consecutive_failures = 0
        last_attempt: float | None = None
        while True:
            try:
                stale = self._refresh_token_value
                await self.refresh_strategy.wait_until_refresh_needed(
                    self.access_token_response.expires_at
                )
                # Whatever the strategy decides, never send refresh requests
                # closer together than the minimum interval. The sleep also
                # always yields to the event loop.
                remaining = 0.0
                if last_attempt is not None:
                    remaining = self.min_refresh_interval_seconds - (
                        time.monotonic() - last_attempt
                    )
                await asyncio.sleep(max(remaining, 0))
                last_attempt = time.monotonic()
                self._refresh_token(stale_refresh_token=stale)
                if not self._is_due_for_refresh():
                    consecutive_failures = 0
                    continue
                raise ValueError("Refreshed access token is already due for refresh")
            except Exception as e:
                # An inline refresh may have latched the rejection already, in
                # which case it reaches here wrapped and looks transient.
                if self._terminal_error is not None or is_terminal_refresh_error(e):
                    logger.error(
                        "OAuth refresh token was rejected by the server; "
                        "stopping background refresh. %s",
                        REAUTHENTICATE_MESSAGE,
                    )
                    return
                consecutive_failures += 1
                # Log the type only for validation errors: their text can echo
                # fragments of the token response.
                logger.error(
                    "Error in background refresh worker: %s",
                    type(e).__name__ if _echoes_input(e) else e,
                )
                if looks_like_ssl_error(e):
                    logger.error(MULTICELL_HINT)
                if consecutive_failures >= self.max_consecutive_refresh_failures:
                    logger.error(
                        "Background token refresh failed %d times in a row; "
                        "stopping background refresh. %s",
                        consecutive_failures,
                        REAUTHENTICATE_MESSAGE,
                    )
                    return
                await self.refresh_strategy.wait_after_error(consecutive_failures)


class StaticTokenProvider:
    """
    Token provider for tokens that aren't refreshed (e.g. service tokens and PATs)
    """

    def __init__(self, token: str | None = None):
        self.token = token

    def get_token(self) -> str:
        if not self.token:
            raise ValueError("No token provided")
        return self.token
