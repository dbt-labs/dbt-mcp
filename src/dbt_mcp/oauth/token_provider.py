import asyncio
import logging
import time
from typing import Protocol

from dbt_mcp.errors.hints import MULTICELL_HINT, looks_like_ssl_error
from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.expiry import INLINE_REFRESH_BUFFER_SECONDS
from dbt_mcp.oauth.refresh import refresh_oauth_token
from dbt_mcp.oauth.refresh_strategy import DefaultRefreshStrategy, RefreshStrategy
from dbt_mcp.oauth.token import AccessTokenResponse

logger = logging.getLogger(__name__)

# Re-export for backward compatibility.
TOKEN_EXPIRY_BUFFER_SECONDS = INLINE_REFRESH_BUFFER_SECONDS


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
    ):
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

    def _refresh_token(self) -> None:
        """Refresh the OAuth access token using the refresh token.

        Used by both the background worker and the inline safety-net in
        ``get_token()``.  All operations (authlib's ``refresh_token``, context
        persistence) are synchronous, so a plain ``def`` is sufficient.
        """
        logger.info("Refreshing OAuth access token")
        dbt_platform_context = refresh_oauth_token(
            refresh_token=self.access_token_response.refresh_token,
            token_url=self.token_url,
            dbt_platform_url=self.dbt_platform_url,
        )
        self.context_manager.update_context(dbt_platform_context)
        if not dbt_platform_context.decoded_access_token:
            raise ValueError("No decoded access token found in context")
        self.access_token_response = (
            dbt_platform_context.decoded_access_token.access_token_response
        )
        logger.info("OAuth access token refreshed successfully")

    def get_token(self) -> str:
        if self._is_token_expired():
            try:
                self._refresh_token()
            except Exception as e:
                raise ValueError(
                    "OAuth access token is expired and inline refresh failed. "
                    "Please re-authenticate by restarting the MCP server."
                ) from e
        return self.access_token_response.access_token

    def start_background_refresh(self) -> asyncio.Task[None]:
        logger.info("Starting oauth token background refresh")
        return asyncio.create_task(
            self._background_refresh_worker(), name="oauth-token-refresh"
        )

    async def _background_refresh_worker(self) -> None:
        """Background worker that periodically refreshes tokens before expiry."""
        logger.info("Background token refresh worker started")
        while True:
            try:
                await self.refresh_strategy.wait_until_refresh_needed(
                    self.access_token_response.expires_at
                )
                self._refresh_token()
            except Exception as e:
                logger.error("Error in background refresh worker: %s", e, exc_info=True)
                if looks_like_ssl_error(e):
                    logger.error(MULTICELL_HINT)
                await self.refresh_strategy.wait_after_error()


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
