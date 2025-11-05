import asyncio
import logging
from typing import Protocol

from authlib.integrations.requests_client import OAuth2Session

from dbt_mcp.oauth.client_id import OAUTH_CLIENT_ID
from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.dbt_platform import dbt_platform_context_from_token_response
from dbt_mcp.oauth.refresh_strategy import DefaultRefreshStrategy, RefreshStrategy
from dbt_mcp.oauth.token import AccessTokenResponse

logger = logging.getLogger(__name__)


class TokenProvider(Protocol):
    def get_token(self) -> str: ...


class OAuthTokenProvider:
    """
    Token provider for OAuth access token with periodic refresh.
    """

    def __init__(
        self,
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
        self.oauth_client = OAuth2Session(
            client_id=OAUTH_CLIENT_ID,
            token_endpoint=self.token_url,
        )
        self.refresh_started = False

    def _get_access_token_response(self) -> AccessTokenResponse:
        dbt_platform_context = self.context_manager.read_context()
        if not dbt_platform_context or not dbt_platform_context.decoded_access_token:
            raise ValueError("No decoded access token found in context")
        return dbt_platform_context.decoded_access_token.access_token_response

    def get_token(self) -> str:
        if not self.refresh_started:
            self.start_background_refresh()
            self.refresh_started = True
        return self.access_token_response.access_token

    def start_background_refresh(self) -> asyncio.Task[None]:
        logger.info("Starting oauth token background refresh")
        return asyncio.create_task(
            self._background_refresh_worker(), name="oauth-token-refresh"
        )

    async def _refresh_token(self) -> None:
        logger.info("Refreshing OAuth access token using authlib")
        token_response = self.oauth_client.refresh_token(
            url=self.token_url,
            refresh_token=self.access_token_response.refresh_token,
        )
        dbt_platform_context = dbt_platform_context_from_token_response(
            token_response, self.dbt_platform_url
        )
        self.context_manager.update_context(dbt_platform_context)
        if not dbt_platform_context.decoded_access_token:
            raise ValueError("No decoded access token found in context")
        self.access_token_response = (
            dbt_platform_context.decoded_access_token.access_token_response
        )
        logger.info("OAuth access token refreshed and context updated successfully")

    async def _background_refresh_worker(self) -> None:
        """Background worker that periodically refreshes tokens before expiry."""
        logger.info("Background token refresh worker started")
        while True:
            try:
                await self.refresh_strategy.wait_until_refresh_needed(
                    self.access_token_response.expires_at
                )
                await self._refresh_token()
            except Exception as e:
                logger.error(f"Error in background refresh worker: {e}")
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
