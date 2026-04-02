from authlib.integrations.requests_client import OAuth2Session

from dbt_mcp.oauth.client_id import OAUTH_CLIENT_ID
from dbt_mcp.oauth.dbt_platform import (
    DbtPlatformContext,
    dbt_platform_context_from_token_response,
)


def refresh_oauth_token(
    *,
    refresh_token: str,
    token_url: str,
    dbt_platform_url: str,
) -> DbtPlatformContext:
    """Perform an OAuth token refresh and return the new platform context.

    This is the single shared implementation used by both the background
    token-refresh worker (``OAuthTokenProvider``) and the startup refresh
    path (``_try_refresh_token`` in settings).
    """
    oauth_client = OAuth2Session(
        client_id=OAUTH_CLIENT_ID,
        token_endpoint=token_url,
    )
    token_response = oauth_client.refresh_token(
        url=token_url,
        refresh_token=refresh_token,
    )
    return dbt_platform_context_from_token_response(token_response, dbt_platform_url)
