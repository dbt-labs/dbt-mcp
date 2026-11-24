from collections.abc import Callable, Mapping
from typing import Any

from authlib.integrations.requests_client import OAuth2Session
from authlib.common.errors import AuthlibBaseError
from authlib.integrations.base_client.errors import OAuthError
from authlib.oauth2.base import OAuth2Error

from dbt_mcp.oauth.client_id import OAUTH_CLIENT_ID
from dbt_mcp.oauth.dbt_platform import (
    DbtPlatformContext,
    dbt_platform_context_from_token_response,
)

# OAuth error codes (RFC 6749 section 5.2) that definitively reject the refresh
# request, such as ``invalid_grant`` for a revoked, expired or already-rotated
# refresh token. Repeating the same request cannot succeed. This is an
# allowlist: an unrecognized code (for example a proxy or rate limiter answering
# with its own JSON error) stays retryable, and retries are bounded elsewhere.
TERMINAL_OAUTH_ERROR_CODES = frozenset(
    {
        "invalid_grant",
        "invalid_client",
        "unauthorized_client",
        "unsupported_grant_type",
        "invalid_scope",
    }
)


def is_terminal_refresh_error(error: BaseException) -> bool:
    """Return True when the authorization server definitively rejected the refresh.

    Only an explicit OAuth error response carrying a known terminal code
    counts. Network failures, timeouts, TLS errors, 5xx and malformed
    responses stay retryable.
    """
    # ``OAuth2Session`` raises ``OAuthError`` for an error body from the token
    # endpoint. ``OAuth2Error`` covers the RFC 6749 error classes.
    if not isinstance(error, OAuthError | OAuth2Error):
        return False
    code = error.error if isinstance(error, AuthlibBaseError) else None
    return isinstance(code, str) and code in TERMINAL_OAUTH_ERROR_CODES


def refresh_oauth_token(
    *,
    refresh_token: str,
    token_url: str,
    dbt_platform_url: str,
    on_token_response: Callable[[Mapping[str, Any]], None] | None = None,
) -> DbtPlatformContext:
    """Perform an OAuth token refresh and return the new platform context.

    This is the single shared implementation used by both the background
    token-refresh worker (``OAuthTokenProvider``) and the startup refresh
    path (``_try_refresh_token`` in settings).

    ``on_token_response`` is called with the raw token response right after the
    token endpoint answers, before any later step that can fail. Callers use it
    to keep a rotated refresh token that the server has already issued.
    """
    oauth_client = OAuth2Session(
        client_id=OAUTH_CLIENT_ID,
        token_endpoint=token_url,
    )
    token_response = oauth_client.refresh_token(
        url=token_url,
        refresh_token=refresh_token,
    )
    if on_token_response is not None:
        on_token_response(token_response)
    return dbt_platform_context_from_token_response(token_response, dbt_platform_url)
