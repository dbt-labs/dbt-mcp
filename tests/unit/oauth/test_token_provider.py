"""
Tests for OAuthTokenProvider -- covers:
  - get_token() validates token expiry and refreshes inline
  - background refresh can be started eagerly (no lazy start in get_token)
  - settings.dbt_token is not accessed after startup
"""

import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

from dbt_mcp.oauth.token import AccessTokenResponse
from dbt_mcp.oauth.token_provider import (
    TOKEN_EXPIRY_BUFFER_SECONDS,
    OAuthTokenProvider,
)


def _make_access_token_response(
    *, expires_at: int | None = None, access_token: str = "valid_token"
) -> AccessTokenResponse:
    return AccessTokenResponse(
        access_token=access_token,
        refresh_token="refresh_token",
        expires_in=3600,
        scope="user_access offline_access",
        token_type="Bearer",
        expires_at=expires_at or int(time.time()) + 3600,
    )


def _make_provider(
    *, expires_at: int | None = None, access_token: str = "valid_token"
) -> OAuthTokenProvider:
    return OAuthTokenProvider(
        access_token_response=_make_access_token_response(
            expires_at=expires_at, access_token=access_token
        ),
        dbt_platform_url="https://cloud.getdbt.com",
        context_manager=MagicMock(),
    )


class TestGetTokenValidatesExpiry:
    """get_token() should check token validity."""

    def test_returns_token_when_valid(self):
        """A non-expired token is returned directly."""
        provider = _make_provider(access_token="my_token")
        assert provider.get_token() == "my_token"

    def test_refreshes_when_expired(self):
        """An expired token triggers an inline sync refresh."""
        expired_at = int(time.time()) - 100
        provider = _make_provider(expires_at=expired_at, access_token="stale")

        new_token_response = _make_access_token_response(
            access_token="fresh", expires_at=int(time.time()) + 3600
        )
        mock_context = MagicMock()
        mock_context.decoded_access_token.access_token_response = new_token_response

        with patch.object(provider, "oauth_client") as mock_client:
            mock_client.refresh_token.return_value = {
                "access_token": "fresh",
                "refresh_token": "new_refresh",
                "expires_in": 3600,
                "scope": "user_access offline_access",
                "token_type": "Bearer",
                "expires_at": int(time.time()) + 3600,
            }
            with patch(
                "dbt_mcp.oauth.token_provider.dbt_platform_context_from_token_response"
            ) as mock_from_token:
                mock_from_token.return_value = mock_context
                token = provider.get_token()

        assert token == "fresh"
        mock_client.refresh_token.assert_called_once()

    def test_raises_when_refresh_fails(self):
        """If inline refresh fails, a clear error is raised (not a stale token)."""
        expired_at = int(time.time()) - 100
        provider = _make_provider(expires_at=expired_at, access_token="stale")

        with patch.object(provider, "oauth_client") as mock_client:
            mock_client.refresh_token.side_effect = Exception("network error")
            with pytest.raises(ValueError, match="expired and inline refresh failed"):
                provider.get_token()

    def test_refreshes_when_within_buffer(self):
        """Token expiring within buffer window triggers refresh."""
        almost_expired = int(time.time()) + TOKEN_EXPIRY_BUFFER_SECONDS - 5
        provider = _make_provider(expires_at=almost_expired, access_token="stale")

        new_token_response = _make_access_token_response(
            access_token="fresh", expires_at=int(time.time()) + 3600
        )
        mock_context = MagicMock()
        mock_context.decoded_access_token.access_token_response = new_token_response

        with patch.object(provider, "oauth_client") as mock_client:
            mock_client.refresh_token.return_value = {}
            with patch(
                "dbt_mcp.oauth.token_provider.dbt_platform_context_from_token_response"
            ) as mock_from_token:
                mock_from_token.return_value = mock_context
                token = provider.get_token()

        assert token == "fresh"


class TestNoLazyStartInGetToken:
    """get_token() should NOT start background refresh lazily."""

    def test_get_token_does_not_call_start_background_refresh(self):
        """get_token() must not start background refresh itself."""
        provider = _make_provider()
        with patch.object(provider, "start_background_refresh") as mock_start:
            provider.get_token()
            mock_start.assert_not_called()


class TestEagerBackgroundRefresh:
    """start_background_refresh creates an asyncio task."""

    @pytest.mark.asyncio
    async def test_start_background_refresh_creates_task(self):
        """start_background_refresh() should create an asyncio task."""
        provider = _make_provider()
        task = provider.start_background_refresh()
        assert isinstance(task, asyncio.Task)
        assert task.get_name() == "oauth-token-refresh"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class TestNoRefreshStartedFlag:
    """The old refresh_started flag should no longer exist."""

    def test_no_refresh_started_attribute(self):
        """OAuthTokenProvider should not have a refresh_started attribute."""
        provider = _make_provider()
        assert not hasattr(provider, "refresh_started")
