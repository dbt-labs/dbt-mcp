"""Tests for the shared refresh_oauth_token function."""

import time
from unittest.mock import MagicMock, patch

import pytest

from dbt_mcp.oauth.dbt_platform import DbtPlatformContext
from dbt_mcp.oauth.refresh import refresh_oauth_token
from dbt_mcp.oauth.token import AccessTokenResponse, DecodedAccessToken


def _mock_token_response(*, expires_at: int | None = None) -> dict:
    return {
        "access_token": "new_access_token",
        "refresh_token": "new_refresh_token",
        "expires_in": 3600,
        "scope": "user_access offline_access",
        "token_type": "Bearer",
        "expires_at": expires_at or int(time.time()) + 3600,
    }


class TestRefreshOauthToken:
    def test_returns_context_on_success(self):
        """Successful refresh returns a DbtPlatformContext with a decoded token."""
        mock_response = _mock_token_response()
        expected_context = DbtPlatformContext(
            decoded_access_token=DecodedAccessToken(
                access_token_response=AccessTokenResponse(**mock_response),
                decoded_claims={"sub": "123"},
            )
        )

        with (
            patch("dbt_mcp.oauth.refresh.OAuth2Session") as mock_session_cls,
            patch(
                "dbt_mcp.oauth.refresh.dbt_platform_context_from_token_response"
            ) as mock_from_token,
        ):
            mock_session = MagicMock()
            mock_session.refresh_token.return_value = mock_response
            mock_session_cls.return_value = mock_session

            mock_from_token.return_value = expected_context

            result = refresh_oauth_token(
                refresh_token="old_refresh",
                token_url="https://cloud.getdbt.com/oauth/token",
                dbt_platform_url="https://cloud.getdbt.com",
            )

        assert result == expected_context
        mock_session.refresh_token.assert_called_once_with(
            url="https://cloud.getdbt.com/oauth/token",
            refresh_token="old_refresh",
        )
        mock_from_token.assert_called_once_with(mock_response, "https://cloud.getdbt.com")

    def test_propagates_exceptions(self):
        """Network or auth errors propagate without being swallowed."""
        with patch("dbt_mcp.oauth.refresh.OAuth2Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.refresh_token.side_effect = Exception("network error")
            mock_session_cls.return_value = mock_session

            with pytest.raises(Exception, match="network error"):
                refresh_oauth_token(
                    refresh_token="old_refresh",
                    token_url="https://cloud.getdbt.com/oauth/token",
                    dbt_platform_url="https://cloud.getdbt.com",
                )
