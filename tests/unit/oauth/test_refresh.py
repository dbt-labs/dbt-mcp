"""Tests for the shared refresh_oauth_token function."""

from unittest.mock import MagicMock, patch

from dbt_mcp.oauth.refresh import refresh_oauth_token


class TestRefreshOauthToken:
    def test_returns_context_on_success(self):
        """A successful refresh returns the DbtPlatformContext."""
        mock_context = MagicMock()

        with (
            patch("dbt_mcp.oauth.refresh.OAuth2Session") as mock_session_cls,
            patch(
                "dbt_mcp.oauth.refresh.dbt_platform_context_from_token_response"
            ) as mock_from_token,
        ):
            mock_session = MagicMock()
            mock_session.refresh_token.return_value = {"access_token": "new"}
            mock_session_cls.return_value = mock_session
            mock_from_token.return_value = mock_context

            result = refresh_oauth_token(
                refresh_token="old_refresh",
                token_url="https://cloud.getdbt.com/oauth/token",
                dbt_platform_url="https://cloud.getdbt.com",
            )

        assert result is mock_context
        mock_session.refresh_token.assert_called_once_with(
            url="https://cloud.getdbt.com/oauth/token",
            refresh_token="old_refresh",
        )
        mock_from_token.assert_called_once_with(
            {"access_token": "new"}, "https://cloud.getdbt.com"
        )

    def test_propagates_exceptions(self):
        """Exceptions from the OAuth session are not caught."""
        with patch("dbt_mcp.oauth.refresh.OAuth2Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.refresh_token.side_effect = RuntimeError("network failure")
            mock_session_cls.return_value = mock_session

            try:
                refresh_oauth_token(
                    refresh_token="old_refresh",
                    token_url="https://cloud.getdbt.com/oauth/token",
                    dbt_platform_url="https://cloud.getdbt.com",
                )
                assert False, "Expected RuntimeError"
            except RuntimeError as e:
                assert "network failure" in str(e)
