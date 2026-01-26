import time
from unittest.mock import MagicMock, patch

from dbt_mcp.config.settings import (
    _is_context_complete,
    _is_token_valid,
    _try_refresh_token,
)
from dbt_mcp.oauth.dbt_platform import (
    DbtPlatformContext,
    DbtPlatformEnvironment,
)
from dbt_mcp.oauth.token import AccessTokenResponse, DecodedAccessToken


def _create_mock_context(
    expires_at: int | None = None,
    with_account: bool = True,
    with_host_prefix: bool = True,
    with_dev_env: bool = True,
    with_prod_env: bool = True,
    with_token: bool = True,
) -> DbtPlatformContext:
    """Helper to create a mock DbtPlatformContext for testing."""
    decoded_access_token = None
    if with_token:
        access_token_response = AccessTokenResponse(
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_in=3600,
            scope="user_access offline_access",
            token_type="Bearer",
            expires_at=expires_at or int(time.time()) + 3600,
        )
        decoded_access_token = DecodedAccessToken(
            access_token_response=access_token_response,
            decoded_claims={"sub": "123"},
        )

    dev_environment = None
    if with_dev_env:
        dev_environment = DbtPlatformEnvironment(
            id=111, name="Development", deployment_type="development"
        )

    prod_environment = None
    if with_prod_env:
        prod_environment = DbtPlatformEnvironment(
            id=222, name="Production", deployment_type="production"
        )

    return DbtPlatformContext(
        decoded_access_token=decoded_access_token,
        account_id=456 if with_account else None,
        host_prefix="acme" if with_host_prefix else None,
        dev_environment=dev_environment,
        prod_environment=prod_environment,
    )


class TestIsContextComplete:
    """Tests for _is_context_complete helper function."""

    def test_complete_context_returns_true(self):
        """A context with all required fields should return True."""
        ctx = _create_mock_context()
        assert _is_context_complete(ctx) is True

    def test_none_context_returns_false(self):
        """None context should return False."""
        assert _is_context_complete(None) is False

    def test_missing_account_id_returns_false(self):
        """Context without account_id should return False."""
        ctx = _create_mock_context(with_account=False)
        assert _is_context_complete(ctx) is False

    def test_missing_host_prefix_returns_false(self):
        """Context without host_prefix should return False."""
        ctx = _create_mock_context(with_host_prefix=False)
        assert _is_context_complete(ctx) is False

    def test_missing_dev_environment_returns_true(self):
        """Context without dev_environment should still return True (dev_env is optional)."""
        ctx = _create_mock_context(with_dev_env=False)
        assert _is_context_complete(ctx) is True

    def test_missing_prod_environment_returns_false(self):
        """Context without prod_environment should return False."""
        ctx = _create_mock_context(with_prod_env=False)
        assert _is_context_complete(ctx) is False

    def test_missing_token_returns_false(self):
        """Context without decoded_access_token should return False."""
        ctx = _create_mock_context(with_token=False)
        assert _is_context_complete(ctx) is False


class TestIsTokenValid:
    """Tests for _is_token_valid helper function."""

    def test_valid_token_returns_true(self):
        """Token expiring in the future should return True."""
        future_expiry = int(time.time()) + 3600  # 1 hour from now
        ctx = _create_mock_context(expires_at=future_expiry)
        assert _is_token_valid(ctx) is True

    def test_expired_token_returns_false(self):
        """Token that has expired should return False."""
        past_expiry = int(time.time()) - 3600  # 1 hour ago
        ctx = _create_mock_context(expires_at=past_expiry)
        assert _is_token_valid(ctx) is False

    def test_token_expiring_within_buffer_returns_false(self):
        """Token expiring within 2-minute buffer should return False."""
        near_expiry = int(time.time()) + 60  # 1 minute from now (within 2 min buffer)
        ctx = _create_mock_context(expires_at=near_expiry)
        assert _is_token_valid(ctx) is False

    def test_no_token_returns_false(self):
        """Context without token should return False."""
        ctx = _create_mock_context(with_token=False)
        assert _is_token_valid(ctx) is False


class TestTryRefreshToken:
    """Tests for _try_refresh_token helper function."""

    def test_successful_refresh(self):
        """Test successful token refresh."""
        ctx = _create_mock_context(expires_at=int(time.time()) - 3600)  # Expired
        mock_context_manager = MagicMock()

        # Mock the OAuth2Session and its refresh_token method
        mock_token_response = {
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
            "expires_in": 3600,
            "scope": "user_access offline_access",
            "token_type": "Bearer",
            "expires_at": int(time.time()) + 3600,
        }

        with (
            patch("dbt_mcp.config.settings.OAuth2Session") as mock_oauth_session_class,
            patch(
                "dbt_mcp.config.settings.dbt_platform_context_from_token_response"
            ) as mock_from_token,
        ):
            mock_oauth_session = MagicMock()
            mock_oauth_session.refresh_token.return_value = mock_token_response
            mock_oauth_session_class.return_value = mock_oauth_session

            # Create a new context that would be returned after refresh
            new_ctx = _create_mock_context(expires_at=int(time.time()) + 3600)
            mock_from_token.return_value = new_ctx

            result = _try_refresh_token(
                ctx, "https://cloud.getdbt.com", mock_context_manager
            )

            assert result is not None
            mock_oauth_session.refresh_token.assert_called_once()
            mock_context_manager.write_context_to_file.assert_called_once()

    def test_refresh_fails_gracefully(self):
        """Test that refresh failure returns None and logs warning."""
        ctx = _create_mock_context(expires_at=int(time.time()) - 3600)
        mock_context_manager = MagicMock()

        with patch("dbt_mcp.config.settings.OAuth2Session") as mock_oauth_session_class:
            mock_oauth_session = MagicMock()
            mock_oauth_session.refresh_token.side_effect = Exception("Network error")
            mock_oauth_session_class.return_value = mock_oauth_session

            result = _try_refresh_token(
                ctx, "https://cloud.getdbt.com", mock_context_manager
            )

            assert result is None
            mock_context_manager.write_context_to_file.assert_not_called()

    def test_no_token_returns_none(self):
        """Test that context without token returns None."""
        ctx = _create_mock_context(with_token=False)
        mock_context_manager = MagicMock()

        result = _try_refresh_token(
            ctx, "https://cloud.getdbt.com", mock_context_manager
        )

        assert result is None
