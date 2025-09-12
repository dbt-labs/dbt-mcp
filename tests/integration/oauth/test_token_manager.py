"""
Integration tests for OAuth token manager that test real refresh functionality
with mock timing strategy for controlled test execution.
"""

import asyncio
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest
from authlib.integrations.requests_client import OAuth2Session

from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.token import AccessTokenResponse
from dbt_mcp.oauth.token_provider import OAuthTokenProvider
from tests.mocks.refresh_strategy import MockRefreshStrategy


@pytest.fixture
def temp_config_dir():
    """Create a temporary directory for config files."""
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def context_manager(temp_config_dir):
    """Create a context manager with temporary config location."""
    config_path = temp_config_dir / "dbt_platform_context.yml"
    return DbtPlatformContextManager(config_location=config_path)


@pytest.fixture
def sample_access_token_response():
    """Create a sample access token response for testing."""
    current_time = int(time.time())
    return AccessTokenResponse(
        access_token="test_access_token_123",
        refresh_token="test_refresh_token_456",
        expires_in=3600,
        scope="read write",
        token_type="Bearer",
        expires_at=current_time + 3600,  # Expires in 1 hour
    )


@pytest.fixture
def mock_refresh_strategy():
    """Create a mock refresh strategy that doesn't wait."""
    return MockRefreshStrategy(should_wait=False)


@pytest.fixture
def sample_decoded_claims():
    """Create sample decoded JWT claims."""
    return {
        "sub": "user_123",
        "iss": "https://auth.getdbt.com",
        "exp": int(time.time()) + 3600,
        "iat": int(time.time()),
        "scope": "read write",
        "user_id": 123,
        "account_id": 456,
    }


class TestOAuthTokenManagerIntegration:
    """Integration tests for OAuthTokenManager with real endpoints."""

    @pytest.mark.asyncio
    async def test_token_manager_initialization(
        self,
        sample_access_token_response,
        context_manager,
        mock_refresh_strategy,
    ):
        """Test that token manager initializes correctly with mock timing strategy."""

        token_manager = OAuthTokenProvider(
            access_token_response=sample_access_token_response,
            dbt_platform_url="https://auth.getdbt.com",
            context_manager=context_manager,
            refresh_strategy=mock_refresh_strategy,
        )

        # Verify the token manager is properly initialized
        assert token_manager.get_token() == "test_access_token_123"
        assert token_manager.refresh_strategy == mock_refresh_strategy
        assert token_manager._refresh_task is not None

        # Give the background task a moment to start
        await asyncio.sleep(0.1)

        # Verify the mock strategy was called for timing
        assert mock_refresh_strategy.call_count >= 1
        assert (
            sample_access_token_response.expires_at in mock_refresh_strategy.wait_calls
        )

        # Cleanup
        token_manager._refresh_task.cancel()
        try:
            await token_manager._refresh_task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_real_token_refresh_success(
        self,
        sample_access_token_response,
        context_manager,
        mock_refresh_strategy,
        sample_decoded_claims,
    ):
        """Test successful token refresh with real HTTP call but mock timing."""

        # Mock the OAuth2Session and HTTP response
        mock_token_response = {
            "access_token": "new_access_token_789",
            "refresh_token": "new_refresh_token_012",
            "expires_in": 3600,
            "scope": "read write",
            "token_type": "Bearer",
            "expires_at": int(time.time()) + 3600,
        }

        with (
            patch.object(
                OAuth2Session, "refresh_token", return_value=mock_token_response
            ),
            patch(
                "dbt_mcp.oauth.token_manager.fetch_jwks_and_verify_token",
                return_value=sample_decoded_claims,
            ),
        ):
            token_manager = OAuthTokenProvider(
                access_token_response=sample_access_token_response,
                dbt_platform_url="https://auth.getdbt.com",
                context_manager=context_manager,
                refresh_strategy=mock_refresh_strategy,
            )

            # Manually trigger a token refresh
            await token_manager._refresh_token()

            # Verify the token was updated
            assert token_manager.get_token() == "new_access_token_789"
            assert (
                token_manager.access_token_response.refresh_token
                == "new_refresh_token_012"
            )

        # Cleanup
        token_manager._refresh_task.cancel()
        try:
            await token_manager._refresh_task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_token_refresh_error_handling(
        self,
        sample_access_token_response,
        context_manager,
        mock_refresh_strategy,
    ):
        """Test error handling during token refresh with mock timing."""

        # Mock OAuth2Session to raise an exception
        with patch.object(
            OAuth2Session,
            "refresh_token",
            side_effect=Exception("Token refresh failed"),
        ):
            token_manager = OAuthTokenProvider(
                access_token_response=sample_access_token_response,
                dbt_platform_url="https://auth.getdbt.com",
                context_manager=context_manager,
                refresh_strategy=mock_refresh_strategy,
            )

            # Manually trigger refresh and expect it to handle the error
            with pytest.raises(Exception, match="Token refresh failed"):
                await token_manager._refresh_token()

            # Original token should remain unchanged
            assert token_manager.get_token() == "test_access_token_123"

        # Cleanup
        token_manager._refresh_task.cancel()
        try:
            await token_manager._refresh_task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_background_refresh_with_error_retry(
        self,
        sample_access_token_response,
        context_manager,
        mock_refresh_strategy,
    ):
        """Test background refresh worker handles errors and retries with mock timing."""

        # Mock _refresh_token to fail initially, then succeed
        failed_once = False

        async def mock_refresh_token():
            nonlocal failed_once
            if not failed_once:
                failed_once = True
                raise Exception("First attempt failed")
            # Second attempt succeeds (do nothing)
            pass

        token_manager = OAuthTokenProvider(
            access_token_response=sample_access_token_response,
            dbt_platform_url="https://auth.getdbt.com",
            context_manager=context_manager,
            refresh_strategy=mock_refresh_strategy,
        )

        # Replace the _refresh_token method with our mock
        token_manager._refresh_token = mock_refresh_token

        # Let the background worker run for a short time
        await asyncio.sleep(0.2)

        # Verify error retry was called
        assert mock_refresh_strategy.error_wait_calls >= 1

        # Verify refresh timing was also called
        assert mock_refresh_strategy.call_count >= 1

        # Cleanup
        token_manager._refresh_task.cancel()
        try:
            await token_manager._refresh_task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_context_manager_integration(
        self,
        sample_access_token_response,
        context_manager,
        mock_refresh_strategy,
        sample_decoded_claims,
        temp_config_dir,
    ):
        """Test that token refresh properly updates the context manager."""

        # Mock successful token refresh
        mock_token_response = {
            "access_token": "updated_access_token",
            "refresh_token": "updated_refresh_token",
            "expires_in": 3600,
            "scope": "read write",
            "token_type": "Bearer",
            "expires_at": int(time.time()) + 3600,
        }

        with (
            patch.object(
                OAuth2Session, "refresh_token", return_value=mock_token_response
            ),
            patch(
                "dbt_mcp.oauth.token_manager.fetch_jwks_and_verify_token",
                return_value=sample_decoded_claims,
            ),
        ):
            token_manager = OAuthTokenProvider(
                access_token_response=sample_access_token_response,
                dbt_platform_url="https://auth.getdbt.com",
                context_manager=context_manager,
                refresh_strategy=mock_refresh_strategy,
            )

            # Manually trigger refresh
            await token_manager._refresh_token()

            # Verify context file was updated (it should exist)
            config_file = temp_config_dir / "dbt_platform_context.yml"
            assert config_file.exists()

            # Verify the token was updated in the manager
            assert token_manager.get_token() == "updated_access_token"

        # Cleanup
        token_manager._refresh_task.cancel()
        try:
            await token_manager._refresh_task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_multiple_refresh_cycles(
        self,
        sample_access_token_response,
        context_manager,
        sample_decoded_claims,
    ):
        """Test multiple refresh cycles with fast mock timing."""

        # Create a mock strategy that triggers refresh quickly
        fast_mock_strategy = MockRefreshStrategy(should_wait=False)

        refresh_count = 0

        def mock_refresh_token_response(*args, **kwargs):
            nonlocal refresh_count
            refresh_count += 1
            return {
                "access_token": f"token_refresh_{refresh_count}",
                "refresh_token": f"refresh_token_{refresh_count}",
                "expires_in": 3600,
                "scope": "read write",
                "token_type": "Bearer",
                "expires_at": int(time.time()) + 3600,
            }

        with (
            patch.object(
                OAuth2Session, "refresh_token", side_effect=mock_refresh_token_response
            ),
            patch(
                "dbt_mcp.oauth.token_manager.fetch_jwks_and_verify_token",
                return_value=sample_decoded_claims,
            ),
        ):
            token_manager = OAuthTokenProvider(
                access_token_response=sample_access_token_response,
                dbt_platform_url="https://auth.getdbt.com",
                context_manager=context_manager,
                refresh_strategy=fast_mock_strategy,
            )

            # Let it run for a bit to trigger multiple refreshes
            await asyncio.sleep(0.2)

            # Should have triggered multiple refresh attempts
            assert fast_mock_strategy.call_count >= 2

            # Should have refreshed at least once
            assert refresh_count >= 1

            # Current token should be from a refresh
            current_token = token_manager.get_token()
            assert current_token.startswith("token_refresh_")

        # Cleanup
        token_manager._refresh_task.cancel()
        try:
            await token_manager._refresh_task
        except asyncio.CancelledError:
            pass
