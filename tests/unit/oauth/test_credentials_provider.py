from unittest.mock import MagicMock, patch

import pytest

from dbt_mcp.config.settings import (
    AuthenticationMethod,
    CredentialsProvider,
    DbtMcpSettings,
)


class TestCredentialsProviderAuthenticationMethod:
    """Test the authentication_method field on CredentialsProvider"""

    @pytest.mark.asyncio
    async def test_authentication_method_oauth(self):
        """Test that authentication_method is set to OAUTH when using OAuth flow"""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_host="cloud.getdbt.com",
            dbt_prod_env_id=123,
            dbt_account_id=456,
            dbt_token=None,  # No token means OAuth
        )

        credentials_provider = CredentialsProvider(mock_settings)

        # Mock OAuth flow - create a properly structured context
        mock_dbt_context = MagicMock()
        mock_dbt_context.account_id = 456
        mock_dbt_context.host_prefix = ""
        mock_dbt_context.user_id = 789
        mock_dbt_context.dev_environment.id = 111
        mock_dbt_context.prod_environment.id = 123
        mock_decoded_token = MagicMock()
        mock_decoded_token.access_token_response.access_token = "mock_token"
        mock_dbt_context.decoded_access_token = mock_decoded_token

        with (
            patch(
                "dbt_mcp.config.settings.get_dbt_platform_context",
                return_value=mock_dbt_context,
            ),
            patch(
                "dbt_mcp.config.settings.get_dbt_host", return_value="cloud.getdbt.com"
            ),
            patch("dbt_mcp.config.settings.OAuthTokenProvider") as mock_token_provider,
            patch("dbt_mcp.config.settings.validate_settings"),
        ):
            mock_token_provider.return_value = MagicMock()

            settings, token_provider = await credentials_provider.get_credentials()

            assert (
                credentials_provider.authentication_method == AuthenticationMethod.OAUTH
            )
            assert token_provider is not None

    @pytest.mark.asyncio
    async def test_authentication_method_env_var(self):
        """Test that authentication_method is set to ENV_VAR when using token from env"""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_host="test.dbt.com",
            dbt_prod_env_id=123,
            dbt_token="test_token",  # Token provided
        )

        credentials_provider = CredentialsProvider(mock_settings)

        with patch("dbt_mcp.config.settings.validate_settings"):
            settings, token_provider = await credentials_provider.get_credentials()

            assert (
                credentials_provider.authentication_method
                == AuthenticationMethod.ENV_VAR
            )
            assert token_provider is not None

    @pytest.mark.asyncio
    async def test_authentication_method_initially_none(self):
        """Test that authentication_method starts as None before get_credentials is called"""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_token="test_token",
        )

        credentials_provider = CredentialsProvider(mock_settings)

        assert credentials_provider.authentication_method is None

    @pytest.mark.asyncio
    async def test_authentication_method_persists_after_get_credentials(self):
        """Test that authentication_method persists after get_credentials is called"""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_host="test.dbt.com",
            dbt_prod_env_id=123,
            dbt_token="test_token",
        )

        credentials_provider = CredentialsProvider(mock_settings)

        with patch("dbt_mcp.config.settings.validate_settings"):
            # First call
            await credentials_provider.get_credentials()
            assert (
                credentials_provider.authentication_method
                == AuthenticationMethod.ENV_VAR
            )

            # Second call - should still be set
            await credentials_provider.get_credentials()
            assert (
                credentials_provider.authentication_method
                == AuthenticationMethod.ENV_VAR
            )
