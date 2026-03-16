from unittest.mock import AsyncMock, MagicMock, patch

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
                "dbt_mcp.config.credentials.get_dbt_platform_context",
                return_value=mock_dbt_context,
            ),
            patch(
                "dbt_mcp.config.credentials.get_dbt_host", return_value="cloud.getdbt.com"
            ),
            patch("dbt_mcp.config.credentials.OAuthTokenProvider") as mock_token_provider,
            patch("dbt_mcp.config.settings.validate_dbt_cli_settings", return_value=[]),
        ):
            mock_provider_instance = MagicMock()
            mock_token_provider.create = AsyncMock(return_value=mock_provider_instance)

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


class TestCredentialsProviderOAuthDoesNotSetDbtToken:
    """OAuth path should not mutate settings.dbt_token."""

    @pytest.mark.asyncio
    async def test_oauth_path_does_not_set_dbt_token(self):
        """After OAuth credential setup, settings.dbt_token must remain None."""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_host="cloud.getdbt.com",
            dbt_prod_env_id=123,
            dbt_account_id=456,
            dbt_token=None,
        )

        credentials_provider = CredentialsProvider(mock_settings)

        mock_dbt_context = MagicMock()
        mock_dbt_context.account_id = 456
        mock_dbt_context.host_prefix = ""
        mock_dbt_context.user_id = 789
        mock_dbt_context.dev_environment.id = 111
        mock_dbt_context.prod_environment.id = 123
        mock_decoded_token = MagicMock()
        mock_decoded_token.access_token_response.access_token = "mock_oauth_token"
        mock_dbt_context.decoded_access_token = mock_decoded_token

        with (
            patch(
                "dbt_mcp.config.credentials.get_dbt_platform_context",
                return_value=mock_dbt_context,
            ),
            patch(
                "dbt_mcp.config.credentials.get_dbt_host", return_value="cloud.getdbt.com"
            ),
            patch("dbt_mcp.config.credentials.OAuthTokenProvider") as mock_tp_cls,
            patch("dbt_mcp.config.settings.validate_dbt_cli_settings", return_value=[]),
        ):
            mock_tp_cls.create = AsyncMock(return_value=MagicMock())

            settings, _ = await credentials_provider.get_credentials()

            # settings.dbt_token must NOT have been set to the OAuth access token
            assert settings.dbt_token is None

    @pytest.mark.asyncio
    async def test_oauth_path_uses_factory_with_background_refresh(self):
        """OAuth path must use the create() factory which starts background refresh."""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_host="cloud.getdbt.com",
            dbt_prod_env_id=123,
            dbt_account_id=456,
            dbt_token=None,
        )

        credentials_provider = CredentialsProvider(mock_settings)

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
                "dbt_mcp.config.credentials.get_dbt_platform_context",
                return_value=mock_dbt_context,
            ),
            patch(
                "dbt_mcp.config.credentials.get_dbt_host", return_value="cloud.getdbt.com"
            ),
            patch("dbt_mcp.config.credentials.OAuthTokenProvider") as mock_tp_cls,
            patch("dbt_mcp.config.settings.validate_dbt_cli_settings", return_value=[]),
        ):
            mock_provider_instance = MagicMock()
            mock_tp_cls.create = AsyncMock(return_value=mock_provider_instance)

            await credentials_provider.get_credentials()

            mock_tp_cls.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_oauth_path_does_not_call_validate_settings(self):
        """OAuth path should not call validate_settings (which checks dbt_token).

        The OAuth path validates CLI settings directly instead, since
        dbt_token is not used when a token provider supplies the token.
        """
        mock_settings = DbtMcpSettings.model_construct(
            dbt_host="cloud.getdbt.com",
            dbt_prod_env_id=123,
            dbt_account_id=456,
            dbt_token=None,
        )

        credentials_provider = CredentialsProvider(mock_settings)

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
                "dbt_mcp.config.credentials.get_dbt_platform_context",
                return_value=mock_dbt_context,
            ),
            patch(
                "dbt_mcp.config.credentials.get_dbt_host", return_value="cloud.getdbt.com"
            ),
            patch("dbt_mcp.config.credentials.OAuthTokenProvider") as mock_tp_cls,
            patch("dbt_mcp.config.settings.validate_settings") as mock_validate,
            patch("dbt_mcp.config.settings.validate_dbt_cli_settings", return_value=[]),
        ):
            mock_tp_cls.create = AsyncMock(return_value=MagicMock())

            await credentials_provider.get_credentials()

            mock_validate.assert_not_called()
