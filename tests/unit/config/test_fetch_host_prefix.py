"""Tests for the env-var auth path's host prefix fetching from dbt Platform."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.config.credentials import (
    CredentialsProvider,
    _fetch_host_prefix_from_platform,
    _infer_prefix_from_host,
)
from dbt_mcp.config.settings import DbtMcpSettings


class TestFetchHostPrefixFromPlatform:
    """Unit tests for the _fetch_host_prefix_from_platform helper."""

    @pytest.mark.asyncio
    async def test_returns_prefix_from_account(self):
        """Returns static_subdomain-based prefix when account fetch succeeds."""
        mock_account = MagicMock()
        mock_account.host_prefix = "ab123"

        with patch(
            "dbt_mcp.config.credentials.get_account",
            new=AsyncMock(return_value=mock_account),
        ):
            result = await _fetch_host_prefix_from_platform(
                dbt_platform_url="https://us1.dbt.com",
                account_id=42,
                token="my-token",
            )

        assert result == "ab123"

    @pytest.mark.asyncio
    async def test_returns_none_when_account_has_no_prefix(self):
        """Returns None when the account exists but has no host_prefix."""
        mock_account = MagicMock()
        mock_account.host_prefix = None

        with patch(
            "dbt_mcp.config.credentials.get_account",
            new=AsyncMock(return_value=mock_account),
        ):
            result = await _fetch_host_prefix_from_platform(
                dbt_platform_url="https://us1.dbt.com",
                account_id=42,
                token="my-token",
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_api_error_and_logs_warning(
        self, caplog: pytest.LogCaptureFixture
    ):
        """Returns None gracefully and logs a warning when the API call fails."""
        with patch(
            "dbt_mcp.config.credentials.get_account",
            new=AsyncMock(side_effect=Exception("network error")),
        ):
            with caplog.at_level(logging.WARNING, logger="dbt_mcp.config.credentials"):
                result = await _fetch_host_prefix_from_platform(
                    dbt_platform_url="https://us1.dbt.com",
                    account_id=42,
                    token="my-token",
                )

        assert result is None
        assert "DBT_HOST_PREFIX" in caplog.text

    @pytest.mark.asyncio
    async def test_passes_bearer_token_in_headers(self):
        """The token is passed as a bearer/service token auth header."""
        mock_account = MagicMock()
        mock_account.host_prefix = "ab123"
        captured_calls: list[dict] = []

        async def capture_get_account(**kwargs: object) -> MagicMock:
            captured_calls.append(dict(kwargs))
            return mock_account

        with patch(
            "dbt_mcp.config.credentials.get_account",
            new=capture_get_account,
        ):
            await _fetch_host_prefix_from_platform(
                dbt_platform_url="https://us1.dbt.com",
                account_id=42,
                token="my-token",
            )

        assert len(captured_calls) == 1
        headers = captured_calls[0]["headers"]
        assert headers["Authorization"] == "Token my-token"


class TestCredentialsProviderEnvVarPrefixFetch:
    """Integration tests: env var auth path fetches host prefix from platform when unset."""

    def _make_settings(
        self,
        *,
        host: str = "us1.dbt.com",
        token: str = "my-token",
        account_id: int | None = 42,
        prod_env_id: int = 123,
        host_prefix: str | None = None,
        multicell_account_prefix: str | None = None,
    ) -> DbtMcpSettings:
        return DbtMcpSettings.model_construct(
            dbt_host=host,
            dbt_token=token,
            dbt_account_id=account_id,
            dbt_prod_env_id=prod_env_id,
            host_prefix=host_prefix,
            multicell_account_prefix=multicell_account_prefix,
            disable_semantic_layer=True,
            disable_discovery=True,
            disable_admin_api=True,
            disable_sql=True,
            disable_dbt_cli=True,
        )

    @pytest.mark.asyncio
    async def test_fetches_and_applies_prefix_when_unset(self):
        """When host_prefix is unset, prefix is fetched and dbt_host is normalized to the base host."""
        settings = self._make_settings(
            host="ab123.us1.dbt.com", host_prefix=None, account_id=42
        )
        provider = CredentialsProvider(settings)

        with (
            patch(
                "dbt_mcp.config.credentials._fetch_host_prefix_from_platform",
                new=AsyncMock(return_value="ab123"),
            ),
            patch("dbt_mcp.config.settings.validate_settings"),
        ):
            returned_settings, _ = await provider.get_credentials()

        assert returned_settings.host_prefix == "ab123"
        assert returned_settings.dbt_host == "us1.dbt.com"
        assert returned_settings.base_host == "us1.dbt.com"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"host_prefix": "existing-prefix"},
            {"multicell_account_prefix": "legacy-prefix"},
            {"account_id": None},
        ],
        ids=["host_prefix_set", "multicell_prefix_set", "no_account_id"],
    )
    async def test_does_not_fetch_when_prefix_configured_or_no_account_id(
        self, kwargs: dict
    ):
        """No API call is made when a prefix is already configured or account_id is missing."""
        settings = self._make_settings(**kwargs)
        provider = CredentialsProvider(settings)

        mock_fetch = AsyncMock(return_value="should-not-be-used")
        with (
            patch(
                "dbt_mcp.config.credentials._fetch_host_prefix_from_platform",
                new=mock_fetch,
            ),
            patch("dbt_mcp.config.settings.validate_settings"),
        ):
            await provider.get_credentials()

        mock_fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetches_and_applies_prefix_with_clean_base_host(self):
        """When DBT_HOST is already a clean base host (no embedded prefix), the fetched prefix is
        applied to host_prefix and dbt_host is left unchanged."""
        settings = self._make_settings(
            host="us1.dbt.com", host_prefix=None, account_id=42
        )
        provider = CredentialsProvider(settings)

        with (
            patch(
                "dbt_mcp.config.credentials._fetch_host_prefix_from_platform",
                new=AsyncMock(return_value="ab123"),
            ),
            patch("dbt_mcp.config.settings.validate_settings"),
        ):
            returned_settings, _ = await provider.get_credentials()

        assert returned_settings.host_prefix == "ab123"
        assert returned_settings.dbt_host == "us1.dbt.com"
        assert returned_settings.base_host == "us1.dbt.com"

    @pytest.mark.asyncio
    async def test_graceful_when_api_returns_none_3label_host(self):
        """With a 3-label host, API returning None leaves host_prefix unset (no inference applies)."""
        settings = self._make_settings(
            host="us1.dbt.com", host_prefix=None, account_id=42
        )
        provider = CredentialsProvider(settings)

        with (
            patch(
                "dbt_mcp.config.credentials._fetch_host_prefix_from_platform",
                new=AsyncMock(return_value=None),
            ),
            patch("dbt_mcp.config.settings.validate_settings"),
        ):
            returned_settings, _ = await provider.get_credentials()

        assert returned_settings.host_prefix is None


class TestInferPrefixFromHost:
    """Unit tests for the _infer_prefix_from_host helper."""

    @pytest.mark.parametrize(
        "host, expected",
        [
            ("di041.us1.dbt.com", "di041"),
            ("ab123.eu1.dbt.com", "ab123"),
            ("us1.dbt.com", None),  # 3-label — no prefix to infer
            ("cloud.getdbt.com", None),  # non-dbt.com domain
            (
                "us.staging.dbt.com",
                None,
            ),  # 4-label but cell label is not a valid cell shape
            ("a.b.c.d.dbt.com", None),  # 5-label — not matched
        ],
        ids=[
            "4label_di041",
            "4label_ab123",
            "3label",
            "getdbt",
            "staging_4label",
            "5label",
        ],
    )
    def test_infer_prefix_from_host(self, host: str, expected: str | None):
        assert _infer_prefix_from_host(host) == expected


class TestCredentialsProviderPrefixFallbackInference:
    """When auto-fetch returns None and DBT_HOST is 4-label, prefix is inferred from the host."""

    def _make_settings(
        self,
        *,
        host: str,
        account_id: int | None = 42,
        prod_env_id: int = 123,
    ) -> DbtMcpSettings:
        return DbtMcpSettings.model_construct(
            dbt_host=host,
            dbt_token="my-token",
            dbt_account_id=account_id,
            dbt_prod_env_id=prod_env_id,
            host_prefix=None,
            multicell_account_prefix=None,
            disable_semantic_layer=True,
            disable_discovery=True,
            disable_admin_api=True,
            disable_sql=True,
            disable_dbt_cli=True,
        )

    @pytest.mark.asyncio
    async def test_infers_prefix_from_4label_host_when_fetch_returns_none(
        self, caplog: pytest.LogCaptureFixture
    ):
        """When fetch returns None and DBT_HOST is 4-label, prefix is inferred and dbt_host is stripped."""
        settings = self._make_settings(host="di041.us1.dbt.com")
        provider = CredentialsProvider(settings)

        with (
            patch(
                "dbt_mcp.config.credentials._fetch_host_prefix_from_platform",
                new=AsyncMock(return_value=None),
            ),
            patch("dbt_mcp.config.settings.validate_settings"),
            caplog.at_level(logging.WARNING, logger="dbt_mcp.config.credentials"),
        ):
            returned_settings, _ = await provider.get_credentials()

        assert returned_settings.host_prefix == "di041"
        assert returned_settings.dbt_host == "us1.dbt.com"
        assert "Inferred prefix 'di041' from DBT_HOST" in caplog.text
        assert "DBT_HOST_PREFIX explicitly" in caplog.text

    @pytest.mark.asyncio
    async def test_inferred_prefix_produces_correct_discovery_url(self):
        """After inference, actual_host_prefix and base_host combine to the right discovery URL."""
        from dbt_mcp.config.config_providers.discovery import (
            DefaultDiscoveryConfigProvider,
        )
        from dbt_mcp.config.credentials import CredentialsProvider

        settings = self._make_settings(host="di041.us1.dbt.com", prod_env_id=999)
        provider = CredentialsProvider(settings)

        with (
            patch(
                "dbt_mcp.config.credentials._fetch_host_prefix_from_platform",
                new=AsyncMock(return_value=None),
            ),
            patch("dbt_mcp.config.settings.validate_settings"),
        ):
            await provider.get_credentials()

        discovery_provider = DefaultDiscoveryConfigProvider(provider)

        with patch(
            "dbt_mcp.config.credentials.CredentialsProvider.get_credentials",
            return_value=(provider.settings, provider.token_provider),
        ):
            config = await discovery_provider.get_config()

        assert config.url == "https://di041.metadata.us1.dbt.com/graphql"

    @pytest.mark.asyncio
    async def test_no_inference_for_3label_host_logs_observability_warning(
        self, caplog: pytest.LogCaptureFixture
    ):
        """With a 3-label host, no inference happens and the observability warning is logged."""
        settings = self._make_settings(host="us1.dbt.com")
        provider = CredentialsProvider(settings)

        with (
            patch(
                "dbt_mcp.config.credentials._fetch_host_prefix_from_platform",
                new=AsyncMock(return_value=None),
            ),
            patch("dbt_mcp.config.settings.validate_settings"),
            caplog.at_level(logging.WARNING, logger="dbt_mcp.config.credentials"),
        ):
            returned_settings, _ = await provider.get_credentials()

        assert returned_settings.host_prefix is None
        assert "Discovery and Semantic Layer URLs may be incorrect" in caplog.text
