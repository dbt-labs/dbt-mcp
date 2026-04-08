"""Tests for the env-var auth path's host prefix fetching from dbt Platform."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.config.credentials import (
    CredentialsProvider,
    _fetch_host_prefix_from_platform,
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
    async def test_graceful_when_api_returns_none(self):
        """When the API returns None, settings.host_prefix stays None (no crash)."""
        settings = self._make_settings(host_prefix=None, account_id=42)
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
