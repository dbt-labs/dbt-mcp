from __future__ import annotations

import logging
from typing import Any

import httpx

from dbt_mcp.config.config_providers import AdminApiConfig, ConfigProvider
from dbt_mcp.errors import AdminAPIError, InvalidParameterError

logger = logging.getLogger(__name__)


class OnboardingClient:
    """HTTP client for the dbt platform onboarding API.

    Reuses AdminApiConfig (same base URL + auth headers) since the onboarding
    endpoints live under /api/v3/accounts/{account_id}/onboarding/.
    """

    def __init__(self, config_provider: ConfigProvider[AdminApiConfig]) -> None:
        self.config_provider = config_provider

    def _base_url(self, config: AdminApiConfig, account_id: int) -> str:
        return f"{config.url}/api/v3/accounts/{account_id}/onboarding/"

    async def _headers(self, config: AdminApiConfig) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
        } | config.headers_provider.get_headers()

    async def get(self, account_id: int) -> dict[str, Any] | None:
        """Fetch the current onboarding record for the account.

        Returns the raw data dict, or None if no onboarding exists yet (404).
        """
        config = await self.config_provider.get_config()
        url = self._base_url(config, account_id)
        headers = await self._headers(config)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers, follow_redirects=True)
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                return response.json().get("data")
        except httpx.HTTPStatusError as e:
            if 400 <= e.response.status_code < 500:
                raise InvalidParameterError(
                    f"Onboarding get failed ({e.response.status_code}) for account {account_id}"
                ) from e
            raise AdminAPIError(
                f"Onboarding get failed ({e.response.status_code}) for account {account_id}"
            ) from e
        except httpx.HTTPError as e:
            raise AdminAPIError(
                f"Onboarding get failed for account {account_id}"
            ) from e

    async def validate(self, account_id: int, data: dict[str, Any]) -> dict[str, Any]:
        """Validate onboarding data without applying it.

        Returns a dict with 'valid' bool and 'errors' list.
        """
        config = await self.config_provider.get_config()
        url = self._base_url(config, account_id)
        headers = await self._headers(config)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url, headers=headers, json=data, params={"dry_run": "1"}, follow_redirects=True
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            if 400 <= e.response.status_code < 500:
                # Validation errors are expected — return them as structured data
                try:
                    return response.json()
                except Exception:
                    pass
                raise InvalidParameterError(
                    f"Onboarding validate failed ({e.response.status_code}) for account {account_id}"
                ) from e
            raise AdminAPIError(
                f"Onboarding validate failed ({e.response.status_code}) for account {account_id}"
            ) from e
        except httpx.HTTPError as e:
            raise AdminAPIError(
                f"Onboarding validate failed for account {account_id}"
            ) from e

    async def apply(self, account_id: int, data: dict[str, Any]) -> dict[str, Any]:
        """Submit onboarding data. Idempotent — safe to call multiple times as data is gathered."""
        config = await self.config_provider.get_config()
        url = self._base_url(config, account_id)
        headers = await self._headers(config)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url, headers=headers, json=data, follow_redirects=True
                )
                response.raise_for_status()
                return response.json().get("data", {})
        except httpx.HTTPStatusError as e:
            if 400 <= e.response.status_code < 500:
                raise InvalidParameterError(
                    f"Onboarding apply failed ({e.response.status_code}) for account {account_id}"
                ) from e
            raise AdminAPIError(
                f"Onboarding apply failed ({e.response.status_code}) for account {account_id}"
            ) from e
        except httpx.HTTPError as e:
            raise AdminAPIError(
                f"Onboarding apply failed for account {account_id}"
            ) from e
