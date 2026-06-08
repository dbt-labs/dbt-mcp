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
        """Fetch the current onboarding model for the account.

        Returns the raw data dict, or None if no onboarding exists yet.
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
                    f"Onboarding request failed ({e.response.status_code}) for account {account_id}"
                ) from e
            raise AdminAPIError(
                f"Onboarding request failed ({e.response.status_code}) for account {account_id}"
            ) from e
        except httpx.HTTPError as e:
            raise AdminAPIError(
                f"Onboarding request failed for account {account_id}"
            ) from e

    async def create_or_get(self, account_id: int) -> dict[str, Any]:
        """Create the onboarding model if it doesn't exist, or return the existing one.

        POST is idempotent on the backend — returns the existing record if one is present.
        """
        config = await self.config_provider.get_config()
        url = self._base_url(config, account_id)
        headers = await self._headers(config)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url, headers=headers, json={}, follow_redirects=True
                )
                response.raise_for_status()
                return response.json().get("data", {})
        except httpx.HTTPStatusError as e:
            if 400 <= e.response.status_code < 500:
                raise InvalidParameterError(
                    f"Onboarding create failed ({e.response.status_code}) for account {account_id}"
                ) from e
            raise AdminAPIError(
                f"Onboarding create failed ({e.response.status_code}) for account {account_id}"
            ) from e
        except httpx.HTTPError as e:
            raise AdminAPIError(
                f"Onboarding create failed for account {account_id}"
            ) from e
