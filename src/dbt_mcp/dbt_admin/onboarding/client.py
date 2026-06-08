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

    async def get_state(self, account_id: int) -> dict[str, Any]:
        """Fetch the server-side onboarding state for the account.

        Returns the OnboardingSession row: applied resource IDs and status.
        """
        config = await self.config_provider.get_config()
        url = f"{config.url}/api/v3/accounts/{account_id}/onboarding/state/"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        } | config.headers_provider.get_headers()

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url, headers=headers, follow_redirects=True
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            if 400 <= e.response.status_code < 500:
                raise InvalidParameterError(
                    f"Onboarding state request failed ({e.response.status_code}) "
                    f"for account {account_id}"
                ) from e
            raise AdminAPIError(
                f"Onboarding state request failed ({e.response.status_code}) "
                f"for account {account_id}"
            ) from e
        except httpx.HTTPError as e:
            raise AdminAPIError(
                f"Onboarding state request failed for account {account_id}"
            ) from e
