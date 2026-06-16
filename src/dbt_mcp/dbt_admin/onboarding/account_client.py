from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import httpx

from dbt_mcp.errors import AdminAPIError, InvalidParameterError

if TYPE_CHECKING:
    from dbt_mcp.config.credentials import CredentialsProvider

logger = logging.getLogger(__name__)


class AccountClient:
    """HTTP client for the public, unauthenticated account-create endpoint.

    Account creation happens *before* any account or token exists, so this
    client only needs the host — it deliberately does NOT call
    ``credentials_provider.get_credentials()`` (which would trigger the OAuth
    login flow). It reads the host straight off the configured settings.
    """

    def __init__(self, credentials_provider: CredentialsProvider) -> None:
        self.credentials_provider = credentials_provider

    def _url(self) -> str:
        settings = self.credentials_provider.settings
        host = settings.actual_host
        if not host:
            raise InvalidParameterError(
                "DBT_HOST is required to create an account but is not configured."
            )
        if settings.actual_host_prefix:
            base = f"https://{settings.actual_host_prefix}.{settings.base_host}"
        else:
            base = f"https://{host}"
        return f"{base}/api/v3/accounts/"

    async def create(
        self,
        *,
        name: str,
        owner_email: str,
        created_via: str | None = None,
    ) -> dict[str, Any]:
        """Create a trial account + owner token. Returns the response ``data`` dict."""
        url = self._url()
        body: dict[str, Any] = {"name": name, "owner_email": owner_email}
        if created_via is not None:
            body["created_via"] = created_via

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                    json=body,
                    follow_redirects=True,
                )
                response.raise_for_status()
                data = response.json().get("data")
                if not data:
                    raise AdminAPIError("Account create returned no data")
                return data
        except httpx.HTTPStatusError as e:
            if 400 <= e.response.status_code < 500:
                raise InvalidParameterError(
                    f"Account create failed ({e.response.status_code})"
                ) from e
            raise AdminAPIError(
                f"Account create failed ({e.response.status_code})"
            ) from e
        except httpx.HTTPError as e:
            raise AdminAPIError("Account create failed") from e
