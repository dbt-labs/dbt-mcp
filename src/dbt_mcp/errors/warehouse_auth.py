from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_mcp.dbt_admin.client import DbtAdminAPIClient

logger = logging.getLogger(__name__)

SNOWFLAKE_REAUTH_FAQ_URL = (
    "https://docs.getdbt.com/faqs/Troubleshooting/refresh-snowflake-oauth-credentials"
)
SEMANTIC_LAYER_CREDENTIALS_DOCS_URL = (
    "https://docs.getdbt.com/docs/use-dbt-semantic-layer/setup-sl?version=2"
    "#2-configure-credentials-and-create-tokens"
)

_WAREHOUSE_AUTH_ERROR_PATTERN = re.compile(
    r"authentication has expired"
    r"|re-?connect to (snowflake|your)"
    r"|(oauth|refresh)[\w\s]{0,20}token[\w\s]{0,20}(expired|revoked|invalid)",
    re.IGNORECASE,
)


def is_warehouse_auth_error(message: str) -> bool:
    """Whether an error message says the warehouse connection must be re-authenticated."""
    return bool(_WAREHOUSE_AUTH_ERROR_PATTERN.search(message))


class WarehouseAuthHintProvider:
    """Builds an actionable message for expired warehouse (e.g. Snowflake OAuth) auth.

    Lookups only happen when an auth error occurs. Any lookup failure degrades to a
    docs-only hint so the original warehouse error is never hidden.
    """

    def __init__(self, admin_client: DbtAdminAPIClient):
        self._admin_client = admin_client
        self._credentials_url_cache: dict[int, str] = {}

    async def get_hint(self, *, environment_id: int | None) -> str:
        credentials_url = await self._get_credentials_url(environment_id)
        if credentials_url:
            return (
                "The warehouse authentication has expired. Ask the user to "
                f"re-connect their warehouse account at {credentials_url} "
                f"(see {SNOWFLAKE_REAUTH_FAQ_URL}), then retry."
            )
        return (
            "The warehouse authentication has expired. If you authenticate with "
            "a personal access token or OAuth, re-connect the warehouse account "
            "from Profile settings > Credentials in dbt platform "
            f"(see {SNOWFLAKE_REAUTH_FAQ_URL}). If you use a service token, "
            "update the Semantic Layer credentials "
            f"(see {SEMANTIC_LAYER_CREDENTIALS_DOCS_URL}). Then retry."
        )

    async def _get_credentials_url(self, environment_id: int | None) -> str | None:
        if environment_id is None:
            return None
        if environment_id in self._credentials_url_cache:
            return self._credentials_url_cache[environment_id]
        try:
            url = await self._lookup_credentials_url(environment_id)
        except Exception:
            logger.warning(
                "Could not look up the credentials page for environment %s",
                environment_id,
                exc_info=True,
            )
            return None
        if url:
            self._credentials_url_cache[environment_id] = url
        return url

    async def _lookup_credentials_url(self, environment_id: int) -> str | None:
        environment = await self._admin_client.get_environment(environment_id)
        project_id = environment.get("project_id")
        user_id = ((await self._admin_client.get_current_user()).get("user") or {}).get(
            "id"
        )
        if project_id is None or user_id is None:
            return None
        # Only users with their own development credentials for this project have
        # a credentials page to reconnect; service tokens are rejected by the API.
        user_credentials = await self._admin_client.list_user_credentials(user_id)
        if not any(
            entry.get("project_id") == project_id and entry.get("state") == 1
            for entry in user_credentials
        ):
            return None
        platform_url = (await self._admin_client.config_provider.get_config()).url
        # The credentials page in the UI is keyed by project ID
        return f"{platform_url}/settings/profile/credentials/{project_id}"
