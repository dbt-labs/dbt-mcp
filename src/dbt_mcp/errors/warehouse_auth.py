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
        self._credentials_url_cache: dict[tuple[int, int | None], str] = {}

    async def get_hint(
        self,
        *,
        environment_id: int | None,
        developer_credentials: bool = False,
        user_id: int | None = None,
    ) -> str:
        """Build the message telling the user how to re-authenticate.

        Set ``developer_credentials`` for tools that run as the configured developer
        (e.g. execute_sql) rather than with Semantic Layer credentials, and pass that
        developer's ``user_id``. The credentials page is only linked when the platform
        token can read that user's credentials, i.e. it belongs to the same user.
        Without a ``user_id``, the owner of the platform token is used.
        """
        credentials_url = await self._get_credentials_url(environment_id, user_id)
        if credentials_url:
            return (
                "The warehouse authentication has expired. Ask the user to "
                f"re-connect their warehouse account at {credentials_url} "
                f"(see {SNOWFLAKE_REAUTH_FAQ_URL}), then retry."
            )
        if developer_credentials:
            return (
                "The warehouse authentication has expired. Re-connect the warehouse "
                "account of the configured developer from Profile settings > "
                f"Credentials in dbt platform (see {SNOWFLAKE_REAUTH_FAQ_URL}), "
                "then retry."
            )
        return (
            "The warehouse authentication has expired. If you authenticate with "
            "a personal access token or OAuth, re-connect the warehouse account "
            "from Profile settings > Credentials in dbt platform "
            f"(see {SNOWFLAKE_REAUTH_FAQ_URL}). If you use a service token, "
            "update the Semantic Layer credentials "
            f"(see {SEMANTIC_LAYER_CREDENTIALS_DOCS_URL}). Then retry."
        )

    async def _get_credentials_url(
        self, environment_id: int | None, user_id: int | None
    ) -> str | None:
        if environment_id is None:
            return None
        cache_key = (environment_id, user_id)
        if cache_key in self._credentials_url_cache:
            return self._credentials_url_cache[cache_key]
        try:
            url = await self._lookup_credentials_url(environment_id, user_id)
        except Exception:
            logger.warning(
                "Could not look up the credentials page for environment %s",
                environment_id,
                exc_info=True,
            )
            return None
        if url:
            self._credentials_url_cache[cache_key] = url
        return url

    async def _lookup_credentials_url(
        self, environment_id: int, user_id: int | None
    ) -> str | None:
        environment = await self._admin_client.get_environment(environment_id)
        project_id = environment.get("project_id")
        if user_id is None:
            current_user = await self._admin_client.get_current_user()
            user_id = (current_user.get("user") or {}).get("id")
        if project_id is None or user_id is None:
            return None
        # The API only lists a user's credentials to that user's own token, so this
        # also rejects service tokens and a token owned by someone else.
        user_credentials = await self._admin_client.list_user_credentials(user_id)
        if not any(
            entry.get("project_id") == project_id and entry.get("state") == 1
            for entry in user_credentials
        ):
            return None
        platform_url = (await self._admin_client.config_provider.get_config()).url
        # The credentials page in the UI is keyed by project ID
        return f"{platform_url}/settings/profile/credentials/{project_id}"


def append_hint(message: str, hint: str) -> str:
    """Add a hint to an error message, tagged so it reads as guidance, not error text."""
    return f"{message}\n\n<hint>{hint}</hint>"
