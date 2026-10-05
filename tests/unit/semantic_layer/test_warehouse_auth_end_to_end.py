"""End-to-end check of the expired warehouse auth hint.

Wires the real config providers, hint provider and admin client together. Only the
HTTP layer (canned responses shaped like the real dbt platform API) and the failing
warehouse call are faked. If the platform response shapes or the error wording
change, update ``PLATFORM_RESPONSES`` / ``EXPIRED_AUTH_MESSAGE`` here and in
``dbt_mcp.errors.warehouse_auth``.
"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dbtsl.error import QueryFailedError

from dbt_mcp.config.config_providers.proxied_tool import (
    DefaultProxiedToolConfigProvider,
)
from dbt_mcp.config.config_providers.semantic_layer import (
    DefaultSemanticLayerConfigProvider,
)
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.proxy.tools import format_remote_tool_error
from dbt_mcp.semantic_layer.tools import (
    SemanticLayerToolContext,
    get_dimension_values,
    get_metrics_compiled_sql,
    query_metrics,
)
from dbt_mcp.semantic_layer.types import DimensionValuesError

# Verbatim from a real expired Snowflake OAuth connection
EXPIRED_AUTH_MESSAGE = (
    " SSO authentication has expired, please re-connect to Snowflake: "
    "https://docs.getdbt.com/faqs/Troubleshooting/refresh-snowflake-oauth-credentials "
)

PLATFORM_URL = "https://vu491.us1.dbt.com"
ACCOUNT_ID = 1
USER_ID = 54461
PROD_ENV_ID = 345198
DEV_ENV_ID = 999
# Deliberately different from every other ID so the link can't pass by coincidence
PROJECT_ID = 672

# Shapes captured from the live API (only the fields we read, plus neighbours)
PLATFORM_RESPONSES: dict[str, dict[str, Any]] = {
    f"/api/v2/accounts/{ACCOUNT_ID}/environments/{PROD_ENV_ID}/": {
        "data": {"id": PROD_ENV_ID, "project_id": PROJECT_ID, "name": "Production"},
        "status": {"code": 200},
    },
    f"/api/v2/accounts/{ACCOUNT_ID}/environments/{DEV_ENV_ID}/": {
        "data": {"id": DEV_ENV_ID, "project_id": PROJECT_ID, "name": "Development"},
        "status": {"code": 200},
    },
    "/api/v2/whoami/": {
        "data": {"id": None, "user": {"id": USER_ID, "email": "someone@example.com"}},
        "status": {"code": 200},
    },
    f"/api/v3/users/{USER_ID}/credentials/": {
        "data": [
            {
                "id": 48106,
                "credentials_id": 92608,
                "project_id": PROJECT_ID,
                "state": 1,
            },
            {"id": 277827, "credentials_id": 458499, "project_id": 310077, "state": 1},
        ],
        "status": {"code": 200},
    },
}

EXPECTED_LINK = f"{PLATFORM_URL}/settings/profile/credentials/{PROJECT_ID}"


@pytest.fixture
def credentials_provider() -> MagicMock:
    settings = SimpleNamespace(
        actual_host="us1.dbt.com",
        actual_host_prefix="vu491",
        base_host="us1.dbt.com",
        dbt_account_id=ACCOUNT_ID,
        actual_prod_environment_id=PROD_ENV_ID,
        dbt_dev_env_id=DEV_ENV_ID,
        dbt_user_id=USER_ID,
    )
    provider = MagicMock()
    provider.get_credentials = AsyncMock(
        return_value=(settings, StaticTokenProvider(token="token"))
    )
    return provider


@pytest.fixture(autouse=True)
def platform_api():
    async def fake_make_request(
        self: DbtAdminAPIClient, method: str, endpoint: str, **kwargs: Any
    ) -> dict[str, Any]:
        assert method == "GET"
        return PLATFORM_RESPONSES[endpoint]

    with patch.object(DbtAdminAPIClient, "_make_request", fake_make_request):
        yield


def _expired_auth_client_provider() -> MagicMock:
    sl_client = MagicMock()
    session = MagicMock()
    session.__enter__ = MagicMock(return_value=sl_client)
    session.__exit__ = MagicMock(return_value=False)
    sl_client.session.return_value = session
    error = QueryFailedError(message=EXPIRED_AUTH_MESSAGE, status=5)
    sl_client.query.side_effect = error
    sl_client.dimension_values.side_effect = error
    sl_client.compile_sql.side_effect = error
    client_provider = MagicMock()
    client_provider.get_client = AsyncMock(return_value=sl_client)
    return client_provider


@pytest.fixture
def tool_context(credentials_provider: MagicMock) -> SemanticLayerToolContext:
    return SemanticLayerToolContext(
        config_provider=DefaultSemanticLayerConfigProvider(credentials_provider),
        client_provider=_expired_auth_client_provider(),
    )


def _assert_points_to_credentials_page(text: str) -> None:
    assert "SSO authentication has expired" in text
    assert EXPECTED_LINK in text


async def test_query_metrics_tool_explains_how_to_reconnect(tool_context):
    result = await query_metrics.fn(context=tool_context, metrics=["revenue"])

    _assert_points_to_credentials_page(result)


async def test_get_metrics_compiled_sql_tool_explains_how_to_reconnect(tool_context):
    result = await get_metrics_compiled_sql.fn(
        context=tool_context, metrics=["revenue"]
    )

    _assert_points_to_credentials_page(result)


async def test_get_dimension_values_tool_explains_how_to_reconnect(tool_context):
    result = await get_dimension_values.fn(
        context=tool_context, dimension="region", metrics=["revenue"], limit=10
    )

    assert isinstance(result, DimensionValuesError)
    _assert_points_to_credentials_page(result.error)


async def test_execute_sql_error_explains_how_to_reconnect(credentials_provider):
    config = await DefaultProxiedToolConfigProvider(credentials_provider).get_config()

    message = await format_remote_tool_error(
        "execute_sql", f"[TextContent(text={EXPIRED_AUTH_MESSAGE!r})]", config
    )

    _assert_points_to_credentials_page(message)


async def test_service_token_without_user_credentials_gets_docs_fallback(
    tool_context,
):
    """Service tokens are rejected by the user-credentials endpoint."""

    async def rejecting_make_request(
        self: DbtAdminAPIClient, method: str, endpoint: str, **kwargs: Any
    ) -> dict[str, Any]:
        if endpoint.startswith("/api/v3/users/"):
            raise RuntimeError("403")
        return PLATFORM_RESPONSES[endpoint]

    with patch.object(DbtAdminAPIClient, "_make_request", rejecting_make_request):
        result = await query_metrics.fn(context=tool_context, metrics=["revenue"])

    assert "SSO authentication has expired" in result
    assert "/settings/profile/credentials/" not in result
    assert "setup-sl" in result
