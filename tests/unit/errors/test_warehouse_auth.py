from unittest.mock import AsyncMock, MagicMock

import pytest

from dbt_mcp.errors.warehouse_auth import (
    SEMANTIC_LAYER_CREDENTIALS_DOCS_URL,
    SNOWFLAKE_REAUTH_FAQ_URL,
    WarehouseAuthHintProvider,
    is_warehouse_auth_error,
)

PLATFORM_URL = "https://vu491.us1.dbt.com"


@pytest.mark.parametrize(
    "message",
    [
        'QueryFailedError(message=" SSO authentication has expired, please '
        'run re-connect to Snowflake: https://docs.getdbt.com/faqs"), status=5)',
        "OAuth refresh token has expired",
        "Please re-connect to Snowflake",
        "AUTHENTICATION HAS EXPIRED",
    ],
)
def test_is_warehouse_auth_error_matches(message: str):
    assert is_warehouse_auth_error(message)


@pytest.mark.parametrize(
    "message",
    [
        "",
        "Invalid metric name: revenue",
        "SQL compilation error: object 'FOO' does not exist",
        "Query timed out after 30s",
    ],
)
def test_is_warehouse_auth_error_does_not_match(message: str):
    assert not is_warehouse_auth_error(message)


def _admin_client(
    *,
    project_id: int | None = 10,
    user_id: int | None = 5,
    user_credentials: list[dict] | None = None,
) -> MagicMock:
    client = MagicMock()
    client.config_provider.get_config = AsyncMock(
        return_value=MagicMock(url=PLATFORM_URL)
    )
    client.get_environment = AsyncMock(return_value={"project_id": project_id})
    client.get_current_user = AsyncMock(
        return_value={"user": {"id": user_id}} if user_id is not None else {}
    )
    client.list_user_credentials = AsyncMock(
        return_value=user_credentials
        if user_credentials is not None
        else [
            {"project_id": 99, "credentials_id": 1, "state": 1},
            {"project_id": 10, "credentials_id": 672, "state": 1},
        ]
    )
    return client


async def test_hint_links_to_credentials_page_keyed_by_project_id():
    provider = WarehouseAuthHintProvider(_admin_client())

    hint = await provider.get_hint(environment_id=20)

    assert f"{PLATFORM_URL}/settings/profile/credentials/10 " in hint
    assert "credentials/672" not in hint
    assert SNOWFLAKE_REAUTH_FAQ_URL in hint


async def test_hint_resolution_is_cached_per_environment():
    client = _admin_client()
    provider = WarehouseAuthHintProvider(client)

    await provider.get_hint(environment_id=20)
    await provider.get_hint(environment_id=20)

    client.get_environment.assert_awaited_once()
    client.list_user_credentials.assert_awaited_once()


@pytest.mark.parametrize(
    "client",
    [
        # no active credential for the project
        _admin_client(user_credentials=[{"project_id": 99, "state": 1}]),
        _admin_client(user_credentials=[{"project_id": 10, "state": 2}]),
        # project cannot be resolved from the environment
        _admin_client(project_id=None),
        # whoami returns no user (e.g. service token)
        _admin_client(user_id=None),
    ],
)
async def test_hint_falls_back_to_docs_when_link_cannot_be_built(client: MagicMock):
    hint = await WarehouseAuthHintProvider(client).get_hint(environment_id=20)

    assert "/settings/profile/credentials/" not in hint
    assert SEMANTIC_LAYER_CREDENTIALS_DOCS_URL in hint
    assert SNOWFLAKE_REAUTH_FAQ_URL in hint


@pytest.mark.parametrize(
    "failing_method",
    ["get_environment", "get_current_user", "list_user_credentials"],
)
async def test_hint_falls_back_to_docs_when_a_lookup_raises(failing_method: str):
    client = _admin_client()
    setattr(client, failing_method, AsyncMock(side_effect=RuntimeError("boom")))

    hint = await WarehouseAuthHintProvider(client).get_hint(environment_id=20)

    assert SEMANTIC_LAYER_CREDENTIALS_DOCS_URL in hint


async def test_hint_without_environment_id_falls_back_to_docs():
    client = _admin_client()

    hint = await WarehouseAuthHintProvider(client).get_hint(environment_id=None)

    client.get_environment.assert_not_called()
    assert SEMANTIC_LAYER_CREDENTIALS_DOCS_URL in hint
