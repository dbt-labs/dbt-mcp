import os

import pytest

from dbt_mcp.config.config_providers.admin_api import DefaultAdminApiConfigProvider
from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.errors import InvalidParameterError
from dbt_mcp.errors.warehouse_auth import (
    SNOWFLAKE_REAUTH_FAQ_URL,
    WarehouseAuthHintProvider,
)


@pytest.fixture
def admin_client() -> DbtAdminAPIClient:
    host = os.getenv("DBT_HOST")
    token = os.getenv("DBT_TOKEN")
    account_id = os.getenv("DBT_ACCOUNT_ID")
    if not host or not token or not account_id:
        pytest.skip(
            "DBT_HOST, DBT_TOKEN, and DBT_ACCOUNT_ID environment variables are required"
        )
    settings = DbtMcpSettings()  # type: ignore
    credentials_provider = CredentialsProvider(settings)
    return DbtAdminAPIClient(DefaultAdminApiConfigProvider(credentials_provider))


@pytest.mark.asyncio
async def test_get_current_user(admin_client: DbtAdminAPIClient) -> None:
    result = await admin_client.get_current_user()
    assert isinstance(result, dict)
    assert "user" in result
    user = result["user"]
    assert isinstance(user, dict)
    assert "id" in user
    assert isinstance(user["id"], int)


@pytest.mark.asyncio
async def test_list_jobs_with_project_filter(
    admin_client: DbtAdminAPIClient,
) -> None:
    account_id = int(os.environ["DBT_ACCOUNT_ID"])
    jobs = await admin_client.list_jobs(account_id)
    assert jobs, "The integration test account must contain at least one job"

    project_id = jobs[0]["project_id"]
    assert isinstance(project_id, int)

    project_jobs = await admin_client.list_jobs(account_id, project_id=project_id)
    assert project_jobs
    assert all(job["project_id"] == project_id for job in project_jobs)


@pytest.fixture
def environment_id() -> int:
    value = os.getenv("DBT_PROD_ENV_ID")
    if not value:
        pytest.skip("DBT_PROD_ENV_ID environment variable is required")
    return int(value)


@pytest.mark.asyncio
async def test_get_environment_returns_project_id(
    admin_client: DbtAdminAPIClient, environment_id: int
) -> None:
    result = await admin_client.get_environment(environment_id)
    assert result["id"] == environment_id
    assert isinstance(result["project_id"], int)


@pytest.mark.asyncio
async def test_list_user_credentials_shape(admin_client: DbtAdminAPIClient) -> None:
    user_id = (await admin_client.get_current_user())["user"]["id"]
    try:
        credentials = await admin_client.list_user_credentials(user_id)
    except InvalidParameterError:
        pytest.skip("Service tokens cannot list user credentials")
    for entry in credentials:
        assert isinstance(entry["project_id"], int)
        assert entry["state"] in (1, 2)


@pytest.mark.asyncio
async def test_warehouse_auth_hint_against_live_api(
    admin_client: DbtAdminAPIClient, environment_id: int
) -> None:
    """The hint must point to the user's credentials page when they have one."""
    project_id = (await admin_client.get_environment(environment_id))["project_id"]
    user_id = (await admin_client.get_current_user())["user"]["id"]
    try:
        has_credentials = any(
            entry["project_id"] == project_id and entry["state"] == 1
            for entry in await admin_client.list_user_credentials(user_id)
        )
    except InvalidParameterError:
        has_credentials = False

    hint = await WarehouseAuthHintProvider(admin_client).get_hint(
        environment_id=environment_id
    )

    assert SNOWFLAKE_REAUTH_FAQ_URL in hint
    assert (f"/settings/profile/credentials/{project_id} " in hint) == has_credentials
