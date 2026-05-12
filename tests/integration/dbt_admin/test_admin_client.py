import os

import pytest

from dbt_mcp.config.config_providers.admin_api import DefaultAdminApiConfigProvider
from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient


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
