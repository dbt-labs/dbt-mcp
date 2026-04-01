from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from dbt_mcp.config.config_providers import AdminApiConfig
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.oauth.dbt_platform import DbtPlatformAccount
from dbt_mcp.project.project_resolver import get_all_projects_for_account
from tests.unit.dbt_admin.test_client import (
    MockAdminApiConfigProvider,
    MockHeadersProvider,
)


@pytest.fixture
def base_headers():
    return {"Accept": "application/json", "Authorization": "Bearer token"}


@pytest.fixture
def account():
    return DbtPlatformAccount(
        id=1,
        name="Account 1",
        locked=False,
        state=1,
        static_subdomain=None,
        vanity_subdomain=None,
    )


def create_mock_response(data: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


def create_mock_httpx_client(responses: list) -> AsyncMock:
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=responses)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    return mock_client


async def test_get_all_projects_for_account_paginates(base_headers, account):
    # Two pages: first full page (limit=2), second partial page (1 item) -> stop
    first_page_resp = create_mock_response(
        {
            "data": [
                {"id": 101, "name": "Proj A", "account_id": account.id},
                {"id": 102, "name": "Proj B", "account_id": account.id},
            ]
        }
    )
    second_page_resp = create_mock_response(
        {
            "data": [
                {"id": 103, "name": "Proj C", "account_id": account.id},
            ]
        }
    )

    mock_client = create_mock_httpx_client([first_page_resp, second_page_resp])

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await get_all_projects_for_account(
            dbt_platform_url="https://cloud.getdbt.com",
            account=account,
            headers=base_headers,
            page_size=2,
        )

    # Should aggregate 3 projects and include account_name field
    assert len(result) == 3
    assert {p.id for p in result} == {101, 102, 103}
    assert all(p.account_name == account.name for p in result)

    # Verify correct pagination URLs called
    expected_urls = [
        "https://cloud.getdbt.com/api/v3/accounts/1/projects/?state=1&offset=0&limit=2",
        "https://cloud.getdbt.com/api/v3/accounts/1/projects/?state=1&offset=2&limit=2",
    ]
    actual_urls = [call.args[0] for call in mock_client.get.call_args_list]
    assert actual_urls == expected_urls


async def test_fetch_project_environment_responses_paginates(base_headers):
    # Two pages: first full page (limit=2), second partial (1 item) -> stop
    first_page_resp = create_mock_response(
        {
            "data": [
                {"id": 201, "name": "Dev", "deployment_type": "development"},
                {"id": 202, "name": "Prod", "deployment_type": "production"},
            ]
        }
    )
    second_page_resp = create_mock_response(
        {
            "data": [
                {"id": 203, "name": "Staging", "deployment_type": "development"},
            ]
        }
    )

    admin_config = AdminApiConfig(
        url="https://cloud.getdbt.com",
        account_id=1,
        headers_provider=MockHeadersProvider(dict(base_headers)),
    )
    client = DbtAdminAPIClient(MockAdminApiConfigProvider(admin_config))

    mock_client = AsyncMock()
    mock_client.request = AsyncMock(
        side_effect=[first_page_resp, second_page_resp],
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await client.fetch_project_environment_responses(
            project_id=9,
            page_size=2,
        )

    assert len(result) == 3
    assert {e.id for e in result} == {201, 202, 203}

    base_url = "https://cloud.getdbt.com/api/v3/accounts/1/projects/9/environments/"
    headers = await client.get_headers()
    assert mock_client.request.call_args_list == [
        call(
            "GET",
            base_url,
            headers=headers,
            params={"state": 1, "offset": 0, "limit": 2},
        ),
        call(
            "GET",
            base_url,
            headers=headers,
            params={"state": 1, "offset": 2, "limit": 2},
        ),
    ]
