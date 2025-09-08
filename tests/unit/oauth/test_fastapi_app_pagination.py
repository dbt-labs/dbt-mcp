from unittest.mock import Mock, patch

import pytest

from dbt_mcp.oauth.dbt_platform import DbtPlatformAccount
from dbt_mcp.oauth.fastapi_app import (
    _get_all_environments_for_project,
    _get_all_projects_for_account,
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


@patch("dbt_mcp.oauth.fastapi_app.requests.get")
def test_get_all_projects_for_account_paginates(mock_get: Mock, base_headers, account):
    # Two pages: first full page (limit=2), second partial page (1 item) -> stop
    first_page_resp = Mock()
    first_page_resp.json.return_value = {
        "data": [
            {"id": 101, "name": "Proj A", "account_id": account.id},
            {"id": 102, "name": "Proj B", "account_id": account.id},
        ]
    }
    first_page_resp.raise_for_status.return_value = None

    second_page_resp = Mock()
    second_page_resp.json.return_value = {
        "data": [
            {"id": 103, "name": "Proj C", "account_id": account.id},
        ]
    }
    second_page_resp.raise_for_status.return_value = None

    mock_get.side_effect = [first_page_resp, second_page_resp]

    result = _get_all_projects_for_account(
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
    actual_urls = [
        call.kwargs["url"] if "url" in call.kwargs else call.args[0]
        for call in mock_get.call_args_list
    ]
    assert actual_urls == expected_urls


@patch("dbt_mcp.oauth.fastapi_app.requests.get")
def test_get_all_environments_for_project_paginates(mock_get: Mock, base_headers):
    # Two pages: first full page (limit=2), second partial (1 item)
    first_page_resp = Mock()
    first_page_resp.json.return_value = {
        "data": [
            {"id": 201, "name": "Dev", "deployment_type": "development"},
            {"id": 202, "name": "Prod", "deployment_type": "production"},
        ]
    }
    first_page_resp.raise_for_status.return_value = None

    second_page_resp = Mock()
    second_page_resp.json.return_value = {
        "data": [
            {"id": 203, "name": "Staging", "deployment_type": "development"},
        ]
    }
    second_page_resp.raise_for_status.return_value = None

    mock_get.side_effect = [first_page_resp, second_page_resp]

    result = _get_all_environments_for_project(
        dbt_platform_url="https://cloud.getdbt.com",
        account_id=1,
        project_id=9,
        headers=base_headers,
        page_size=2,
    )

    assert len(result) == 3
    assert {e.id for e in result} == {201, 202, 203}

    expected_urls = [
        "https://cloud.getdbt.com/api/v3/accounts/1/projects/9/environments/?state=1&offset=0&limit=2",
        "https://cloud.getdbt.com/api/v3/accounts/1/projects/9/environments/?state=1&offset=2&limit=2",
    ]
    actual_urls = [
        call.kwargs["url"] if "url" in call.kwargs else call.args[0]
        for call in mock_get.call_args_list
    ]
    assert actual_urls == expected_urls
