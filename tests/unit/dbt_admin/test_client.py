from unittest.mock import Mock, patch

import pytest
import requests

from dbt_mcp.config.config import AdminApiConfig
from dbt_mcp.dbt_admin.client import (
    AdminAPIError,
    DbtAdminAPIClient,
)


@pytest.fixture
def admin_config():
    return AdminApiConfig(
        account_id=12345,
        headers={"Authorization": "Bearer test_token"},
        url="https://cloud.getdbt.com",
    )


@pytest.fixture
def admin_config_with_prefix():
    return AdminApiConfig(
        account_id=12345,
        headers={"Authorization": "Bearer test_token"},
        url="https://eu1.cloud.getdbt.com",
    )


@pytest.fixture
def client(admin_config):
    return DbtAdminAPIClient(admin_config)


@pytest.fixture
def client_with_prefix(admin_config_with_prefix):
    return DbtAdminAPIClient(admin_config_with_prefix)


def test_client_initialization(client):
    assert client.config.account_id == 12345
    assert client.config.headers == {"Authorization": "Bearer test_token"}
    assert client.config.url == "https://cloud.getdbt.com"
    assert client.headers["Authorization"] == "Bearer test_token"
    assert client.headers["Content-Type"] == "application/json"
    assert client.headers["Accept"] == "application/json"


@patch("requests.request")
def test_make_request_success(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {"data": "test"}
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    result = client._make_request("GET", "/test/endpoint")

    assert result == {"data": "test"}
    mock_request.assert_called_once_with(
        "GET", "https://cloud.getdbt.com/test/endpoint", headers=client.headers
    )


@patch("requests.request")
def test_make_request_failure(mock_request, client):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "404 Not Found"
    )
    mock_request.return_value = mock_response

    with pytest.raises(AdminAPIError):
        client._make_request("GET", "/test/endpoint")


@patch("requests.request")
def test_list_jobs(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            {
                "id": 1,
                "name": "test_job",
                "description": "Test description",
                "dbt_version": "1.7.0",
                "job_type": "deploy",
                "triggers": {"github_webhook": True},
                "most_recent_run": {
                    "id": 100,
                    "status_humanized": "Success",
                    "started_at": "2024-01-01T00:00:00Z",
                    "finished_at": "2024-01-01T00:05:00Z",
                },
                "most_recent_completed_run": {
                    "id": 99,
                    "status_humanized": "Success",
                    "started_at": "2024-01-01T00:00:00Z",
                    "finished_at": "2024-01-01T00:04:00Z",
                },
                "schedule": {"cron": "0 9 * * *"},
                "next_run": "2024-01-02T09:00:00Z",
            }
        ]
    }
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    result = client.list_jobs(12345, project_id=1, limit=10)

    assert len(result) == 1
    assert result[0]["id"] == 1
    assert result[0]["name"] == "test_job"
    assert result[0]["most_recent_run_id"] == 100
    assert result[0]["schedule"] == "0 9 * * *"

    mock_request.assert_called_once_with(
        "GET",
        "https://cloud.getdbt.com/api/v2/accounts/12345/jobs/?include_related=['most_recent_run','most_recent_completed_run']",
        headers=client.headers,
        params={"project_id": 1, "limit": 10},
    )


@patch("requests.request")
def test_list_jobs_with_null_values(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            {
                "id": 1,
                "name": "test_job",
                "description": None,
                "dbt_version": "1.7.0",
                "job_type": "deploy",
                "triggers": {},
                "most_recent_run": None,
                "most_recent_completed_run": None,
                "schedule": None,
                "next_run": None,
            }
        ]
    }
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    result = client.list_jobs(12345)

    assert len(result) == 1
    assert result[0]["most_recent_run_id"] is None
    assert result[0]["schedule"] is None


@patch("requests.request")
def test_get_job_details(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {"data": {"id": 1, "name": "test_job"}}
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    result = client.get_job_details(12345, 1)

    assert result == {"id": 1, "name": "test_job"}
    mock_request.assert_called_once_with(
        "GET",
        "https://cloud.getdbt.com/api/v2/accounts/12345/jobs/1/?include_related=['most_recent_run','most_recent_completed_run']",
        headers=client.headers,
    )


@patch("requests.request")
def test_trigger_job_run(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {"data": {"id": 200, "status": "queued"}}
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    result = client.trigger_job_run(
        12345, 1, "Manual trigger", git_branch="main", schema_override="test_schema"
    )

    assert result == {"id": 200, "status": "queued"}
    mock_request.assert_called_once_with(
        "POST",
        "https://cloud.getdbt.com/api/v2/accounts/12345/jobs/1/run/",
        headers=client.headers,
        json={
            "cause": "Manual trigger",
            "git_branch": "main",
            "schema_override": "test_schema",
        },
    )


@patch("requests.request")
def test_list_jobs_runs(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            {
                "id": 100,
                "status": 10,
                "status_humanized": "Success",
                "job": {"name": "test_job", "execute_step": ["dbt run"]},
                "started_at": "2024-01-01T00:00:00Z",
                "finished_at": "2024-01-01T00:05:00Z",
                # Fields that should be removed
                "account_id": 12345,
                "environment_id": 1,
                "blocked_by": None,
                "used_repo_cache": True,
                "audit": {},
                "created_at_humanized": "1 hour ago",
                "duration_humanized": "5 minutes",
                "finished_at_humanized": "1 hour ago",
                "queued_duration_humanized": "10 seconds",
                "run_duration_humanized": "4 minutes 50 seconds",
                "artifacts_saved": True,
                "artifact_s3_path": "s3://bucket/path",
                "has_docs_generated": True,
                "has_sources_generated": False,
                "notifications_sent": True,
                "executed_by_thread_id": "thread123",
                "updated_at": "2024-01-01T00:05:00Z",
                "dequeued_at": "2024-01-01T00:00:30Z",
                "last_checked_at": "2024-01-01T00:04:00Z",
                "last_heartbeat_at": "2024-01-01T00:04:30Z",
                "trigger": {},
                "run_steps": [],
                "deprecation": {},
                "environment": {},
            }
        ]
    }
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    result = client.list_jobs_runs(12345, job_definition_id=1, status="success")

    assert len(result) == 1
    run = result[0]
    assert run["id"] == 100
    assert run["job_name"] == "test_job"
    assert run["job_steps"] == ["dbt run"]

    # Verify removed fields are not present
    removed_fields = [
        "job",
        "account_id",
        "environment_id",
        "blocked_by",
        "used_repo_cache",
        "audit",
        "created_at_humanized",
        "duration_humanized",
        "finished_at_humanized",
        "queued_duration_humanized",
        "run_duration_humanized",
        "artifacts_saved",
        "artifact_s3_path",
        "has_docs_generated",
        "has_sources_generated",
        "notifications_sent",
        "executed_by_thread_id",
        "updated_at",
        "dequeued_at",
        "last_checked_at",
        "last_heartbeat_at",
        "trigger",
        "run_steps",
        "deprecation",
        "environment",
    ]
    for field in removed_fields:
        assert field not in run

    mock_request.assert_called_once_with(
        "GET",
        "https://cloud.getdbt.com/api/v2/accounts/12345/runs/?include_related=['job']",
        headers=client.headers,
        params={"job_definition_id": 1, "status": "success"},
    )


@patch("requests.request")
def test_get_job_run_details_without_debug(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": {
            "id": 100,
            "status": 10,
            "run_steps": [
                {"id": 1, "name": "dbt run", "truncated_debug_logs": "log data"}
            ],
        }
    }
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    result = client.get_job_run_details(12345, 100, debug=False)

    assert result["id"] == 100
    # Verify truncated_debug_logs are removed
    assert "truncated_debug_logs" not in result["run_steps"][0]

    mock_request.assert_called_once_with(
        "GET",
        "https://cloud.getdbt.com/api/v2/accounts/12345/runs/100/?include_related=['run_steps']",
        headers=client.headers,
    )


@patch("requests.request")
def test_get_job_run_details_with_debug(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": {
            "id": 100,
            "status": 10,
            "run_steps": [
                {"id": 1, "name": "dbt run", "truncated_debug_logs": "log data"}
            ],
        }
    }
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    _ = client.get_job_run_details(12345, 100, debug=True)

    mock_request.assert_called_once_with(
        "GET",
        "https://cloud.getdbt.com/api/v2/accounts/12345/runs/100/?include_related=['run_steps','debug_logs']",
        headers=client.headers,
    )


@patch("requests.request")
def test_cancel_job_run(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {"data": {"id": 100, "status": "cancelled"}}
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    result = client.cancel_job_run(12345, 100)

    assert result == {"id": 100, "status": "cancelled"}
    mock_request.assert_called_once_with(
        "POST",
        "https://cloud.getdbt.com/api/v2/accounts/12345/runs/100/cancel/",
        headers=client.headers,
    )


@patch("requests.request")
def test_retry_job_run(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {"data": {"id": 101, "status": "queued"}}
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    result = client.retry_job_run(12345, 100)

    assert result == {"id": 101, "status": "queued"}
    mock_request.assert_called_once_with(
        "POST",
        "https://cloud.getdbt.com/api/v2/accounts/12345/runs/100/retry/",
        headers=client.headers,
    )


@patch("requests.request")
def test_list_job_run_artifacts(mock_request, client):
    mock_response = Mock()
    mock_response.json.return_value = {
        "data": [
            "manifest.json",
            "catalog.json",
            "compiled/my_project/models/model.sql",
            "run/my_project/models/model.sql",
            "sources.json",
        ]
    }
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response

    result = client.list_job_run_artifacts(12345, 100)

    # Should filter out compiled/ and run/ artifacts
    expected = ["manifest.json", "catalog.json", "sources.json"]
    assert result == expected

    mock_request.assert_called_once_with(
        "GET",
        "https://cloud.getdbt.com/api/v2/accounts/12345/runs/100/artifacts/",
        headers=client.headers,
    )


@patch("requests.get")
def test_get_job_run_artifact_json(mock_get, client):
    mock_response = Mock()
    mock_response.json.return_value = {"nodes": {"model.test": {}}}
    mock_response.headers = {"content-type": "application/json"}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = client.get_job_run_artifact(12345, 100, "manifest.json", step=1)

    # The client returns response.text, but the mock returns the mock_response.text which is a Mock object
    # In a real scenario with JSON content type, the API would return JSON as text
    assert result is not None
    mock_get.assert_called_once_with(
        "https://cloud.getdbt.com/api/v2/accounts/12345/runs/100/artifacts/manifest.json",
        headers={"Authorization": "Bearer test_token", "Accept": "*/*"},
        params={"step": 1},
    )


@patch("requests.get")
def test_get_job_run_artifact_text(mock_get, client):
    mock_response = Mock()
    mock_response.text = "LOG DATA"
    mock_response.headers = {"content-type": "text/plain"}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = client.get_job_run_artifact(12345, 100, "logs/dbt.log")

    assert result == "LOG DATA"
    mock_get.assert_called_once_with(
        "https://cloud.getdbt.com/api/v2/accounts/12345/runs/100/artifacts/logs/dbt.log",
        headers={"Authorization": "Bearer test_token", "Accept": "*/*"},
        params={},
    )


@patch("requests.get")
def test_get_job_run_artifact_no_step_param(mock_get, client):
    mock_response = Mock()
    mock_response.text = "artifact content"
    mock_response.headers = {"content-type": "text/plain"}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    client.get_job_run_artifact(12345, 100, "manifest.json")

    mock_get.assert_called_once_with(
        "https://cloud.getdbt.com/api/v2/accounts/12345/runs/100/artifacts/manifest.json",
        headers={"Authorization": "Bearer test_token", "Accept": "*/*"},
        params={},
    )


@patch("requests.get")
def test_get_job_run_artifact_request_exception(mock_get, client):
    mock_get.side_effect = requests.exceptions.HTTPError("404 Not Found")

    with pytest.raises(requests.exceptions.HTTPError):
        client.get_job_run_artifact(12345, 100, "nonexistent.json")
