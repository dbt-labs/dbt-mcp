import json
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_mcp.config.config_providers import AdminApiConfig
from dbt_mcp.dbt_admin.run_results_errors.parser import ErrorFetcher


class MockHeadersProvider:
    """Mock headers provider for testing."""

    def get_headers(self) -> dict[str, str]:
        return {"Authorization": "Bearer test_token"}


@pytest.fixture
def admin_config():
    """Admin API config for testing."""
    return AdminApiConfig(
        account_id=12345,
        headers_provider=MockHeadersProvider(),
        url="https://cloud.getdbt.com",
    )


@pytest.fixture
def mock_client():
    """Base mock client - behavior configured per test."""
    return Mock()


@pytest.mark.parametrize(
    "run_details,artifact_responses,expected_step_count,expected_error_messages",
    [
        # Cancelled run
        (
            {
                "id": 300,
                "status": 30,
                "is_cancelled": True,
                "finished_at": "2024-01-01T09:00:00Z",
                "run_steps": [],
            },
            [],
            1,
            ["Job run was cancelled"],
        ),
        # Source freshness fails (doesn't stop job) + model error downstream
        (
            {
                "id": 400,
                "status": 20,
                "is_cancelled": False,
                "finished_at": "2024-01-01T10:00:00Z",
                "run_steps": [
                    {
                        "index": 1,
                        "name": "Source freshness",
                        "status": 20,
                        "finished_at": "2024-01-01T09:30:00Z",
                    },
                    {
                        "index": 2,
                        "name": "Invoke dbt with `dbt build`",
                        "status": 20,
                        "finished_at": "2024-01-01T10:00:00Z",
                    },
                ],
            },
            [
                None,  # Source freshness artifact not available
                {
                    "results": [
                        {
                            "unique_id": "model.test_model",
                            "status": "error",
                            "message": "Model compilation failed",
                            "relation_name": "analytics.test_model",
                        }
                    ],
                    "args": {"target": "prod"},
                },
            ],
            2,
            ["Source freshness error - returning logs", "Model compilation failed"],
        ),
    ],
)
async def test_error_scenarios(
    mock_client,
    admin_config,
    run_details,
    artifact_responses,
    expected_step_count,
    expected_error_messages,
):
    """Test various error scenarios with parametrized data."""
    # Map step_index to run_results_content
    step_index_to_run_results = {}
    for i, failed_step in enumerate(run_details.get("run_steps", [])):
        if i < len(artifact_responses):
            step_index = failed_step["index"]
            step_index_to_run_results[step_index] = artifact_responses[i]

    async def mock_get_artifact(account_id, run_id, artifact_path, step=None):
        run_results_content = step_index_to_run_results.get(step)
        if run_results_content is None:
            raise Exception("Artifact not available")
        return json.dumps(run_results_content)

    mock_client.get_job_run_artifact = AsyncMock(side_effect=mock_get_artifact)

    error_fetcher = ErrorFetcher(
        run_id=run_details["id"],
        run_details=run_details,
        client=mock_client,
        admin_api_config=admin_config,
    )

    result = await error_fetcher.analyze_run_errors()

    assert len(result["failed_steps"]) == expected_step_count
    for i, expected_msg in enumerate(expected_error_messages):
        assert expected_msg in result["failed_steps"][i]["errors"][0]["message"]


async def test_schema_validation_failure(mock_client, admin_config):
    """Test handling of run_results.json schema changes - should fallback to logs."""
    run_details = {
        "id": 400,
        "status": 20,
        "is_cancelled": False,
        "finished_at": "2024-01-01T11:00:00Z",
        "run_steps": [
            {
                "index": 1,
                "name": "Invoke dbt with `dbt build`",
                "status": 20,
                "finished_at": "2024-01-01T11:00:00Z",
                "logs": "Model compilation failed due to missing table",
            }
        ],
    }

    # Return valid JSON but with missing required fields (schema mismatch)
    # Expected schema: {"results": [...], "args": {...}, "metadata": {...}}
    mock_client.get_job_run_artifact = AsyncMock(
        return_value='{"metadata": {"some": "value"}, "invalid_field": true}'
    )

    error_fetcher = ErrorFetcher(
        run_id=400,
        run_details=run_details,
        client=mock_client,
        admin_api_config=admin_config,
    )

    result = await error_fetcher.analyze_run_errors()

    # Should fallback to logs when schema validation fails
    assert len(result["failed_steps"]) == 1
    step = result["failed_steps"][0]
    assert step["step_name"] == "Invoke dbt with `dbt build`"
    assert "run_results.json not available" in step["errors"][0]["message"]
    assert "Model compilation failed" in step["errors"][0]["truncated_logs"]
