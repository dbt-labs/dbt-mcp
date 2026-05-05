import json
from unittest.mock import AsyncMock

import pytest

from dbt_mcp.dbt_admin.run_artifacts.parser import ErrorFetcher
from dbt_mcp.errors import ArtifactRetrievalError, NotFoundError


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
                    "metadata": {
                        "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json",
                        "dbt_version": "1.7.0",
                        "generated_at": "2024-01-01T10:00:00Z",
                        "invocation_id": "test-invocation-id",
                    },
                    "results": [
                        {
                            "unique_id": "model.test_model",
                            "status": "error",
                            "message": "Model compilation failed",
                            "relation_name": "analytics.test_model",
                            "timing": [],
                            "thread_id": "Thread-1",
                            "execution_time": 1.0,
                            "adapter_response": {},
                        }
                    ],
                    "args": {"target": "prod"},
                    "elapsed_time": 1.0,
                },
            ],
            2,
            [
                "run_results.json not available - returning logs",
                "Model compilation failed",
            ],
        ),
        # Source freshness "fail" status — not in strict sources schema, goes via LenientSources
        # dbt-artifacts-parser rejects "fail" (not a valid Status1 enum value), so the lenient
        # fallback is used. to_freshness_error must still catch it via RunResultsStatus.FAIL.
        (
            {
                "id": 501,
                "status": 20,
                "is_cancelled": False,
                "finished_at": "2024-01-01T12:00:00Z",
                "run_steps": [
                    {
                        "index": 1,
                        "name": "Source freshness",
                        "status": 20,
                        "finished_at": "2024-01-01T12:00:00Z",
                    },
                ],
            },
            [
                {
                    "metadata": {
                        "dbt_schema_version": "https://schemas.getdbt.com/dbt/sources/v2.json"
                    },
                    "elapsed_time": 1.0,
                    "results": [
                        {
                            "unique_id": "source.project.raw_data.orders",
                            "status": "fail",
                            "max_loaded_at": "2024-01-01T00:00:00Z",
                            "snapshotted_at": "2024-01-01T12:00:00Z",
                            "max_loaded_at_time_ago_in_s": 86400.0,
                            "criteria": {
                                "warn_after": None,
                                "error_after": None,
                                "filter": None,
                            },
                            "adapter_response": {},
                            "timing": [],
                            "thread_id": "Thread-1",
                            "execution_time": 1.0,
                        }
                    ],
                },
            ],
            1,
            ["Source freshness error: 86400s since last load"],
        ),
        # Source freshness fails WITH sources.json available - should parse structured errors
        (
            {
                "id": 500,
                "status": 20,
                "is_cancelled": False,
                "finished_at": "2024-01-01T11:00:00Z",
                "run_steps": [
                    {
                        "index": 1,
                        "name": "Source freshness",
                        "status": 20,
                        "finished_at": "2024-01-01T11:00:00Z",
                    },
                ],
            },
            [
                {
                    "metadata": {
                        "dbt_schema_version": "https://schemas.getdbt.com/dbt/sources/v2.json"
                    },
                    "elapsed_time": 1.0,
                    "results": [
                        {
                            "unique_id": "source.project.raw_data.users",
                            "status": "error",
                            "max_loaded_at": "2024-01-01T00:00:00Z",
                            "snapshotted_at": "2024-01-01T11:00:00Z",
                            "max_loaded_at_time_ago_in_s": 172800.0,
                            "criteria": {
                                "warn_after": None,
                                "error_after": None,
                                "filter": None,
                            },
                            "adapter_response": {},
                            "timing": [],
                            "thread_id": "Thread-1",
                            "execution_time": 1.0,
                        }
                    ],
                },
            ],
            1,
            ["Source freshness error: 172800s since last load"],
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
    # Map step_index to artifact content
    step_index_to_artifacts = {}
    for i, failed_step in enumerate(run_details.get("run_steps", [])):
        if i < len(artifact_responses):
            step_index = failed_step["index"]
            step_index_to_artifacts[step_index] = artifact_responses[i]

    async def mock_get_artifact(account_id, run_id, artifact_path, step=None):  # noqa: ARG001
        artifact_content = step_index_to_artifacts.get(step)
        if artifact_content is None:
            raise ArtifactRetrievalError("Artifact not available")

        # Determine if this artifact is sources.json or run_results.json based on structure
        is_sources_json = False
        is_run_results_json = False

        if "results" in artifact_content and artifact_content.get("results"):
            first_result = artifact_content["results"][0]
            if "max_loaded_at_time_ago_in_s" in first_result:
                is_sources_json = True
            elif "unique_id" in first_result and "status" in first_result:
                is_run_results_json = True

        # Return artifact only if it matches the requested type
        if artifact_path == "sources.json" and is_sources_json:
            return json.dumps(artifact_content)
        elif artifact_path == "run_results.json" and is_run_results_json:
            return json.dumps(artifact_content)

        raise ArtifactRetrievalError(f"{artifact_path} not available")

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
        assert expected_msg in result["failed_steps"][i]["results"][0]["message"]


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

    # Return valid JSON that is missing the "results" key.
    # rr_artifact.parse() fails strict validation and falls back to LenientRunResults,
    # which succeeds with results=[] (the default). Empty errors → "No failures found"
    # message with truncated logs still attached.
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

    # Lenient fallback parses successfully with no results → "No failures found" + logs
    assert len(result["failed_steps"]) == 1
    step = result["failed_steps"][0]
    assert step["step_name"] == "Invoke dbt with `dbt build`"
    assert "No failures found in run_results.json" in step["results"][0]["message"]
    assert "Model compilation failed" in step["results"][0]["truncated_logs"]


async def test_not_found_error_treated_as_missing_artifact(mock_client, admin_config):
    """Regression test: NotFoundError (404) from get_job_run_artifact must be caught.

    In v1.15, get_job_run_artifact started raising NotFoundError for 404 responses
    instead of ArtifactRetrievalError. If _fetch_run_results_artifact only catches
    ArtifactRetrievalError, the NotFoundError propagates into asyncio.gather and
    causes every failed step to be silently dropped, returning {"failed_steps": []}.
    """
    run_details = {
        "id": 500,
        "status": 20,
        "is_cancelled": False,
        "finished_at": "2024-01-01T12:00:00Z",
        "run_steps": [
            {
                "index": 1,
                "name": "Invoke dbt with `dbt run`",
                "status": 20,
                "finished_at": "2024-01-01T12:00:00Z",
                "logs": "Error in model my_model",
            }
        ],
    }

    mock_client.get_job_run_artifact = AsyncMock(
        side_effect=NotFoundError("Artifact not found for run 500")
    )

    error_fetcher = ErrorFetcher(
        run_id=500,
        run_details=run_details,
        client=mock_client,
        admin_api_config=admin_config,
    )

    result = await error_fetcher.analyze_run_errors()

    # Should fall back to logs rather than returning empty failed_steps
    assert len(result["failed_steps"]) == 1
    step = result["failed_steps"][0]
    assert step["step_name"] == "Invoke dbt with `dbt run`"
    assert "run_results.json not available" in step["results"][0]["message"]
    assert "Error in model my_model" in step["results"][0]["truncated_logs"]
