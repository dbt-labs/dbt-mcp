import json
from unittest.mock import AsyncMock

import pytest

from dbt_mcp.dbt_admin.run_artifacts.parser import WarningFetcher
from dbt_mcp.errors import ArtifactRetrievalError


@pytest.mark.parametrize(
    "run_details,artifact_responses,expected_has_warnings,expected_counts",
    [
        # Cancelled run - should return empty response
        (
            {
                "id": 100,
                "status": 30,
                "is_cancelled": True,
                "finished_at": "2024-01-01T09:00:00Z",
                "run_steps": [],
            },
            [],
            False,
            {
                "total_warnings": 0,
                "test_warnings": 0,
                "freshness_warnings": 0,
                "log_warnings": 0,
            },
        ),
        # Successful run with test warnings in run_results.json
        (
            {
                "id": 200,
                "status": 10,
                "is_cancelled": False,
                "finished_at": "2024-01-01T10:00:00Z",
                "run_steps": [
                    {
                        "index": 1,
                        "name": "Invoke dbt with `dbt test`",
                        "status": 10,
                        "finished_at": "2024-01-01T10:00:00Z",
                    }
                ],
            },
            [
                {
                    "results": [
                        {
                            "unique_id": "test.my_project.test_null_check",
                            "status": "warn",
                            "message": "Test passed with warnings",
                            "relation_name": "analytics.users",
                        }
                    ],
                    "args": {"target": "prod"},
                }
            ],
            True,
            {
                "total_warnings": 1,
                "test_warnings": 1,
                "freshness_warnings": 0,
                "log_warnings": 0,
            },
        ),
        # Source freshness warnings
        (
            {
                "id": 300,
                "status": 10,
                "is_cancelled": False,
                "finished_at": "2024-01-01T11:00:00Z",
                "run_steps": [
                    {
                        "index": 1,
                        "name": "Source freshness",
                        "status": 10,
                        "finished_at": "2024-01-01T11:00:00Z",
                    }
                ],
            },
            [
                {
                    "results": [
                        {
                            "unique_id": "source.project.raw_data.orders",
                            "status": "warn",
                            "max_loaded_at_time_ago_in_s": 90000.0,
                        }
                    ],
                    "metadata": {
                        "dbt_schema_version": "https://schemas.getdbt.com/dbt/sources/v3.json"
                    },
                }
            ],
            True,
            {
                "total_warnings": 1,
                "test_warnings": 0,
                "freshness_warnings": 1,
                "log_warnings": 0,
            },
        ),
        # Log warnings extracted from step logs
        (
            {
                "id": 400,
                "status": 10,
                "is_cancelled": False,
                "finished_at": "2024-01-01T12:00:00Z",
                "run_steps": [
                    {
                        "index": 1,
                        "name": "Invoke dbt with `dbt run`",
                        "status": 10,
                        "finished_at": "2024-01-01T12:00:00Z",
                        "logs": "10:00:00 [WARNING] Deprecated function usage detected\n10:00:01 Model completed successfully",
                    }
                ],
            },
            [None],  # No run_results.json available
            True,
            {
                "total_warnings": 1,
                "test_warnings": 0,
                "freshness_warnings": 0,
                "log_warnings": 1,
            },
        ),
    ],
)
async def test_warning_scenarios(
    mock_client,
    admin_config,
    run_details,
    artifact_responses,
    expected_has_warnings,
    expected_counts,
):
    """Test various warning scenarios with parametrized data."""
    # Map step_index to artifact content
    step_index_to_artifacts = {}
    for i, step in enumerate(run_details.get("run_steps", [])):
        if i < len(artifact_responses):
            step_index = step["index"]
            step_index_to_artifacts[step_index] = artifact_responses[i]

    async def mock_get_artifact(account_id, run_id, artifact_path, step=None):  # noqa: ARG001
        artifact_content = step_index_to_artifacts.get(step)
        if artifact_content is None:
            raise ArtifactRetrievalError("Artifact not available")

        # Determine artifact type based on structure
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

    warning_fetcher = WarningFetcher(
        run_id=run_details["id"],
        run_details=run_details,
        client=mock_client,
        admin_api_config=admin_config,
    )

    result = await warning_fetcher.analyze_run_warnings()

    assert result["has_warnings"] == expected_has_warnings
    assert result["summary"] == expected_counts
