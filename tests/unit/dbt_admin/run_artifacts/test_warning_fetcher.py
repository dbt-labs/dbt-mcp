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
                    "metadata": {
                        "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json",
                        "dbt_version": "1.7.0",
                        "generated_at": "2024-01-01T10:00:00Z",
                        "invocation_id": "test-invocation-id",
                    },
                    "results": [
                        {
                            "unique_id": "test.my_project.test_null_check",
                            "status": "warn",
                            "message": "Test passed with warnings",
                            "relation_name": "analytics.users",
                            "timing": [],
                            "thread_id": "Thread-1",
                            "execution_time": 1.0,
                            "adapter_response": {},
                        }
                    ],
                    "args": {"target": "prod"},
                    "elapsed_time": 1.0,
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
                    "metadata": {
                        "dbt_schema_version": "https://schemas.getdbt.com/dbt/sources/v2.json"
                    },
                    "elapsed_time": 1.0,
                    "results": [
                        {
                            "unique_id": "source.project.raw_data.orders",
                            "status": "warn",
                            "max_loaded_at": "2024-01-01T00:00:00Z",
                            "snapshotted_at": "2024-01-01T11:00:00Z",
                            "max_loaded_at_time_ago_in_s": 90000.0,
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
        # Fusion log warnings extracted from WARN-prefixed log entries
        (
            {
                "id": 500,
                "status": 10,
                "is_cancelled": False,
                "finished_at": "2024-01-01T12:00:00Z",
                "run_steps": [
                    {
                        "index": 1,
                        "name": "Invoke dbt with `dbt run`",
                        "status": 10,
                        "finished_at": "2024-01-01T12:00:00Z",
                        "logs": (
                            "04:00:14      INFO \x1b[1m     Running\x1b[0m Fusion version: 2.0.0-preview.173\n"
                            "04:00:14      WARN \x1b[33;1m        Warn\x1b[0m \x1b[1mdbt1088\x1b[0m: Updated version available for zendesk@1.4.1: 1.5.1\n"
                            "04:00:15      WARN \x1b[33;1m        Warn\x1b[0m \x1b[1mdbt1088\x1b[0m: Updated version available for tiktok_ads@1.1.0: 1.2.0\n"
                            "04:00:16      INFO \x1b[1;32m   Installed\x1b[0m 2 packages"
                        ),
                    }
                ],
            },
            [None],  # No run_results.json available
            True,
            {
                "total_warnings": 2,
                "test_warnings": 0,
                "freshness_warnings": 0,
                "log_warnings": 2,
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


@pytest.mark.parametrize(
    "clean_logs,expected",
    [
        # Fusion banner present → detected as Fusion
        ("04:00:14      INFO      Running Fusion version: 2.0.0-preview.173\n", True),
        # Core logs — no Fusion banner
        ("10:00:00 [WARNING] Deprecated function usage detected\n", False),
        # Contains "Fusion" in a model name but not the banner — must not false-positive
        ("10:00:00 INFO model fusion_events completed successfully\n", False),
    ],
)
def test_is_fusion_logs(mock_client, admin_config, clean_logs, expected):
    fetcher = WarningFetcher(
        run_id=1,
        run_details={
            "id": 1,
            "status": 10,
            "is_cancelled": False,
            "finished_at": "2024-01-01T00:00:00Z",
            "run_steps": [],
        },
        client=mock_client,
        admin_api_config=admin_config,
    )
    assert fetcher._is_fusion_logs(clean_logs) == expected


@pytest.mark.parametrize(
    "clean_logs,expected_messages",
    [
        # No WARN lines → empty result
        (
            "04:00:14      INFO      Running Fusion version: 2.0.0-preview.173\n"
            "04:00:16      INFO      Installed 2 packages\n",
            [],
        ),
        # Single WARN line → one result with the full line as the message
        (
            "04:00:14      WARN      Warn dbt1088: Updated version available for zendesk@1.4.1: 1.5.1\n",
            [
                "04:00:14      WARN      Warn dbt1088: Updated version available for zendesk@1.4.1: 1.5.1"
            ],
        ),
    ],
)
def test_extract_fusion_log_warnings(
    mock_client, admin_config, clean_logs, expected_messages
):
    fetcher = WarningFetcher(
        run_id=1,
        run_details={
            "id": 1,
            "status": 10,
            "is_cancelled": False,
            "finished_at": "2024-01-01T00:00:00Z",
            "run_steps": [],
        },
        client=mock_client,
        admin_api_config=admin_config,
    )
    results = fetcher._extract_fusion_log_warnings(clean_logs)
    assert [r.message for r in results] == expected_messages
