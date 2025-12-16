from unittest.mock import AsyncMock, Mock, patch

import pytest

from dbt_mcp.dbt_admin.tools import (
    ADMIN_TOOLS,
    AdminToolContext,
    JobRunStatus,
    cancel_job_run,
    get_job_details,
    get_job_run_artifact,
    get_job_run_details,
    get_job_run_error,
    list_job_run_artifacts,
    list_jobs,
    list_jobs_runs,
    register_admin_api_tools,
    retry_job_run,
    trigger_job_run,
)
from tests.mocks.config import mock_config


@pytest.fixture
def mock_admin_client():
    client = Mock()

    # Create AsyncMock methods with proper return values
    client.list_jobs = AsyncMock(
        return_value=[
            {
                "id": 1,
                "name": "test_job",
                "description": "Test job description",
                "dbt_version": "1.7.0",
                "job_type": "deploy",
                "triggers": {},
                "most_recent_run_id": 100,
                "most_recent_run_status": "success",
                "schedule": "0 9 * * *",
            }
        ]
    )

    client.get_job_details = AsyncMock(return_value={"id": 1, "name": "test_job"})
    client.trigger_job_run = AsyncMock(return_value={"id": 200, "status": "queued"})
    client.list_jobs_runs = AsyncMock(
        return_value=[
            {
                "id": 100,
                "status": 10,
                "status_humanized": "Success",
                "job_definition_id": 1,
                "started_at": "2024-01-01T00:00:00Z",
                "finished_at": "2024-01-01T00:05:00Z",
            }
        ]
    )
    client.get_job_run_details = AsyncMock(
        return_value={
            "id": 100,
            "status": 10,
            "status_humanized": "Success",
            "is_cancelled": False,
            "run_steps": [
                {
                    "index": 1,
                    "name": "Invoke dbt with `dbt build`",
                    "status": 20,
                    "status_humanized": "Error",
                    "logs_url": "https://example.com/logs",
                }
            ],
        }
    )
    client.cancel_job_run = AsyncMock(
        return_value={
            "id": 100,
            "status": 20,
            "status_humanized": "Cancelled",
        }
    )
    client.retry_job_run = AsyncMock(
        return_value={
            "id": 101,
            "status": 1,
            "status_humanized": "Queued",
        }
    )
    client.list_job_run_artifacts = AsyncMock(
        return_value=["manifest.json", "catalog.json"]
    )
    client.get_job_run_artifact = AsyncMock(return_value={"nodes": {}})

    return client


@pytest.fixture
def admin_context(mock_admin_client):
    """Create AdminToolContext with mocked client."""
    context = AdminToolContext(mock_config.admin_api_config_provider)
    # Replace the client with our mock
    context.admin_client = mock_admin_client
    return context


@patch("dbt_mcp.dbt_admin.tools.register_tools")
async def test_register_admin_api_tools_all_tools(mock_register_tools, mock_fastmcp):
    fastmcp, tools = mock_fastmcp

    register_admin_api_tools(
        fastmcp,
        mock_config.admin_api_config_provider,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    # Should call register_tools with 10 tool definitions
    mock_register_tools.assert_called_once()
    args, kwargs = mock_register_tools.call_args
    tool_definitions = kwargs["tool_definitions"]
    assert len(tool_definitions) == 10


@patch("dbt_mcp.dbt_admin.tools.register_tools")
async def test_register_admin_api_tools_with_disabled_tools(
    mock_register_tools, mock_fastmcp
):
    fastmcp, tools = mock_fastmcp

    disable_tools = ["list_jobs", "get_job", "trigger_job_run"]
    register_admin_api_tools(
        fastmcp,
        mock_config.admin_api_config_provider,
        disabled_tools=set(disable_tools),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    # Should still call register_tools with all 10 tool definitions
    # The exclude_tools parameter is passed to register_tools to handle filtering
    mock_register_tools.assert_called_once()
    args, kwargs = mock_register_tools.call_args
    tool_definitions = kwargs["tool_definitions"]
    disabled_tools = kwargs["disabled_tools"]
    assert len(tool_definitions) == 10
    assert disabled_tools == set(disable_tools)


async def test_list_jobs_tool(admin_context):
    result = await list_jobs.fn(admin_context, limit=10)

    assert isinstance(result, list)
    admin_context.admin_client.list_jobs.assert_called_once()


async def test_get_job_details_tool(admin_context):
    result = await get_job_details.fn(admin_context, job_id=1)

    assert isinstance(result, dict)
    admin_context.admin_client.get_job_details.assert_called_once_with(12345, 1)


async def test_trigger_job_run_tool(admin_context):
    result = await trigger_job_run.fn(
        admin_context, job_id=1, cause="Manual trigger", git_branch="main"
    )

    assert isinstance(result, dict)
    admin_context.admin_client.trigger_job_run.assert_called_once_with(
        12345, 1, "Manual trigger", git_branch="main"
    )


async def test_list_jobs_runs_tool(admin_context):
    result = await list_jobs_runs.fn(
        admin_context, job_id=1, status=JobRunStatus.SUCCESS, limit=5
    )

    assert isinstance(result, list)
    admin_context.admin_client.list_jobs_runs.assert_called_once_with(
        12345, job_definition_id=1, status=10, limit=5
    )


async def test_get_job_run_details_tool(admin_context):
    result = await get_job_run_details.fn(admin_context, run_id=100)

    assert isinstance(result, dict)
    admin_context.admin_client.get_job_run_details.assert_called_once_with(12345, 100)


async def test_cancel_job_run_tool(admin_context):
    result = await cancel_job_run.fn(admin_context, run_id=100)

    assert isinstance(result, dict)
    admin_context.admin_client.cancel_job_run.assert_called_once_with(12345, 100)


async def test_retry_job_run_tool(admin_context):
    result = await retry_job_run.fn(admin_context, run_id=100)

    assert isinstance(result, dict)
    admin_context.admin_client.retry_job_run.assert_called_once_with(12345, 100)


async def test_list_job_run_artifacts_tool(admin_context):
    result = await list_job_run_artifacts.fn(admin_context, run_id=100)

    assert isinstance(result, list)
    admin_context.admin_client.list_job_run_artifacts.assert_called_once_with(
        12345, 100
    )


async def test_get_job_run_artifact_tool(admin_context):
    result = await get_job_run_artifact.fn(
        admin_context, run_id=100, artifact_path="manifest.json", step=1
    )

    assert result is not None
    admin_context.admin_client.get_job_run_artifact.assert_called_once_with(
        12345, 100, "manifest.json", 1
    )


async def test_tools_handle_exceptions():
    # Create a context with a failing client
    mock_admin_client = Mock()
    mock_admin_client.list_jobs.side_effect = Exception("API Error")

    context = AdminToolContext(mock_config.admin_api_config_provider)
    context.admin_client = mock_admin_client

    with pytest.raises(Exception) as exc_info:
        await list_jobs.fn(context)
    assert "API Error" in str(exc_info.value)


async def test_tools_with_no_optional_parameters(admin_context):
    # Test list_jobs with no parameters
    result = await list_jobs.fn(admin_context)
    assert isinstance(result, list)
    admin_context.admin_client.list_jobs.assert_called_with(12345)

    # Test list_jobs_runs with no parameters
    result = await list_jobs_runs.fn(admin_context)
    assert isinstance(result, list)
    admin_context.admin_client.list_jobs_runs.assert_called_with(12345)

    # Test get_job_run_details
    result = await get_job_run_details.fn(admin_context, run_id=100)
    assert isinstance(result, dict)
    admin_context.admin_client.get_job_run_details.assert_called_with(12345, 100)


async def test_trigger_job_run_with_all_optional_params(admin_context):
    result = await trigger_job_run.fn(
        admin_context,
        job_id=1,
        cause="Manual trigger",
        git_branch="feature-branch",
        git_sha="abc123",
        schema_override="custom_schema",
    )

    assert isinstance(result, dict)
    admin_context.admin_client.trigger_job_run.assert_called_once_with(
        12345,
        1,
        "Manual trigger",
        git_branch="feature-branch",
        git_sha="abc123",
        schema_override="custom_schema",
    )


@patch("dbt_mcp.dbt_admin.tools.ErrorFetcher")
async def test_get_job_run_error_tool(mock_error_fetcher_class, admin_context):
    # Mock the ErrorFetcher instance and its analyze_run_errors method
    mock_error_fetcher_instance = Mock()
    mock_error_fetcher_instance.analyze_run_errors = AsyncMock(
        return_value={
            "failed_steps": [
                {
                    "step_name": "Invoke dbt with `dbt build`",
                    "target": "prod",
                    "finished_at": "2024-01-01T10:00:00Z",
                    "results": [
                        {
                            "unique_id": "model.analytics.user_sessions",
                            "message": "Database Error in model user_sessions...",
                            "relation_name": "prod.analytics.user_sessions",
                            "compiled_code": "SELECT * FROM raw.sessions",
                            "truncated_logs": None,
                        }
                    ],
                }
            ]
        }
    )
    mock_error_fetcher_class.return_value = mock_error_fetcher_instance

    result = await get_job_run_error.fn(admin_context, run_id=100)

    assert isinstance(result, dict)
    assert "failed_steps" in result
    assert len(result["failed_steps"]) == 1

    step = result["failed_steps"][0]
    assert step["step_name"] == "Invoke dbt with `dbt build`"
    assert step["target"] == "prod"
    assert len(step["results"]) == 1
    assert step["results"][0]["message"] == "Database Error in model user_sessions..."

    mock_error_fetcher_class.assert_called_once()
    mock_error_fetcher_instance.analyze_run_errors.assert_called_once()


def test_admin_tools_list_contains_all_tools():
    """Test that ADMIN_TOOLS contains all expected tools."""
    expected_tool_names = {
        "list_jobs",
        "get_job_details",
        "trigger_job_run",
        "list_jobs_runs",
        "get_job_run_details",
        "cancel_job_run",
        "retry_job_run",
        "list_job_run_artifacts",
        "get_job_run_artifact",
        "get_job_run_error",
    }

    actual_tool_names = {tool.fn.__name__ for tool in ADMIN_TOOLS}
    assert actual_tool_names == expected_tool_names
    assert len(ADMIN_TOOLS) == 10
