from unittest.mock import Mock, patch

import pytest

from dbt_mcp.config.config import AdminApiConfig, Config
from dbt_mcp.dbt_admin.tools import (
    JobRunStatus,
    create_admin_api_tool_definitions,
    register_admin_api_tools,
)


@pytest.fixture
def mock_admin_config():
    return AdminApiConfig(
        account_id=12345,
        headers={"Authorization": "Bearer test_token"},
        url="https://cloud.getdbt.com",
    )


@pytest.fixture
def mock_config(mock_admin_config):
    from tests.mocks.config import mock_tracking_config

    return Config(
        tracking_config=mock_tracking_config,
        admin_api_config=mock_admin_config,
        disable_tools=[],
    )


@pytest.fixture
def mock_fastmcp():
    class MockFastMCP:
        def __init__(self):
            self.tools = {}

        def tool(self, **kwargs):
            def decorator(func):
                self.tools[func.__name__] = func
                return func

            return decorator

    fastmcp = MockFastMCP()
    return fastmcp, fastmcp.tools


@pytest.fixture
def mock_admin_client():
    client = Mock()
    client.list_jobs.return_value = [
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
    client.get_job_details.return_value = {"id": 1, "name": "test_job"}
    client.trigger_job_run.return_value = {"id": 200, "status": "queued"}
    client.list_jobs_runs.return_value = [
        {
            "id": 100,
            "status": 10,
            "status_humanized": "Success",
            "job_definition_id": 1,
            "started_at": "2024-01-01T00:00:00Z",
            "finished_at": "2024-01-01T00:05:00Z",
        }
    ]
    client.get_job_run_details.return_value = {
        "id": 100,
        "status": 10,
        "status_humanized": "Success",
    }
    client.cancel_job_run.return_value = {
        "id": 100,
        "status": 20,
        "status_humanized": "Cancelled",
    }
    client.retry_job_run.return_value = {
        "id": 101,
        "status": 1,
        "status_humanized": "Queued",
    }
    client.list_job_run_artifacts.return_value = ["manifest.json", "catalog.json"]
    client.get_job_run_artifact.return_value = {"nodes": {}}
    return client


@patch("dbt_mcp.dbt_admin.tools.register_tools")
@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_register_admin_api_tools_all_tools(
    mock_get_prompt, mock_register_tools, mock_config, mock_fastmcp
):
    mock_get_prompt.return_value = "Test prompt"
    fastmcp, tools = mock_fastmcp

    register_admin_api_tools(fastmcp, mock_config.admin_api_config, [])

    # Should call register_tools with 9 tool definitions
    mock_register_tools.assert_called_once()
    args, kwargs = mock_register_tools.call_args
    tool_definitions = args[1]  # Second argument is the tool definitions list
    assert len(tool_definitions) == 9


@patch("dbt_mcp.dbt_admin.tools.register_tools")
@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_register_admin_api_tools_with_disabled_tools(
    mock_get_prompt, mock_register_tools, mock_config, mock_fastmcp
):
    mock_get_prompt.return_value = "Test prompt"
    fastmcp, tools = mock_fastmcp

    disable_tools = ["list_jobs", "get_job", "trigger_job_run"]
    register_admin_api_tools(fastmcp, mock_config.admin_api_config, disable_tools)

    # Should still call register_tools with all 9 tool definitions
    # The exclude_tools parameter is passed to register_tools to handle filtering
    mock_register_tools.assert_called_once()
    args, kwargs = mock_register_tools.call_args
    tool_definitions = args[1]  # Second argument is the tool definitions list
    exclude_tools_arg = args[2]  # Third argument is exclude_tools
    assert len(tool_definitions) == 9
    assert exclude_tools_arg == disable_tools


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_list_jobs_tool(mock_get_prompt, mock_config, mock_admin_client):
    mock_get_prompt.return_value = "List jobs prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    list_jobs_tool = tool_definitions[0].fn  # First tool is list_jobs

    result = list_jobs_tool(limit=10)

    assert isinstance(result, list)
    mock_admin_client.list_jobs.assert_called_once_with(12345, limit=10)


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_get_job_details_tool(mock_get_prompt, mock_config, mock_admin_client):
    mock_get_prompt.return_value = "Get job prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    get_job_details_tool = tool_definitions[1].fn  # Second tool is get_job_details

    result = get_job_details_tool(job_id=1)

    assert isinstance(result, dict)
    mock_admin_client.get_job_details.assert_called_once_with(12345, 1)


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_trigger_job_run_tool(mock_get_prompt, mock_config, mock_admin_client):
    mock_get_prompt.return_value = "Trigger job run prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    trigger_job_run_tool = tool_definitions[2].fn  # Third tool is trigger_job_run

    result = trigger_job_run_tool(job_id=1, cause="Manual trigger", git_branch="main")

    assert isinstance(result, dict)
    mock_admin_client.trigger_job_run.assert_called_once_with(
        12345, 1, "Manual trigger", git_branch="main"
    )


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_list_jobs_runs_tool(mock_get_prompt, mock_config, mock_admin_client):
    mock_get_prompt.return_value = "List runs prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    list_jobs_runs_tool = tool_definitions[3].fn  # Fourth tool is list_jobs_runs

    result = list_jobs_runs_tool(job_id=1, status=JobRunStatus.SUCCESS, limit=5)

    assert isinstance(result, list)
    mock_admin_client.list_jobs_runs.assert_called_once_with(
        12345, job_definition_id=1, status=10, limit=5
    )


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_get_job_run_details_tool(mock_get_prompt, mock_config, mock_admin_client):
    mock_get_prompt.return_value = "Get run prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    get_job_run_details_tool = tool_definitions[
        4
    ].fn  # Fifth tool is get_job_run_details

    result = get_job_run_details_tool(run_id=100, debug=True)

    assert isinstance(result, dict)
    mock_admin_client.get_job_run_details.assert_called_once_with(
        12345, 100, debug=True
    )


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_cancel_job_run_tool(mock_get_prompt, mock_config, mock_admin_client):
    mock_get_prompt.return_value = "Cancel run prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    cancel_job_run_tool = tool_definitions[5].fn  # Sixth tool is cancel_job_run

    result = cancel_job_run_tool(run_id=100)

    assert isinstance(result, dict)
    mock_admin_client.cancel_job_run.assert_called_once_with(12345, 100)


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_retry_job_run_tool(mock_get_prompt, mock_config, mock_admin_client):
    mock_get_prompt.return_value = "Retry run prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    retry_job_run_tool = tool_definitions[6].fn  # Seventh tool is retry_job_run

    result = retry_job_run_tool(run_id=100)

    assert isinstance(result, dict)
    mock_admin_client.retry_job_run.assert_called_once_with(12345, 100)


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_list_job_run_artifacts_tool(mock_get_prompt, mock_config, mock_admin_client):
    mock_get_prompt.return_value = "List run artifacts prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    list_job_run_artifacts_tool = tool_definitions[
        7
    ].fn  # Eighth tool is list_job_run_artifacts

    result = list_job_run_artifacts_tool(run_id=100)

    assert isinstance(result, list)
    mock_admin_client.list_job_run_artifacts.assert_called_once_with(12345, 100)


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_get_job_run_artifact_tool(mock_get_prompt, mock_config, mock_admin_client):
    mock_get_prompt.return_value = "Get run artifact prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    get_job_run_artifact_tool = tool_definitions[
        8
    ].fn  # Ninth tool is get_job_run_artifact

    result = get_job_run_artifact_tool(
        run_id=100, artifact_path="manifest.json", step=1
    )

    assert result is not None
    mock_admin_client.get_job_run_artifact.assert_called_once_with(
        12345, 100, "manifest.json", 1
    )


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_tools_handle_exceptions(mock_get_prompt, mock_config):
    mock_get_prompt.return_value = "Test prompt"
    mock_admin_client = Mock()
    mock_admin_client.list_jobs.side_effect = Exception("API Error")

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    list_jobs_tool = tool_definitions[0].fn  # First tool is list_jobs

    result = list_jobs_tool()

    assert isinstance(result, str)
    assert "API Error" in result


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_tools_with_no_optional_parameters(
    mock_get_prompt, mock_config, mock_admin_client
):
    mock_get_prompt.return_value = "Test prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )

    # Test list_jobs with no parameters
    list_jobs_tool = tool_definitions[0].fn
    result = list_jobs_tool()
    assert isinstance(result, list)
    mock_admin_client.list_jobs.assert_called_with(12345)

    # Test list_jobs_runs with no parameters
    list_jobs_runs_tool = tool_definitions[3].fn
    result = list_jobs_runs_tool()
    assert isinstance(result, list)
    mock_admin_client.list_jobs_runs.assert_called_with(12345)

    # Test get_job_run_details with default debug parameter
    get_job_run_details_tool = tool_definitions[4].fn
    result = get_job_run_details_tool(run_id=100)
    assert isinstance(result, dict)
    # The debug parameter should be a Field object with default False
    call_args = mock_admin_client.get_job_run_details.call_args
    assert call_args[0] == (12345, 100)
    debug_field = call_args[1]["debug"]
    # Check that it's a Field with the correct default
    assert hasattr(debug_field, "default") and debug_field.default is False


@patch("dbt_mcp.dbt_admin.tools.get_prompt")
def test_trigger_job_run_with_all_optional_params(
    mock_get_prompt, mock_config, mock_admin_client
):
    mock_get_prompt.return_value = "Trigger job run prompt"

    tool_definitions = create_admin_api_tool_definitions(
        mock_admin_client, mock_config.admin_api_config
    )
    trigger_job_run_tool = tool_definitions[2].fn  # Third tool is trigger_job_run

    result = trigger_job_run_tool(
        job_id=1,
        cause="Manual trigger",
        git_branch="feature-branch",
        git_sha="abc123",
        schema_override="custom_schema",
    )

    assert isinstance(result, dict)
    mock_admin_client.trigger_job_run.assert_called_once_with(
        12345,
        1,
        "Manual trigger",
        git_branch="feature-branch",
        git_sha="abc123",
        schema_override="custom_schema",
    )
