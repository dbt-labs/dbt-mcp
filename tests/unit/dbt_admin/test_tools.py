import json
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

from client.session import client_session_context
from dbt_mcp.dbt_admin.param_descriptions import PAGINATION_LIMIT, PAGINATION_OFFSET
from dbt_mcp.dbt_admin.tools import (
    ADMIN_TOOLS,
    AdminToolContext,
    JobRunStatus,
    INLINE_CONTENT_LIMIT,
    cancel_job_run,
    get_job_details,
    get_job_run_artifacts,
    get_job_run_details,
    get_job_run_error,
    list_job_run_artifacts,
    list_jobs,
    list_jobs_runs,
    register_admin_api_tools,
    retry_job_run,
    trigger_job_run,
)
from dbt_mcp.mcp.server import register_multi_project_dbt_mcp
from tests.mocks.config import mock_config

NUM_ADMIN_TOOLS = 11


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
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    # Should call register_tools with 11 tool definitions
    mock_register_tools.assert_called_once()
    args, kwargs = mock_register_tools.call_args
    tool_definitions = kwargs["tool_definitions"]
    assert len(tool_definitions) == NUM_ADMIN_TOOLS


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
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    # Should still call register_tools with all 12 tool definitions
    # The exclude_tools parameter is passed to register_tools to handle filtering
    mock_register_tools.assert_called_once()
    args, kwargs = mock_register_tools.call_args
    tool_definitions = kwargs["tool_definitions"]
    disabled_tools = kwargs["disabled_tools"]
    assert len(tool_definitions) == NUM_ADMIN_TOOLS
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


async def test_get_job_run_artifacts_tool_string_output(admin_context):
    admin_context.admin_client.get_job_run_artifact = AsyncMock(
        return_value='{"nodes": {}}'
    )
    result = await get_job_run_artifacts.fn(
        admin_context, run_id=100, artifact_path="manifest.json"
    )

    assert result == '{"nodes": {}}'
    admin_context.admin_client.get_job_run_artifact.assert_called_once_with(
        12345, 100, "manifest.json", step=None
    )


async def test_get_job_run_artifacts_tool_file_output(admin_context, tmp_path):
    admin_context.admin_client.get_job_run_artifact = AsyncMock(
        return_value='{"nodes": {}}'
    )
    output_file = tmp_path / "manifest.json"
    result = await get_job_run_artifacts.fn(
        admin_context,
        run_id=100,
        artifact_path="manifest.json",
        output_path=str(output_file),
    )

    assert result == f"Artifact written to {output_file}"
    assert output_file.read_text() == '{"nodes": {}}'


async def test_get_job_run_artifacts_tool_with_step(admin_context):
    admin_context.admin_client.get_job_run_artifact = AsyncMock(
        return_value="log content"
    )
    await get_job_run_artifacts.fn(
        admin_context, run_id=100, artifact_path="logs/dbt.log", step=2
    )

    admin_context.admin_client.get_job_run_artifact.assert_called_once_with(
        12345, 100, "logs/dbt.log", step=2
    )


@pytest.mark.parametrize(
    "content_size,expect_inline",
    [
        (1024, True),  # 1 KB — well below limit
        (INLINE_CONTENT_LIMIT - 1, True),  # 1 byte below limit — inline
        (INLINE_CONTENT_LIMIT, True),  # exactly at limit — still inline (strict >)
        (INLINE_CONTENT_LIMIT + 1, False),  # 1 byte over limit — temp file
        (INLINE_CONTENT_LIMIT * 2, False),  # well over limit
    ],
)
async def test_get_job_run_artifacts_size_routing(
    admin_context, content_size, expect_inline
):
    content = "x" * content_size
    admin_context.admin_client.get_job_run_artifact = AsyncMock(return_value=content)

    result = await get_job_run_artifacts.fn(
        admin_context, run_id=100, artifact_path="manifest.json"
    )

    if expect_inline:
        assert result == content
    else:
        path = Path(result.removeprefix("Artifact written to "))
        assert path.exists()
        assert path.read_text() == content
        path.unlink()


async def test_get_job_run_artifacts_auto_temp_preserves_suffix(admin_context):
    content = "x" * (INLINE_CONTENT_LIMIT + 1)
    admin_context.admin_client.get_job_run_artifact = AsyncMock(return_value=content)

    result = await get_job_run_artifacts.fn(
        admin_context, run_id=100, artifact_path="run_results.json"
    )

    path = Path(result.removeprefix("Artifact written to "))
    assert path.suffix == ".json"
    path.unlink()


async def test_get_job_run_artifacts_explicit_output_path_bypasses_size_check(
    admin_context, tmp_path
):
    content = "x" * 100  # tiny — would be inline without output_path
    admin_context.admin_client.get_job_run_artifact = AsyncMock(return_value=content)
    out = tmp_path / "out.json"

    result = await get_job_run_artifacts.fn(
        admin_context, run_id=100, artifact_path="manifest.json", output_path=str(out)
    )

    assert result == f"Artifact written to {out}"
    assert out.read_text() == content


async def test_get_job_run_artifacts_jq_filter_extracts_field(admin_context):
    content = '{"results": [{"status": "error", "unique_id": "model.proj.a"}, {"status": "success", "unique_id": "model.proj.b"}]}'
    admin_context.admin_client.get_job_run_artifact = AsyncMock(return_value=content)

    result = await get_job_run_artifacts.fn(
        admin_context,
        run_id=100,
        artifact_path="run_results.json",
        jq_filter='.results[] | select(.status == "error") | .unique_id',
    )

    assert json.loads(result) == ["model.proj.a"]


async def test_get_job_run_artifacts_jq_filter_empty_result_returns_json_array(
    admin_context,
):
    content = '{"results": [{"status": "success", "unique_id": "model.proj.a"}]}'
    admin_context.admin_client.get_job_run_artifact = AsyncMock(return_value=content)

    result = await get_job_run_artifacts.fn(
        admin_context,
        run_id=100,
        artifact_path="run_results.json",
        jq_filter='.results[] | select(.status == "error")',
    )

    assert json.loads(result) == []


async def test_get_job_run_artifacts_jq_filter_returns_inline_regardless_of_size(
    admin_context,
):
    padding = "x" * (INLINE_CONTENT_LIMIT + 1)
    content = json.dumps({"padding": padding})
    assert len(content) > INLINE_CONTENT_LIMIT
    admin_context.admin_client.get_job_run_artifact = AsyncMock(return_value=content)

    result = await get_job_run_artifacts.fn(
        admin_context,
        run_id=100,
        artifact_path="manifest.json",
        jq_filter=".padding | length",
    )

    assert json.loads(result) == [INLINE_CONTENT_LIMIT + 1]


async def test_get_job_run_artifacts_output_path_and_jq_filter_conflict(admin_context):
    admin_context.admin_client.get_job_run_artifact = AsyncMock(
        return_value='{"nodes": {}}'
    )

    with pytest.raises(ValueError, match="cannot be used together"):
        await get_job_run_artifacts.fn(
            admin_context,
            run_id=100,
            artifact_path="manifest.json",
            output_path="/tmp/out.json",
            jq_filter=".nodes",
        )
    admin_context.admin_client.get_job_run_artifact.assert_not_called()


async def test_get_job_run_artifacts_jq_filter_invalid_syntax(admin_context):
    admin_context.admin_client.get_job_run_artifact = AsyncMock(
        return_value='{"key": "value"}'
    )

    with pytest.raises(ValueError, match="Invalid jq filter:"):
        await get_job_run_artifacts.fn(
            admin_context,
            run_id=100,
            artifact_path="run_results.json",
            jq_filter="not valid jq |||",
        )


async def test_get_job_run_artifacts_output_path_nonexistent_directory_returns_error(
    admin_context,
):
    admin_context.admin_client.get_job_run_artifact = AsyncMock(
        return_value='{"nodes": {}}'
    )

    with pytest.raises(
        ValueError, match="Could not write to /nonexistent/dir/out.json:"
    ):
        await get_job_run_artifacts.fn(
            admin_context,
            run_id=100,
            artifact_path="manifest.json",
            output_path="/nonexistent/dir/out.json",
        )


async def test_get_job_run_artifacts_jq_filter_non_json_artifact(admin_context):
    admin_context.admin_client.get_job_run_artifact = AsyncMock(
        return_value="SELECT * FROM my_table"
    )

    with pytest.raises(ValueError, match="not valid JSON"):
        await get_job_run_artifacts.fn(
            admin_context,
            run_id=100,
            artifact_path="compiled/model.sql",
            jq_filter=".nodes",
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


async def test_admin_tools_registered_in_multi_project_mcp(mock_fastmcp):
    fastmcp, tools = mock_fastmcp
    await register_multi_project_dbt_mcp(fastmcp, mock_config)
    admin_tool_names = {tool.fn.__name__ for tool in ADMIN_TOOLS}
    assert admin_tool_names.issubset(tools.keys())


async def test_trigger_job_run_with_all_optional_params(admin_context):
    result = await trigger_job_run.fn(
        admin_context,
        job_id=1,
        cause="Manual trigger",
        git_branch="feature-branch",
        git_sha="abc123",
        schema_override="custom_schema",
        dbt_version_override="latest",
    )

    assert isinstance(result, dict)
    admin_context.admin_client.trigger_job_run.assert_called_once_with(
        12345,
        1,
        "Manual trigger",
        git_branch="feature-branch",
        git_sha="abc123",
        schema_override="custom_schema",
        dbt_version_override="latest",
    )


async def test_trigger_job_run_with_steps_override(admin_context):
    steps = ["dbt run --select my_model+ --full-refresh"]
    result = await trigger_job_run.fn(
        admin_context,
        job_id=1,
        cause="Selective build",
        steps_override=steps,
    )

    assert isinstance(result, dict)
    admin_context.admin_client.trigger_job_run.assert_called_once_with(
        12345, 1, "Selective build", steps_override=steps
    )


async def test_trigger_job_run_steps_override_empty_list_is_passed_through(
    admin_context,
):
    result = await trigger_job_run.fn(
        admin_context,
        job_id=1,
        cause="Empty override",
        steps_override=[],
    )

    assert isinstance(result, dict)
    admin_context.admin_client.trigger_job_run.assert_called_once_with(
        12345, 1, "Empty override", steps_override=[]
    )


async def test_trigger_job_run_steps_override_none_not_passed(admin_context):
    await trigger_job_run.fn(admin_context, job_id=1, cause="No override")

    admin_context.admin_client.trigger_job_run.assert_called_once_with(
        12345, 1, "No override"
    )


async def test_trigger_job_run_with_dbt_version_override(admin_context):
    result = await trigger_job_run.fn(
        admin_context,
        job_id=1,
        cause="Upgrade verification",
        dbt_version_override="latest",
    )

    assert isinstance(result, dict)
    admin_context.admin_client.trigger_job_run.assert_called_once_with(
        12345, 1, "Upgrade verification", dbt_version_override="latest"
    )


async def test_trigger_job_run_dbt_version_override_none_not_passed(admin_context):
    await trigger_job_run.fn(admin_context, job_id=1, cause="No version override")

    admin_context.admin_client.trigger_job_run.assert_called_once_with(
        12345, 1, "No version override"
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
        "list_projects",
        "list_jobs",
        "get_job_details",
        "trigger_job_run",
        "list_jobs_runs",
        "get_job_run_details",
        "cancel_job_run",
        "retry_job_run",
        "list_job_run_artifacts",
        "get_job_run_artifacts",
        "get_job_run_error",
    }

    actual_tool_names = {tool.fn.__name__ for tool in ADMIN_TOOLS}
    assert actual_tool_names == expected_tool_names
    assert len(ADMIN_TOOLS) == NUM_ADMIN_TOOLS


async def test_admin_tools_list_jobs_params():
    """Test that the list_jobs tool has the correct parameters."""
    async with client_session_context() as client:
        available_tools = (await client.list_tools()).tools
        list_jobs_tool = next(
            tool for tool in available_tools if tool.name == "list_jobs"
        )
        assert list_jobs_tool.inputSchema is not None
        props = list_jobs_tool.inputSchema.get("properties")
        assert props is not None
        assert props["limit"]["description"] == PAGINATION_LIMIT
        assert props["offset"]["description"] == PAGINATION_OFFSET
