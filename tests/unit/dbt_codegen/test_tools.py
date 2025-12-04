import json
import subprocess

import pytest
from pytest import MonkeyPatch

from dbt_mcp.dbt_codegen.tools import register_dbt_codegen_tools
from tests.mocks.config import mock_dbt_codegen_config


@pytest.fixture
def mock_process():
    class MockProcess:
        def __init__(self, returncode=0, output="command output"):
            self.returncode = returncode
            self._output = output

        def communicate(self, timeout=None):
            return self._output, None

    return MockProcess


def test_generate_source_basic_schema(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    """Test generate_source with just schema_name parameter."""
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process()

    # Patch subprocess BEFORE registering tools
    monkeypatch.setattr("subprocess.Popen", mock_popen)

    # Now register tools with the mock in place
    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(
        fastmcp,
        mock_dbt_codegen_config,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    generate_source_tool = fastmcp.tools["generate_source"]

    # Call with just schema_name (provide all required args explicitly)
    generate_source_tool(
        schema_name="raw_data",
        database_name=None,
        table_names=None,
        generate_columns=False,
        include_descriptions=False,
    )

    # Verify the command was called correctly
    assert mock_calls
    args_list = mock_calls[0]

    # Check basic command structure
    assert args_list[0] == "/path/to/dbt"
    assert "--no-use-colors" in args_list
    assert "run-operation" in args_list
    assert "--quiet" in args_list
    assert "generate_source" in args_list

    # Check that args were passed correctly
    assert "--args" in args_list
    args_index = args_list.index("--args")
    args_json = json.loads(args_list[args_index + 1])
    assert args_json["schema_name"] == "raw_data"


def test_generate_source_with_all_parameters(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    """Test generate_source with all parameters."""
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(
        fastmcp,
        mock_dbt_codegen_config,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    generate_source_tool = fastmcp.tools["generate_source"]

    # Call with all parameters
    generate_source_tool(
        schema_name="raw_data",
        database_name="analytics",
        table_names=["users", "orders"],
        generate_columns=True,
        include_descriptions=True,
    )

    # Verify the args were passed correctly
    assert mock_calls
    args_list = mock_calls[0]
    args_index = args_list.index("--args")
    args_json = json.loads(args_list[args_index + 1])

    assert args_json["schema_name"] == "raw_data"
    assert args_json["database_name"] == "analytics"
    assert args_json["table_names"] == ["users", "orders"]
    assert args_json["generate_columns"] is True
    assert args_json["include_descriptions"] is True


def test_generate_model_yaml(monkeypatch: MonkeyPatch, mock_process, mock_fastmcp):
    """Test generate_model_yaml function."""
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(
        fastmcp,
        mock_dbt_codegen_config,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    generate_model_yaml_tool = fastmcp.tools["generate_model_yaml"]

    # Call the tool
    generate_model_yaml_tool(
        model_names=["stg_users", "stg_orders"],
        upstream_descriptions=True,
        include_data_types=False,
    )

    # Verify the command
    assert mock_calls
    args_list = mock_calls[0]
    assert "generate_model_yaml" in args_list

    args_index = args_list.index("--args")
    args_json = json.loads(args_list[args_index + 1])
    assert args_json["model_names"] == ["stg_users", "stg_orders"]
    assert args_json["upstream_descriptions"] is True
    assert args_json["include_data_types"] is False


def test_generate_staging_model(monkeypatch: MonkeyPatch, mock_process, mock_fastmcp):
    """Test generate_staging_model function."""
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(
        fastmcp,
        mock_dbt_codegen_config,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    generate_staging_model_tool = fastmcp.tools["generate_staging_model"]

    # Call the tool
    generate_staging_model_tool(
        source_name="raw_data",
        table_name="users",
        leading_commas=True,
        case_sensitive_cols=False,
        materialized="view",
    )

    # Verify the command
    assert mock_calls
    args_list = mock_calls[0]
    # Note: Still calls the underlying dbt-codegen macro generate_base_model
    assert "generate_base_model" in args_list

    args_index = args_list.index("--args")
    args_json = json.loads(args_list[args_index + 1])
    assert args_json["source_name"] == "raw_data"
    assert args_json["table_name"] == "users"
    assert args_json["leading_commas"] is True
    assert args_json["case_sensitive_cols"] is False
    assert args_json["materialized"] == "view"


def test_codegen_error_handling_missing_package(monkeypatch: MonkeyPatch, mock_fastmcp):
    """Test error handling when dbt-codegen package is not installed."""
    mock_calls = []

    class MockProcessWithError:
        def __init__(self):
            self.returncode = 1

        def communicate(self, timeout=None):
            return "dbt found 1 resource of type macro", None

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return MockProcessWithError()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(
        fastmcp,
        mock_dbt_codegen_config,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    generate_source_tool = fastmcp.tools["generate_source"]

    # Call should return error message about missing package
    result = generate_source_tool(
        schema_name="test_schema",
        database_name=None,
        table_names=None,
        generate_columns=False,
        include_descriptions=False,
    )

    assert "dbt-codegen package may not be installed" in result
    assert "Run 'dbt deps'" in result


def test_codegen_error_handling_general_error(monkeypatch: MonkeyPatch, mock_fastmcp):
    """Test general error handling."""
    mock_calls = []

    class MockProcessWithError:
        def __init__(self):
            self.returncode = 1

        def communicate(self, timeout=None):
            return "Some other error occurred", None

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return MockProcessWithError()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(
        fastmcp,
        mock_dbt_codegen_config,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    generate_source_tool = fastmcp.tools["generate_source"]

    # Call should return the error
    result = generate_source_tool(
        schema_name="test_schema",
        database_name=None,
        table_names=None,
        generate_columns=False,
        include_descriptions=False,
    )

    assert "Error running dbt-codegen macro" in result
    assert "Some other error occurred" in result


def test_codegen_timeout_handling(monkeypatch: MonkeyPatch, mock_fastmcp):
    """Test timeout handling for long-running operations."""

    class MockProcessWithTimeout:
        def communicate(self, timeout=None):
            raise subprocess.TimeoutExpired(cmd=["dbt", "run-operation"], timeout=10)

    def mock_popen(*args, **kwargs):
        return MockProcessWithTimeout()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(
        fastmcp,
        mock_dbt_codegen_config,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    generate_source_tool = fastmcp.tools["generate_source"]

    # Test timeout case
    result = generate_source_tool(
        schema_name="large_schema",
        database_name=None,
        table_names=None,
        generate_columns=False,
        include_descriptions=False,
    )
    assert "Timeout: dbt-codegen operation took longer than" in result
    assert "10 seconds" in result


def test_quiet_flag_placement(monkeypatch: MonkeyPatch, mock_process, mock_fastmcp):
    """Test that --quiet flag is placed correctly in the command."""
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(
        fastmcp,
        mock_dbt_codegen_config,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    generate_source_tool = fastmcp.tools["generate_source"]

    # Call the tool
    generate_source_tool(
        schema_name="test",
        database_name=None,
        table_names=None,
        generate_columns=False,
        include_descriptions=False,
    )

    # Verify --quiet is placed after run-operation
    assert mock_calls
    args_list = mock_calls[0]

    run_op_index = args_list.index("run-operation")
    quiet_index = args_list.index("--quiet")

    # --quiet should come right after run-operation
    assert quiet_index == run_op_index + 1


def test_absolute_path_handling(monkeypatch: MonkeyPatch, mock_process, mock_fastmcp):
    """Test that absolute paths are handled correctly."""
    mock_calls = []
    captured_kwargs = {}

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        captured_kwargs.update(kwargs)
        return mock_process()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(
        fastmcp,
        mock_dbt_codegen_config,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    generate_source_tool = fastmcp.tools["generate_source"]

    # Call the tool (mock config has /test/project which is absolute)
    generate_source_tool(
        schema_name="test",
        database_name=None,
        table_names=None,
        generate_columns=False,
        include_descriptions=False,
    )

    # Verify cwd was set for absolute path
    assert "cwd" in captured_kwargs
    assert captured_kwargs["cwd"] == "/test/project"


def test_all_tools_registered(mock_fastmcp):
    """Test that all expected tools are registered."""
    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(
        fastmcp,
        mock_dbt_codegen_config,
        disabled_tools=set(),
        enabled_tools=set(),
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    tools = fastmcp.tools

    expected_tools = [
        "generate_source",
        "generate_model_yaml",
        "generate_staging_model",
    ]

    for tool_name in expected_tools:
        assert tool_name in tools, f"Tool {tool_name} not registered"
