import inspect
import subprocess

import pytest
from pytest import MonkeyPatch

from dbt_mcp.errors.common import InvalidParameterError
from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools
from dbt_mcp.config.config import DbtCliConfig
from dbt_mcp.dbt_cli.binary_type import BinaryType
from tests.mocks.config import mock_dbt_cli_config


@pytest.fixture
def mock_process():
    class MockProcess:
        def communicate(self, timeout=None):
            return "command output", None

    return MockProcess()


@pytest.mark.parametrize(
    "sql_query,limit_param,expected_args",
    [
        # SQL with explicit LIMIT - should set --limit=-1
        (
            "SELECT * FROM my_model LIMIT 10",
            None,
            [
                "--no-use-colors",
                "show",
                "--inline",
                "SELECT * FROM my_model LIMIT 10",
                "--favor-state",
                "--limit",
                "-1",
                "--output",
                "json",
            ],
        ),
        # SQL with lowercase limit - should set --limit=-1
        (
            "select * from my_model limit 5",
            None,
            [
                "--no-use-colors",
                "show",
                "--inline",
                "select * from my_model limit 5",
                "--favor-state",
                "--limit",
                "-1",
                "--output",
                "json",
            ],
        ),
        # No SQL LIMIT but with limit parameter - should use provided limit
        (
            "SELECT * FROM my_model",
            10,
            [
                "--no-use-colors",
                "show",
                "--inline",
                "SELECT * FROM my_model",
                "--favor-state",
                "--limit",
                "10",
                "--output",
                "json",
            ],
        ),
        # No limits at all - should not include --limit flag
        (
            "SELECT * FROM my_model",
            None,
            [
                "--no-use-colors",
                "show",
                "--inline",
                "SELECT * FROM my_model",
                "--favor-state",
                "--output",
                "json",
            ],
        ),
    ],
)
def test_show_command_limit_logic(
    monkeypatch: MonkeyPatch,
    mock_process,
    mock_fastmcp,
    sql_query,
    limit_param,
    expected_args,
):
    # Mock Popen
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    # Register tools and get show tool
    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    show_tool = tools["show"]

    # Call show tool with test parameters
    show_tool(sql_query=sql_query, limit=limit_param)

    # Verify the command was called with expected arguments
    assert mock_calls
    args_list = mock_calls[0][1:]  # Skip the dbt path
    assert args_list == expected_args


def test_run_command_adds_quiet_flag_to_verbose_commands(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    # Mock Popen
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    # Setup
    mock_fastmcp_obj, tools = mock_fastmcp
    register_dbt_cli_tools(
        mock_fastmcp_obj,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    run_tool = tools["run"]

    # Execute
    run_tool()

    # Verify
    assert mock_calls
    args_list = mock_calls[0]
    assert "--quiet" in args_list


def test_run_command_correctly_formatted(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    # Mock Popen
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp

    # Register the tools
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    run_tool = tools["run"]

    # Run the command with a selection
    run_tool(node_selection="my_model")

    # Verify the command is correctly formatted
    assert mock_calls
    args_list = mock_calls[0]
    assert args_list == [
        "/path/to/dbt",
        "--no-use-colors",
        "run",
        "--quiet",
        "--select",
        "my_model",
    ]


def test_show_command_correctly_formatted(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    # Mock Popen
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    # Setup
    mock_fastmcp_obj, tools = mock_fastmcp
    register_dbt_cli_tools(
        mock_fastmcp_obj,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    show_tool = tools["show"]

    # Execute
    show_tool(sql_query="SELECT * FROM my_model")

    # Verify
    assert mock_calls
    args_list = mock_calls[0]
    assert args_list[0].endswith("dbt")
    assert args_list[1] == "--no-use-colors"
    assert args_list[2] == "show"
    assert args_list[3] == "--inline"
    assert args_list[4] == "SELECT * FROM my_model"
    assert args_list[5] == "--favor-state"


def test_list_command_timeout_handling(monkeypatch: MonkeyPatch, mock_fastmcp):
    # Mock Popen
    class MockProcessWithTimeout:
        def communicate(self, timeout=None):
            raise subprocess.TimeoutExpired(cmd=["dbt", "list"], timeout=10)

    def mock_popen(*args, **kwargs):
        return MockProcessWithTimeout()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    # Setup
    mock_fastmcp_obj, tools = mock_fastmcp
    register_dbt_cli_tools(
        mock_fastmcp_obj,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    list_tool = tools["ls"]

    # Test timeout case
    result = list_tool(resource_type=["model", "snapshot"])
    assert "Timeout: dbt command took too long to complete" in result
    assert "Try using a specific selector to narrow down the results" in result

    # Test with selection - should still timeout
    result = list_tool(node_selection="my_model", resource_type=["model"])
    assert "Timeout: dbt command took too long to complete" in result
    assert "Try using a specific selector to narrow down the results" in result


@pytest.mark.parametrize("command_name", ["run", "build", "clone"])
def test_full_refresh_flag_added_to_command(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, command_name
):
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    tool = tools[command_name]

    tool(is_full_refresh=True)

    assert mock_calls
    args_list = mock_calls[0]
    assert "--full-refresh" in args_list


@pytest.mark.parametrize("command_name", ["build", "run", "test"])
def test_vars_flag_added_to_command(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, command_name
):
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    tool = tools[command_name]

    tool(vars="environment: production")

    assert mock_calls
    args_list = mock_calls[0]
    assert "--vars" in args_list
    assert "environment: production" in args_list


@pytest.mark.parametrize("command_name", ["run", "build"])
def test_sample_flag_added_to_command(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, command_name
):
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    tool = tools[command_name]

    tool(sample="3 days")

    assert mock_calls
    args_list = mock_calls[0]
    assert "--sample" in args_list
    assert "3 days" in args_list


def test_sample_not_added_when_none(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    build_tool = tools["build"]

    build_tool()

    assert mock_calls
    args_list = mock_calls[0]
    assert "--sample" not in args_list


def test_vars_not_added_when_none(monkeypatch: MonkeyPatch, mock_process, mock_fastmcp):
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    build_tool = tools["build"]

    build_tool()  # Non-explicit

    assert mock_calls
    args_list = mock_calls[0]
    assert "--vars" not in args_list


def test_compile_supports_selection(
    monkeypatch: MonkeyPatch,
    mock_process,
    mock_fastmcp,
):
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    compile_tool = tools["compile"]

    assert "node_selection" in inspect.signature(compile_tool).parameters

    compile_tool(node_selection="my_model")
    assert "--select" in mock_calls[0]
    assert "my_model" in mock_calls[0]


@pytest.mark.parametrize(
    "command_name", ["build", "run", "test", "compile", "ls", "clone"]
)
def test_yml_selector_flag_added_to_command(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, command_name
):
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    tool = tools[command_name]

    assert "yml_selector" in inspect.signature(tool).parameters

    tool(yml_selector="nightly")

    assert mock_calls
    args_list = mock_calls[0]
    assert "--selector" in args_list
    assert "nightly" in args_list
    assert "--select" not in args_list


def test_yml_selector_not_added_when_none(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    build_tool = tools["build"]

    build_tool()

    assert mock_calls
    args_list = mock_calls[0]
    assert "--selector" not in args_list


def test_stderr_not_merged_into_stdout(monkeypatch: MonkeyPatch, mock_fastmcp):
    captured_kwargs: dict = {}

    class MockProcessWithStderrWarning:
        returncode = 0

        def communicate(self, timeout=None):
            return "", "urllib3 v2.0 only supports OpenSSL 1.1.1+: NotOpenSSLWarning"

    def mock_popen(args, **kwargs):
        captured_kwargs.update(kwargs)
        return MockProcessWithStderrWarning()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    build_tool = tools["build"]

    result = build_tool()

    assert captured_kwargs.get("stderr") == subprocess.PIPE
    assert result == "OK"


def test_failure_output_returned_when_dbt_errors(
    monkeypatch: MonkeyPatch, mock_fastmcp
):
    class MockProcessWithFailure:
        returncode = 1

        def communicate(self, timeout=None):
            return "Compilation Error in model my_model", ""

    def mock_popen(args, **kwargs):
        return MockProcessWithFailure()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    build_tool = tools["build"]

    result = build_tool()

    assert result == "Compilation Error in model my_model"


def test_clone_command_binary_state_path_logic(
    monkeypatch: MonkeyPatch,
    mock_process,
    mock_fastmcp,
):
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp

    # Case 1: DBT_CORE (--state should be added)
    core_cli_config = DbtCliConfig(
        project_dir="/test/project",
        dbt_path="/path/to/dbt",
        dbt_cli_timeout=10,
        binary_type=BinaryType.DBT_CORE,
    )
    register_dbt_cli_tools(
        fastmcp,
        core_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    clone_tool = tools["clone"]
    clone_tool(state_path="/some/state/path")

    assert "--state" in mock_calls[0]
    assert "/some/state/path" in mock_calls[0]

    # Case 2: DBT_CLOUD_CLI (--state should NOT be added)
    mock_calls.clear()
    cloud_cli_config = DbtCliConfig(
        project_dir="/test/project",
        dbt_path="/path/to/dbt",
        dbt_cli_timeout=10,
        binary_type=BinaryType.DBT_CLOUD_CLI,
    )
    register_dbt_cli_tools(
        fastmcp,
        cloud_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    clone_tool = tools["clone"]

    with pytest.raises(InvalidParameterError) as excinfo:
        clone_tool(state_path="/some/state/path")

    assert "--state is not supported" in str(excinfo.value)

    # Case 3: FUSION (--state should be added)
    mock_calls.clear()
    fusion_cli_config = DbtCliConfig(
        project_dir="/test/project",
        dbt_path="/path/to/dbt",
        dbt_cli_timeout=10,
        binary_type=BinaryType.FUSION,
    )
    register_dbt_cli_tools(
        fastmcp,
        fusion_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    clone_tool = tools["clone"]
    clone_tool(state_path="/some/state/path")

    assert "--state" in mock_calls[0]
    assert "/some/state/path" in mock_calls[0]
