import subprocess

import pytest
from pytest import MonkeyPatch

from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools
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

    # Run the command with a selector
    run_tool(selector="my_model")

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

    # Test with selector - should still timeout
    result = list_tool(selector="my_model", resource_type=["model"])
    assert "Timeout: dbt command took too long to complete" in result
    assert "Try using a specific selector to narrow down the results" in result


@pytest.mark.parametrize("command_name", ["run", "build"])
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


@pytest.mark.parametrize("command_name", ["build", "run", "test", "ls", "compile"])
def test_exclude_flag_added_to_command(
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

    tool(exclude="staging_models")

    assert mock_calls
    args_list = mock_calls[0]
    assert "--exclude" in args_list
    assert "staging_models" in args_list


@pytest.mark.parametrize("command_name", ["build", "run", "test", "compile", "show"])
def test_defer_flag_with_state_parameter(
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

    if command_name == "show":
        tool(sql_query="SELECT 1", defer=True, state="/path/to/state")
    else:
        tool(defer=True, state="/path/to/state")

    assert mock_calls
    args_list = mock_calls[0]
    assert "--defer" in args_list
    assert "--state" in args_list
    assert "/path/to/state" in args_list


@pytest.mark.parametrize("command_name", ["build", "run", "test", "compile"])
def test_defer_flag_without_state_returns_error(
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

    result = tool(defer=True)

    # Should return error message, not call subprocess
    assert not mock_calls
    assert "Error: --defer requires a state path" in result


@pytest.mark.parametrize("command_name", ["build", "run", "test", "ls", "compile"])
def test_state_flag_without_defer(
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

    tool(state="/path/to/state")

    assert mock_calls
    args_list = mock_calls[0]
    assert "--state" in args_list
    assert "/path/to/state" in args_list
    assert "--defer" not in args_list


@pytest.mark.parametrize(
    "command_name", ["build", "run", "test", "ls", "compile", "parse", "docs", "show"]
)
def test_target_flag_added_to_command(
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

    if command_name == "show":
        tool(sql_query="SELECT 1", target="dev")
    else:
        tool(target="dev")

    assert mock_calls
    args_list = mock_calls[0]
    assert "--target" in args_list
    assert "dev" in args_list


@pytest.mark.parametrize("command_name", ["build", "run", "test"])
def test_fail_fast_flag_added_to_command(
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

    tool(fail_fast=True)

    assert mock_calls
    args_list = mock_calls[0]
    assert "--fail-fast" in args_list


@pytest.mark.parametrize("command_name", ["build", "run", "test", "compile"])
def test_threads_flag_added_to_command(
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

    tool(threads=8)

    assert mock_calls
    args_list = mock_calls[0]
    assert "--threads" in args_list
    assert "8" in args_list


def test_defer_uses_config_state_path(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    from dbt_mcp.config.config import DbtCliConfig
    from dbt_mcp.dbt_cli.binary_type import BinaryType

    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    # Create config with state_path set
    config_with_state = DbtCliConfig(
        project_dir="/test/project",
        dbt_path="/path/to/dbt",
        dbt_cli_timeout=10,
        binary_type=BinaryType.DBT_CORE,
        state_path="/default/state/path",
        target=None,
    )

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        config_with_state,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    build_tool = tools["build"]

    # Call with defer=True but no state parameter
    build_tool(defer=True)

    assert mock_calls
    args_list = mock_calls[0]
    assert "--defer" in args_list
    assert "--state" in args_list
    assert "/default/state/path" in args_list


def test_target_uses_config_default(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    from dbt_mcp.config.config import DbtCliConfig
    from dbt_mcp.dbt_cli.binary_type import BinaryType

    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    # Create config with target set
    config_with_target = DbtCliConfig(
        project_dir="/test/project",
        dbt_path="/path/to/dbt",
        dbt_cli_timeout=10,
        binary_type=BinaryType.DBT_CORE,
        state_path=None,
        target="staging",
    )

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        config_with_target,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    run_tool = tools["run"]

    # Call without target parameter - should use config default
    run_tool()

    assert mock_calls
    args_list = mock_calls[0]
    assert "--target" in args_list
    assert "staging" in args_list


def test_target_param_overrides_config_default(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    from dbt_mcp.config.config import DbtCliConfig
    from dbt_mcp.dbt_cli.binary_type import BinaryType

    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    # Create config with target set
    config_with_target = DbtCliConfig(
        project_dir="/test/project",
        dbt_path="/path/to/dbt",
        dbt_cli_timeout=10,
        binary_type=BinaryType.DBT_CORE,
        state_path=None,
        target="staging",
    )

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        config_with_target,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    run_tool = tools["run"]

    # Call with target parameter - should override config default
    run_tool(target="production")

    assert mock_calls
    args_list = mock_calls[0]
    assert "--target" in args_list
    assert "production" in args_list
    assert "staging" not in args_list
