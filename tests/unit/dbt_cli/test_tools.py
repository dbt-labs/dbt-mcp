import subprocess
import inspect
from functools import wraps

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


@pytest.fixture
def mock_fastmcp():
    class MockFastMCP:
        def __init__(self):
            self.tools = {}

        def tool(self, **kwargs):
            def decorator(func):
                # Create a wrapper that handles param defaults with
                # regular defaults AND Pydantic Field objects
                @wraps(func)
                def wrapper(*args, **call_kwargs):
                    sig = inspect.signature(func)

                    processed_kwargs = {}
                    for param_name, param in sig.parameters.items():
                        if param_name in call_kwargs:
                            processed_kwargs[param_name] = call_kwargs[param_name]
                        elif param.default is not inspect.Parameter.empty:
                            default_value = param.default
                            # If the default value is a Pydantic Field object
                            if hasattr(default_value, "default"):
                                processed_kwargs[param_name] = default_value.default
                            else:
                                processed_kwargs[param_name] = default_value

                    return func(**processed_kwargs)

                self.tools[func.__name__] = wrapper
                return func

            return decorator

    fastmcp = MockFastMCP()
    return fastmcp, fastmcp.tools


@pytest.mark.parametrize(
    "sql_query,limit_param,expected_args",
    [
        # SQL with explicit LIMIT - should set --limit=-1
        (
            "SELECT * FROM my_model LIMIT 10",
            None,
            [
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
    register_dbt_cli_tools(fastmcp, mock_dbt_cli_config)
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
    register_dbt_cli_tools(mock_fastmcp_obj, mock_dbt_cli_config)
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
    register_dbt_cli_tools(fastmcp, mock_dbt_cli_config)
    run_tool = tools["run"]

    # Run the command with a selector
    run_tool(selector="my_model")

    # Verify the command is correctly formatted
    assert mock_calls
    args_list = mock_calls[0]
    assert args_list == [
        "/path/to/dbt",
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
    register_dbt_cli_tools(mock_fastmcp_obj, mock_dbt_cli_config)
    show_tool = tools["show"]

    # Execute
    show_tool(sql_query="SELECT * FROM my_model")

    # Verify
    assert mock_calls
    args_list = mock_calls[0]
    assert args_list[0].endswith("dbt")
    assert args_list[1] == "show"
    assert args_list[2] == "--inline"
    assert args_list[3] == "SELECT * FROM my_model"
    assert args_list[4] == "--favor-state"


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
    register_dbt_cli_tools(mock_fastmcp_obj, mock_dbt_cli_config)
    list_tool = tools["ls"]

    # Test timeout case
    result = list_tool(resource_type=["model", "snapshot"])
    assert "Timeout: dbt command took too long to complete" in result
    assert "Try using a specific selector to narrow down the results" in result

    # Test with selector - should still timeout
    result = list_tool(selector="my_model", resource_type=["model"])
    assert "Timeout: dbt command took too long to complete" in result
    assert "Try using a specific selector to narrow down the results" in result
