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
        returncode = 0

        def communicate(self, timeout=None):
            return "command output", ""

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


def test_success_returns_ok_even_with_stderr_noise(
    monkeypatch: MonkeyPatch, mock_fastmcp
):
    """A successful command (returncode=0) returns 'OK' even when stderr has
    noise like urllib3 deprecation warnings — stderr is dropped on success."""

    class MockProcessSuccessWithStderrWarning:
        returncode = 0

        def communicate(self, timeout=None):
            return "", "urllib3 v2.0 only supports OpenSSL 1.1.1+: NotOpenSSLWarning"

    def mock_popen(args, **kwargs):
        return MockProcessSuccessWithStderrWarning()

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

    assert tools["build"]() == "OK"


def test_failure_surfaces_stderr_when_stdout_is_empty(
    monkeypatch: MonkeyPatch, mock_fastmcp
):
    """A failed command (returncode!=0) surfaces stderr — some dbt errors
    only appear there, e.g. authentication or connection problems."""

    class MockProcessFailureStderrOnly:
        returncode = 1

        def communicate(self, timeout=None):
            return "", "Database Error: could not connect to server"

    def mock_popen(args, **kwargs):
        return MockProcessFailureStderrOnly()

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

    result = tools["build"]()

    assert "Database Error" in result
    assert result != "OK"


def test_failure_with_no_output_surfaces_exit_code(
    monkeypatch: MonkeyPatch, mock_fastmcp
):
    """A failed command with empty stdout AND stderr must NOT return 'OK' —
    surface the exit code so the LLM can tell the call actually failed."""

    class MockProcessFailureNoOutput:
        returncode = 137  # e.g. OOM-killed

        def communicate(self, timeout=None):
            return "", ""

    def mock_popen(args, **kwargs):
        return MockProcessFailureNoOutput()

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

    result = tools["build"]()

    assert result != "OK"
    assert "137" in result
    assert "exit code" in result.lower()


def test_failure_combines_stdout_and_stderr(monkeypatch: MonkeyPatch, mock_fastmcp):
    """When both streams have content on failure, both are surfaced."""

    class MockProcessFailureBothStreams:
        returncode = 1

        def communicate(self, timeout=None):
            return "Compilation Error in model my_model", "Stack trace here"

    def mock_popen(args, **kwargs):
        return MockProcessFailureBothStreams()

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

    result = tools["build"]()

    assert "--- stdout ---" in result
    assert "Compilation Error" in result
    assert "--- stderr ---" in result
    assert "Stack trace" in result


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


@pytest.mark.parametrize(
    "injection_payload",
    [
        "--profiles-dir /tmp/custom",
        "--project-dir /tmp/custom",
        "--target custom",
        "my_model --profiles-dir /tmp/custom",
        "-x",
    ],
)
def test_node_selection_rejects_flag_injection(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, injection_payload
):
    monkeypatch.setattr("subprocess.Popen", lambda *a, **kw: mock_process)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    with pytest.raises(InvalidParameterError, match="must not start with '-'"):
        tools["run"](node_selection=injection_payload)


@pytest.mark.parametrize(
    "valid_selection",
    [
        "my_model",
        "my_model my_other_model",
        "+my_model",
        "my_model+",
        "1+my_model+1",
        "tag:nightly",
        "config.materialized:table",
        "path/to/models/",
        "source:my_source",
        "@my_model",
    ],
)
def test_node_selection_accepts_valid_tokens(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, valid_selection
):
    monkeypatch.setattr("subprocess.Popen", lambda *a, **kw: mock_process)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    tools["run"](node_selection=valid_selection)


@pytest.mark.parametrize(
    "injection_payload",
    [
        ["model", "--profiles-dir", "/tmp/custom"],
        ["--target", "custom"],
    ],
)
def test_resource_type_rejects_flag_injection(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, injection_payload
):
    monkeypatch.setattr("subprocess.Popen", lambda *a, **kw: mock_process)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    with pytest.raises(InvalidParameterError, match="invalid values"):
        tools["ls"](resource_type=injection_payload)


@pytest.mark.parametrize(
    "valid_types",
    [
        ["model"],
        ["model", "snapshot"],
        ["source", "seed", "test"],
        ["semantic_model", "metric", "saved_query"],
    ],
)
def test_resource_type_accepts_valid_values(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, valid_types
):
    monkeypatch.setattr("subprocess.Popen", lambda *a, **kw: mock_process)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    tools["ls"](resource_type=valid_types)
