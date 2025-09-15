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
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
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
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
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
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
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


def test_generate_base_model(monkeypatch: MonkeyPatch, mock_process, mock_fastmcp):
    """Test generate_base_model function."""
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
    generate_base_model_tool = fastmcp.tools["generate_base_model"]

    # Call the tool
    generate_base_model_tool(
        source_name="raw_data",
        table_name="users",
        leading_commas=True,
        case_sensitive_cols=False,
        materialized="view",
    )

    # Verify the command
    assert mock_calls
    args_list = mock_calls[0]
    assert "generate_base_model" in args_list

    args_index = args_list.index("--args")
    args_json = json.loads(args_list[args_index + 1])
    assert args_json["source_name"] == "raw_data"
    assert args_json["table_name"] == "users"
    assert args_json["leading_commas"] is True
    assert args_json["case_sensitive_cols"] is False
    assert args_json["materialized"] == "view"


def test_generate_model_import_ctes(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    """Test generate_model_import_ctes function."""
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
    generate_model_import_ctes_tool = fastmcp.tools["generate_model_import_ctes"]

    # Call the tool
    generate_model_import_ctes_tool(
        model_name="fct_orders",
        leading_commas=False,
    )

    # Verify the command
    assert mock_calls
    args_list = mock_calls[0]
    assert "generate_model_import_ctes" in args_list

    args_index = args_list.index("--args")
    args_json = json.loads(args_list[args_index + 1])
    assert args_json["model_name"] == "fct_orders"
    assert args_json["leading_commas"] is False


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
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
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
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
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
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
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
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
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
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
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


def test_create_base_models(monkeypatch: MonkeyPatch, mock_process, mock_fastmcp):
    """Test create_base_models function."""
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        # Return different outputs for different calls
        if len(mock_calls) == 1:
            return mock_process(output="SELECT * FROM {{ source('raw', 'customers') }}")
        else:
            return mock_process(output="SELECT * FROM {{ source('raw', 'orders') }}")

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
    create_base_models_tool = fastmcp.tools["create_base_models"]

    # Call the tool
    result = create_base_models_tool(
        source_name="raw",
        tables=["customers", "orders"],
        leading_commas=True,
        case_sensitive_cols=False,
        materialized=None,
    )

    # Should have called dbt twice (once for each table)
    assert len(mock_calls) == 2

    # Check both calls used generate_base_model macro
    for call_args in mock_calls:
        assert "generate_base_model" in call_args
        args_index = call_args.index("--args")
        args_json = json.loads(call_args[args_index + 1])
        assert args_json["source_name"] == "raw"
        assert args_json["leading_commas"] is True
        assert args_json["case_sensitive_cols"] is False

    # Check result contains both filenames
    assert "stg_raw__customers.sql" in result
    assert "stg_raw__orders.sql" in result


def test_create_base_models_error_propagation(monkeypatch: MonkeyPatch, mock_fastmcp):
    """Test create_base_models propagates errors from generate_base_model."""
    mock_calls = []

    class MockProcessWithError:
        def __init__(self):
            self.returncode = 1

        def communicate(self, timeout=None):
            return "Error: Source not found", None

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return MockProcessWithError()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
    create_base_models_tool = fastmcp.tools["create_base_models"]

    # Call should return error from first table
    result = create_base_models_tool(
        source_name="nonexistent",
        tables=["table1", "table2"],
        leading_commas=False,
        case_sensitive_cols=False,
        materialized=None,
    )

    # Should stop at first error
    assert len(mock_calls) == 1
    assert "Error running dbt-codegen macro" in result


def test_base_model_creation(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, tmp_path
):
    """Test base_model_creation function."""
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        # Return SQL content for the table
        return mock_process(output="SELECT * FROM {{ source('raw', 'customers') }}")

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    # Create a temporary config with the tmp_path as project_dir
    from tests.mocks.config import mock_dbt_codegen_config

    test_config = mock_dbt_codegen_config
    test_config.project_dir = str(tmp_path)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(fastmcp, test_config)
    base_model_creation_tool = fastmcp.tools["base_model_creation"]

    # Call the tool
    result = base_model_creation_tool(
        source_name="raw",
        tables=["customers"],
        leading_commas=False,
        case_sensitive_cols=False,
        materialized="view",
    )

    # Should have called dbt once
    assert len(mock_calls) == 1

    # Verify the command
    call_args = mock_calls[0]
    assert "generate_base_model" in call_args
    args_index = call_args.index("--args")
    args_json = json.loads(call_args[args_index + 1])
    assert args_json["source_name"] == "raw"
    assert args_json["table_name"] == "customers"
    assert args_json["materialized"] == "view"

    # Check that file was created
    models_dir = tmp_path / "models"
    expected_file = models_dir / "stg_raw__customers.sql"
    assert expected_file.exists()
    assert "SELECT * FROM {{ source('raw', 'customers') }}" in expected_file.read_text()

    # Check success message
    assert "Successfully created 1 base model files" in result
    assert str(expected_file) in result


def test_base_model_creation_multiple_files(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, tmp_path
):
    """Test base_model_creation with multiple tables."""
    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        # Return different SQL for each table
        call_count = len(mock_calls)
        if call_count == 1:
            return mock_process(output="SELECT * FROM {{ source('raw', 'customers') }}")
        else:
            return mock_process(output="SELECT * FROM {{ source('raw', 'orders') }}")

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    # Use tmp_path for project directory
    from tests.mocks.config import mock_dbt_codegen_config

    test_config = mock_dbt_codegen_config
    test_config.project_dir = str(tmp_path)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(fastmcp, test_config)
    base_model_creation_tool = fastmcp.tools["base_model_creation"]

    # Call the tool
    result = base_model_creation_tool(
        source_name="raw",
        tables=["customers", "orders"],
        leading_commas=False,
        case_sensitive_cols=False,
        materialized=None,
    )

    # Should have called dbt twice
    assert len(mock_calls) == 2

    # Check both files were created
    models_dir = tmp_path / "models"
    customers_file = models_dir / "stg_raw__customers.sql"
    orders_file = models_dir / "stg_raw__orders.sql"

    assert customers_file.exists()
    assert orders_file.exists()
    assert "customers" in customers_file.read_text()
    assert "orders" in orders_file.read_text()

    # Check success message
    assert "Successfully created 2 base model files" in result


def test_base_model_creation_error_handling(monkeypatch: MonkeyPatch, mock_fastmcp):
    """Test base_model_creation error handling."""
    mock_calls = []

    class MockProcessWithError:
        def __init__(self):
            self.returncode = 1

        def communicate(self, timeout=None):
            return "Error: Source not found", None

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return MockProcessWithError()

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
    base_model_creation_tool = fastmcp.tools["base_model_creation"]

    # Call should return error
    result = base_model_creation_tool(
        source_name="nonexistent",
        tables=["table1"],
        leading_commas=False,
        case_sensitive_cols=False,
        materialized="table",
    )

    assert "Error running dbt-codegen macro" in result


def test_base_model_creation_permission_error(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    """Test base_model_creation handles file permission errors."""

    def mock_popen(args, **kwargs):
        return mock_process(output="SELECT * FROM test")

    def mock_makedirs(*args, **kwargs):
        raise PermissionError("Permission denied")

    monkeypatch.setattr("subprocess.Popen", mock_popen)
    monkeypatch.setattr("os.makedirs", mock_makedirs)

    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
    base_model_creation_tool = fastmcp.tools["base_model_creation"]

    result = base_model_creation_tool(
        source_name="test",
        tables=["table1"],
        leading_commas=False,
        case_sensitive_cols=False,
        materialized="table",
    )

    assert "Error: Cannot access or create models directory" in result
    assert "Permission denied" in result


def test_all_tools_registered(mock_fastmcp):
    """Test that all expected tools are registered."""
    fastmcp, _ = mock_fastmcp
    register_dbt_codegen_tools(fastmcp, mock_dbt_codegen_config)
    tools = fastmcp.tools

    expected_tools = [
        "generate_source",
        "generate_model_yaml",
        "generate_base_model",
        "generate_model_import_ctes",
        "create_base_models",
        "base_model_creation",
    ]

    for tool_name in expected_tools:
        assert tool_name in tools, f"Tool {tool_name} not registered"
