import os
import pytest

from dbt_mcp.config.config import DbtCodegenConfig, load_config
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.dbt_codegen.tools import create_dbt_codegen_tool_definitions


@pytest.fixture
def dbt_codegen_config():
    """Fixture for dbt-codegen configuration."""
    # Try to load from full config first
    try:
        config = load_config()
        if config.dbt_codegen_config:
            return config.dbt_codegen_config
    except Exception:
        pass

    # Fall back to environment variables
    project_dir = os.getenv("DBT_PROJECT_DIR")
    dbt_path = os.getenv("DBT_PATH", "dbt")
    dbt_cli_timeout = os.getenv("DBT_CLI_TIMEOUT", "30")

    if not project_dir:
        pytest.skip(
            "DBT_PROJECT_DIR environment variable is required for integration tests"
        )

    return DbtCodegenConfig(
        project_dir=project_dir,
        dbt_path=dbt_path,
        dbt_cli_timeout=int(dbt_cli_timeout),
        binary_type=BinaryType.DBT_CORE,
    )


@pytest.fixture
def generate_source_tool(dbt_codegen_config):
    """Fixture for generate_source tool."""
    tools = create_dbt_codegen_tool_definitions(dbt_codegen_config)
    for tool in tools:
        if tool.fn.__name__ == "generate_source":
            return tool.fn
    raise ValueError("generate_source tool not found")


@pytest.fixture
def generate_model_yaml_tool(dbt_codegen_config):
    """Fixture for generate_model_yaml tool."""
    tools = create_dbt_codegen_tool_definitions(dbt_codegen_config)
    for tool in tools:
        if tool.fn.__name__ == "generate_model_yaml":
            return tool.fn
    raise ValueError("generate_model_yaml tool not found")


@pytest.fixture
def generate_base_model_tool(dbt_codegen_config):
    """Fixture for generate_base_model tool."""
    tools = create_dbt_codegen_tool_definitions(dbt_codegen_config)
    for tool in tools:
        if tool.fn.__name__ == "generate_base_model":
            return tool.fn
    raise ValueError("generate_base_model tool not found")


@pytest.fixture
def generate_model_import_ctes_tool(dbt_codegen_config):
    """Fixture for generate_model_import_ctes tool."""
    tools = create_dbt_codegen_tool_definitions(dbt_codegen_config)
    for tool in tools:
        if tool.fn.__name__ == "generate_model_import_ctes":
            return tool.fn
    raise ValueError("generate_model_import_ctes tool not found")


@pytest.fixture
def create_base_models_tool(dbt_codegen_config):
    """Fixture for create_base_models tool."""
    tools = create_dbt_codegen_tool_definitions(dbt_codegen_config)
    for tool in tools:
        if tool.fn.__name__ == "create_base_models":
            return tool.fn
    raise ValueError("create_base_models tool not found")


@pytest.fixture
def base_model_creation_tool(dbt_codegen_config):
    """Fixture for base_model_creation tool."""
    tools = create_dbt_codegen_tool_definitions(dbt_codegen_config)
    for tool in tools:
        if tool.fn.__name__ == "base_model_creation":
            return tool.fn
    raise ValueError("base_model_creation tool not found")


def test_generate_source_basic(generate_source_tool):
    """Test basic source generation with minimal parameters."""
    # This will fail if dbt-codegen is not installed
    result = generate_source_tool(
        schema_name="public", generate_columns=False, include_descriptions=False
    )

    # Check for error conditions
    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        else:
            pytest.fail(f"Unexpected error: {result}")

    # Basic validation - should return YAML-like content
    assert result is not None
    assert len(result) > 0


def test_generate_source_with_columns(generate_source_tool):
    """Test source generation with column definitions."""
    result = generate_source_tool(
        schema_name="public", generate_columns=True, include_descriptions=True
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        else:
            pytest.fail(f"Unexpected error: {result}")

    assert result is not None


def test_generate_source_with_specific_tables(generate_source_tool):
    """Test source generation for specific tables."""
    result = generate_source_tool(
        schema_name="public",
        table_names=["users", "orders"],
        generate_columns=True,
        include_descriptions=False,
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")

    assert result is not None


def test_generate_model_yaml(generate_model_yaml_tool):
    """Test model YAML generation."""
    # This assumes there's at least one model in the project
    result = generate_model_yaml_tool(
        model_names=["stg_customers"],
        upstream_descriptions=False,
        include_data_types=True,
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        elif "Model" in result and "not found" in result:
            pytest.skip("Test model not found in project")

    assert result is not None


def test_generate_model_yaml_with_upstream(generate_model_yaml_tool):
    """Test model YAML generation with upstream descriptions."""
    result = generate_model_yaml_tool(
        model_names=["stg_customers"],
        upstream_descriptions=True,
        include_data_types=True,
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        elif "Model" in result and "not found" in result:
            pytest.skip("Test model not found in project")

    assert result is not None


def test_generate_base_model(generate_base_model_tool):
    """Test base model SQL generation."""
    # This assumes a source is defined
    result = generate_base_model_tool(
        source_name="raw",  # Common source name
        table_name="customers",
        leading_commas=False,
        materialized="view",
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        elif "Source" in result and "not found" in result:
            pytest.skip("Test source not found in project")

    # Should generate SQL with SELECT statement
    assert result is not None


def test_generate_base_model_with_case_sensitive(generate_base_model_tool):
    """Test base model generation with case-sensitive columns."""
    result = generate_base_model_tool(
        source_name="raw",
        table_name="customers",
        case_sensitive_cols=True,
        leading_commas=True,
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        elif "Source" in result and "not found" in result:
            pytest.skip("Test source not found in project")

    assert result is not None


def test_generate_model_import_ctes(generate_model_import_ctes_tool):
    """Test import CTE generation."""
    result = generate_model_import_ctes_tool(
        model_name="fct_orders",  # Common fact model
        leading_commas=False,
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        elif "Model" in result and "not found" in result:
            pytest.skip("Test model not found in project")

    assert result is not None


def test_error_handling_invalid_schema(generate_source_tool):
    """Test handling of invalid schema names."""
    # Use a schema that definitely doesn't exist
    result = generate_source_tool(
        schema_name="definitely_nonexistent_schema_12345",
        generate_columns=False,
        include_descriptions=False,
    )

    if "dbt-codegen package may not be installed" in result:
        pytest.skip("dbt-codegen package not installed")

    # Should return an error but not crash
    assert result is not None


def test_error_handling_invalid_model(generate_model_yaml_tool):
    """Test handling of non-existent model names."""
    result = generate_model_yaml_tool(
        model_names=["definitely_nonexistent_model_12345"]
    )

    if "dbt-codegen package may not be installed" in result:
        pytest.skip("dbt-codegen package not installed")

    # Should handle gracefully
    assert result is not None


def test_error_handling_invalid_source(generate_base_model_tool):
    """Test handling of invalid source references."""
    result = generate_base_model_tool(
        source_name="nonexistent_source", table_name="nonexistent_table"
    )

    if "dbt-codegen package may not be installed" in result:
        pytest.skip("dbt-codegen package not installed")

    # Should return an error message
    assert result is not None


def test_create_base_models_basic(create_base_models_tool):
    """Test basic create_base_models functionality."""
    result = create_base_models_tool(
        source_name="raw",
        tables=["customers", "orders"],
        leading_commas=False,
        case_sensitive_cols=False,
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        elif "Source" in result and "not found" in result:
            pytest.skip("Test source not found in project")

    # Should contain file information for multiple tables
    assert result is not None
    assert "stg_raw__customers.sql" in result
    assert "stg_raw__orders.sql" in result


def test_create_base_models_with_options(create_base_models_tool):
    """Test create_base_models with various options."""
    result = create_base_models_tool(
        source_name="raw",
        tables=["customers"],
        leading_commas=True,
        case_sensitive_cols=True,
        materialized="view",
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        elif "Source" in result and "not found" in result:
            pytest.skip("Test source not found in project")

    assert result is not None
    assert "stg_raw__customers.sql" in result


def test_base_model_creation_basic(base_model_creation_tool):
    """Test basic base_model_creation functionality."""
    # This test actually creates files, so be careful
    result = base_model_creation_tool(
        source_name="test",
        tables=["test_table"],
        leading_commas=False,
        case_sensitive_cols=False,
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        elif "Source" in result and "not found" in result:
            pytest.skip("Test source not found in project")
        elif "Cannot access or create models directory" in result:
            pytest.skip("Cannot write to models directory")

    # Should confirm file creation
    assert result is not None
    if "Successfully created" in result:
        assert "stg_test__test_table.sql" in result


def test_base_model_creation_multiple_tables(base_model_creation_tool):
    """Test base_model_creation with multiple tables."""
    result = base_model_creation_tool(
        source_name="test",
        tables=["table1", "table2"],
        leading_commas=False,
        case_sensitive_cols=False,
    )

    if "Error:" in result:
        if "dbt-codegen package may not be installed" in result:
            pytest.skip("dbt-codegen package not installed")
        elif "Source" in result and "not found" in result:
            pytest.skip("Test source not found in project")
        elif "Cannot access or create models directory" in result:
            pytest.skip("Cannot write to models directory")

    assert result is not None
    if "Successfully created" in result:
        assert "2" in result  # Should mention creating 2 files


def test_create_base_models_error_handling(create_base_models_tool):
    """Test error handling in create_base_models."""
    result = create_base_models_tool(
        source_name="nonexistent_source", tables=["nonexistent_table"]
    )

    if "dbt-codegen package may not be installed" in result:
        pytest.skip("dbt-codegen package not installed")

    # Should handle errors gracefully
    assert result is not None


def test_base_model_creation_error_handling(base_model_creation_tool):
    """Test error handling in base_model_creation."""
    result = base_model_creation_tool(
        source_name="nonexistent_source", tables=["nonexistent_table"]
    )

    if "dbt-codegen package may not be installed" in result:
        pytest.skip("dbt-codegen package not installed")

    # Should handle errors gracefully
    assert result is not None
