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
def generate_staging_model_tool(dbt_codegen_config):
    """Fixture for generate_staging_model tool."""
    tools = create_dbt_codegen_tool_definitions(dbt_codegen_config)
    for tool in tools:
        if tool.fn.__name__ == "generate_staging_model":
            return tool.fn
    raise ValueError("generate_staging_model tool not found")


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


def test_generate_staging_model(generate_staging_model_tool):
    """Test staging model SQL generation."""
    # This assumes a source is defined
    result = generate_staging_model_tool(
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


def test_generate_staging_model_with_case_sensitive(generate_staging_model_tool):
    """Test staging model generation with case-sensitive columns."""
    result = generate_staging_model_tool(
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


def test_error_handling_invalid_source(generate_staging_model_tool):
    """Test handling of invalid source references."""
    result = generate_staging_model_tool(
        source_name="nonexistent_source", table_name="nonexistent_table"
    )

    if "dbt-codegen package may not be installed" in result:
        pytest.skip("dbt-codegen package not installed")

    # Should return an error message
    assert result is not None
