"""Tests for custom tools registration."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest
from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import CustomToolsConfig
from dbt_mcp.custom_tools.filesystem import FileSystemProvider
from dbt_mcp.custom_tools.tools import register_custom_tools
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.tools.tool_names import ToolName


class MockFileSystemProvider(FileSystemProvider):
    """Mock filesystem provider for testing."""

    def __init__(self, files: dict[str, str] | None = None):
        self.files = files or {}

    def exists(self, path: str) -> bool:
        return path in self.files

    def read_text(self, path: str) -> str:
        if path not in self.files:
            raise FileNotFoundError(f"File not found: {path}")
        return self.files[path]

    def join_path(self, base: str, *parts: str) -> str:
        result = base.rstrip("/")
        for part in parts:
            result = f"{result}/{part.lstrip('/')}"
        return result


class TestRegisterCustomTools:
    """Tests for register_custom_tools function."""

    def test_register_custom_tools_with_no_models(self, tmp_path):
        """Test register_custom_tools when no custom tool models are found."""
        # Setup
        dbt_mcp = FastMCP("test-server")
        config = CustomToolsConfig(
            project_dir=str(tmp_path),
            dbt_path="dbt",
            dbt_cli_timeout=300,
            binary_type=BinaryType.DBT_CORE,
        )
        fs_provider = MockFileSystemProvider({})
        exclude_tools = []

        # Mock subprocess to return empty dbt ls result
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.communicate.return_value = ("[]", None)
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            # Execute
            register_custom_tools(dbt_mcp, config, exclude_tools, fs_provider)

        # Verify no tools were registered (check the tools dict)
        assert len(dbt_mcp._tool_manager._tools) == 0

    def test_register_custom_tools_with_single_model(self, tmp_path):
        """Test register_custom_tools discovers and registers a custom tool."""
        # Setup
        dbt_mcp = FastMCP("test-server")
        project_dir = str(tmp_path)
        model_path = f"{project_dir}/models/tools/customer_lookup.sql"

        config = CustomToolsConfig(
            project_dir=project_dir,
            dbt_path="dbt",
            dbt_cli_timeout=300,
            binary_type=BinaryType.DBT_CORE,
        )

        # Create a mock filesystem with a custom tool model
        fs_provider = MockFileSystemProvider(
            {
                model_path: """
                {#
                    This tool looks up customer information by ID.
                    @var customer_id: The customer ID to look up
                #}
                SELECT * FROM {{ ref('customers') }}
                WHERE customer_id = {{ var('customer_id') }}
                """
            }
        )

        exclude_tools = []

        # Mock dbt ls command to return our model
        dbt_ls_output = """[
            {
                "name": "customer_lookup",
                "resource_type": "model",
                "original_file_path": "models/tools/customer_lookup.sql",
                "description": "Customer lookup tool"
            }
        ]"""

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.communicate.return_value = (dbt_ls_output, None)
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            # Execute
            register_custom_tools(dbt_mcp, config, exclude_tools, fs_provider)

        # Verify one tool was registered
        assert len(dbt_mcp._tool_manager._tools) == 1
        assert "customer_lookup" in dbt_mcp._tool_manager._tools

    def test_register_custom_tools_with_multiple_models(self, tmp_path):
        """Test register_custom_tools with multiple custom tool models."""
        # Setup
        dbt_mcp = FastMCP("test-server")
        project_dir = str(tmp_path)

        config = CustomToolsConfig(
            project_dir=project_dir,
            dbt_path="dbt",
            dbt_cli_timeout=300,
            binary_type=BinaryType.DBT_CORE,
        )

        # Create mock filesystem with multiple models
        fs_provider = MockFileSystemProvider(
            {
                f"{project_dir}/models/tools/customer_lookup.sql": """
                {# @var customer_id: Customer ID #}
                SELECT * FROM customers WHERE id = {{ var('customer_id') }}
                """,
                f"{project_dir}/models/tools/order_report.sql": """
                {# @var start_date: Start date #}
                {# @var end_date: End date #}
                SELECT * FROM orders
                WHERE date BETWEEN {{ var('start_date') }} AND {{ var('end_date') }}
                """,
                f"{project_dir}/models/tools/inventory_check.sql": """
                {# @var product_id: Product ID #}
                SELECT * FROM inventory WHERE product_id = {{ var('product_id') }}
                """,
            }
        )

        exclude_tools = []

        # Mock dbt ls to return multiple models
        dbt_ls_output = """[
            {
                "name": "customer_lookup",
                "resource_type": "model",
                "original_file_path": "models/tools/customer_lookup.sql",
                "description": ""
            },
            {
                "name": "order_report",
                "resource_type": "model",
                "original_file_path": "models/tools/order_report.sql",
                "description": ""
            },
            {
                "name": "inventory_check",
                "resource_type": "model",
                "original_file_path": "models/tools/inventory_check.sql",
                "description": ""
            }
        ]"""

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.communicate.return_value = (dbt_ls_output, None)
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            # Execute
            register_custom_tools(dbt_mcp, config, exclude_tools, fs_provider)

        # Verify all tools were registered
        assert len(dbt_mcp._tool_manager._tools) == 3
        assert "customer_lookup" in dbt_mcp._tool_manager._tools
        assert "order_report" in dbt_mcp._tool_manager._tools
        assert "inventory_check" in dbt_mcp._tool_manager._tools

    def test_register_custom_tools_with_excluded_tools(self, tmp_path):
        """Test that excluded tools are properly filtered out."""
        # Setup
        dbt_mcp = FastMCP("test-server")
        project_dir = str(tmp_path)

        config = CustomToolsConfig(
            project_dir=project_dir,
            dbt_path="dbt",
            dbt_cli_timeout=300,
            binary_type=BinaryType.DBT_CORE,
        )

        # Create a custom tool with the same name as a built-in tool
        fs_provider = MockFileSystemProvider(
            {
                f"{project_dir}/models/tools/run.sql": """
                SELECT * FROM customers
                """
            }
        )

        # Exclude the 'run' tool
        exclude_tools = [ToolName.RUN]

        dbt_ls_output = """[
            {
                "name": "run",
                "resource_type": "model",
                "original_file_path": "models/tools/run.sql",
                "description": ""
            }
        ]"""

        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.communicate.return_value = (dbt_ls_output, None)
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            # Execute
            register_custom_tools(dbt_mcp, config, exclude_tools, fs_provider)

        # Verify the tool was not registered due to exclusion
        assert len(dbt_mcp._tool_manager._tools) == 0

    def test_register_custom_tools_without_fs_provider(self, tmp_path):
        """Test register_custom_tools uses default filesystem when fs_provider is None."""
        # Setup
        dbt_mcp = FastMCP("test-server")
        config = CustomToolsConfig(
            project_dir=str(tmp_path),
            dbt_path="dbt",
            dbt_cli_timeout=300,
            binary_type=BinaryType.DBT_CORE,
        )
        exclude_tools = []

        # Mock dbt ls to return empty result
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.communicate.return_value = ("[]", None)
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            # Execute without fs_provider (should use default)
            register_custom_tools(dbt_mcp, config, exclude_tools)

        # Should complete without error
        assert len(dbt_mcp._tool_manager._tools) == 0

    def test_register_custom_tools_handles_subprocess_timeout(self, tmp_path):
        """Test register_custom_tools handles subprocess timeout gracefully."""
        # Setup
        dbt_mcp = FastMCP("test-server")
        config = CustomToolsConfig(
            project_dir=str(tmp_path),
            dbt_path="dbt",
            dbt_cli_timeout=1,  # Short timeout
            binary_type=BinaryType.DBT_CORE,
        )
        fs_provider = MockFileSystemProvider({})
        exclude_tools = []

        # Mock subprocess to raise TimeoutExpired
        with patch("subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.communicate.side_effect = subprocess.TimeoutExpired(
                cmd="dbt", timeout=1
            )
            mock_popen.return_value = mock_process

            # Execute - should handle timeout gracefully
            with pytest.raises(subprocess.TimeoutExpired):
                register_custom_tools(dbt_mcp, config, exclude_tools, fs_provider)
