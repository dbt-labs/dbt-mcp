import unittest
from unittest.mock import MagicMock, patch

from dbt_mcp.meta.tools import register_meta_tools


class TestMetaTools(unittest.TestCase):
    def test_get_dbt_mcp_server_version(self):
        # Create a mock FastMCP
        mock_fastmcp = MagicMock()

        # Capture the registered tools
        tools = {}

        # Patch the tool decorator to capture functions
        def mock_tool_decorator(**kwargs):
            def decorator(func):
                tools[func.__name__] = func
                return func

            return decorator

        mock_fastmcp.tool = mock_tool_decorator

        # Register the tools
        register_meta_tools(mock_fastmcp)

        # Test the get_dbt_mcp_server_version function
        with patch("dbt_mcp.meta.tools.version") as mock_version:
            mock_version.return_value = "1.0.0"
            result = tools["get_dbt_mcp_server_version"]()

            # Verify the version function was called with correct package name
            mock_version.assert_called_once_with("dbt-mcp")

            # Verify the result
            self.assertEqual(result, "1.0.0")


if __name__ == "__main__":
    unittest.main()
