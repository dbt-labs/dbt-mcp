import unittest
from unittest.mock import MagicMock, patch

from tests.conftest import MockFastMCP
from tests.mocks.config import mock_config


class TestDbtCliIntegration(unittest.TestCase):
    @patch("subprocess.Popen")
    def test_dbt_command_execution(self, mock_popen):
        """
        Tests the full execution path for dbt commands, ensuring they are properly
        executed with the right arguments.
        """
        # Import here to prevent circular import issues during patching
        from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools

        # Mock setup
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("command output", None)
        mock_popen.return_value = mock_process

        mock_fastmcp = MockFastMCP()

        # Register the tools
        register_dbt_cli_tools(mock_fastmcp, mock_config.dbt_cli_config)

        # Test cases for different command types
        test_cases = [
            # Command name, args, expected command list
            ("build", [], ["/path/to/dbt", "--no-use-colors", "build", "--quiet"]),
            (
                "compile",
                [],
                ["/path/to/dbt", "--no-use-colors", "compile", "--quiet"],
            ),
            (
                "docs",
                [],
                ["/path/to/dbt", "--no-use-colors", "docs", "--quiet", "generate"],
            ),
            (
                "ls",
                [],
                ["/path/to/dbt", "--no-use-colors", "list", "--quiet"],
            ),
            ("parse", [], ["/path/to/dbt", "--no-use-colors", "parse", "--quiet"]),
            ("run", [], ["/path/to/dbt", "--no-use-colors", "run", "--quiet"]),
            ("test", [], ["/path/to/dbt", "--no-use-colors", "test", "--quiet"]),
            (
                "show",
                ["SELECT * FROM model"],
                [
                    "/path/to/dbt",
                    "--no-use-colors",
                    "show",
                    "--inline",
                    "SELECT * FROM model",
                    "--favor-state",
                    "--output",
                    "json",
                ],
            ),
            (
                "show",
                ["SELECT * FROM model", 10],
                [
                    "/path/to/dbt",
                    "--no-use-colors",
                    "show",
                    "--inline",
                    "SELECT * FROM model",
                    "--favor-state",
                    "--limit",
                    "10",
                    "--output",
                    "json",
                ],
            ),
        ]

        # Run each test case
        for command_name, args, expected_args in test_cases:
            mock_popen.reset_mock()

            # Call the function
            result = mock_fastmcp.tools[command_name](*args)

            # Verify the command was called correctly
            mock_popen.assert_called_once()
            actual_args = mock_popen.call_args.kwargs.get("args")

            num_params = 4

            self.assertEqual(actual_args[:num_params], expected_args[:num_params])

            # Verify correct working directory
            self.assertEqual(mock_popen.call_args.kwargs.get("cwd"), "/test/project")

            # Verify the output is returned correctly
            self.assertEqual(result, "command output")


if __name__ == "__main__":
    unittest.main()
