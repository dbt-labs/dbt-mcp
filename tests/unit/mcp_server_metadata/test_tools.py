from unittest.mock import patch

from dbt_mcp.mcp_server_metadata.tools import (
    _get_server_version,
    get_mcp_server_version,
)


def test_get_server_version_returns_version() -> None:
    """Test that _get_server_version returns the package version."""
    with patch("dbt_mcp.mcp_server_metadata.tools.version") as mock_version:
        mock_version.return_value = "1.2.3"
        result = _get_server_version()
        assert result == "1.2.3"
        mock_version.assert_called_once_with("dbt-mcp")


def test_get_server_version_returns_unknown_on_error() -> None:
    """Test that _get_server_version returns 'unknown' when version lookup fails."""
    with patch("dbt_mcp.mcp_server_metadata.tools.version") as mock_version:
        mock_version.side_effect = Exception("Package not found")
        result = _get_server_version()
        assert result == "unknown"


def test_get_mcp_server_version_tool_returns_version() -> None:
    """Test that get_mcp_server_version tool returns the package version."""
    with patch("dbt_mcp.mcp_server_metadata.tools.version") as mock_version:
        mock_version.return_value = "2.0.0"
        # The tool is a ToolDefinition, so we call its fn attribute
        result = get_mcp_server_version.fn()
        assert result == "2.0.0"
