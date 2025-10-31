from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from dbt_mcp.config.config import LspConfig
from dbt_mcp.lsp.lsp_binary_manager import LspBinaryInfo
from dbt_mcp.lsp.tools import (
    cleanup_lsp_connection,
    get_column_lineage,
    register_lsp_tools,
    rename_model,
)
from dbt_mcp.lsp.lsp_client import LSPClient
from dbt_mcp.mcp.server import FastMCP
from dbt_mcp.tools.tool_names import ToolName


@pytest.fixture
def test_mcp_server() -> FastMCP:
    """Create a mock FastMCP server."""

    server = FastMCP(
        name="test",
    )
    return server


@pytest.fixture
def lsp_config(tmp_path: Path) -> LspConfig:
    """Create a test LSP configuration."""
    return LspConfig(
        lsp_path="/usr/local/bin/dbt-lsp",
        project_dir=str(tmp_path),
    )


@pytest.mark.asyncio
async def test_register_lsp_tools_no_binary(
    test_mcp_server: FastMCP, lsp_config: LspConfig
) -> None:
    """Test that registration fails gracefully when no LSP binary is found."""
    with patch("dbt_mcp.lsp.tools.dbt_lsp_binary_info", return_value=None):
        await register_lsp_tools(test_mcp_server, lsp_config)
        assert not await test_mcp_server.list_tools()


@pytest.mark.asyncio
async def test_register_lsp_tools_success(
    test_mcp_server: FastMCP, lsp_config: LspConfig
) -> None:
    """Test successful registration of LSP tools."""

    lsp_connection_mock = AsyncMock()
    lsp_connection_mock.start = AsyncMock()
    lsp_connection_mock.initialize = AsyncMock()

    with (
        patch(
            "dbt_mcp.lsp.tools.dbt_lsp_binary_info",
            return_value=LspBinaryInfo(path="/path/to/lsp", version="1.0.0"),
        ),
        patch("dbt_mcp.lsp.tools.LSPConnection", return_value=lsp_connection_mock),
    ):
        await register_lsp_tools(test_mcp_server, lsp_config)

        # Verify correct tools were registered
        tool_names = [tool.name for tool in await test_mcp_server.list_tools()]
        assert ToolName.GET_COLUMN_LINEAGE.value in tool_names
        assert ToolName.RENAME_MODEL.value in tool_names


@pytest.mark.asyncio
async def test_cleanup_lsp_connection() -> None:
    """Test that cleanup_lsp_connection properly stops the LSP connection."""
    mock_connection = AsyncMock()
    mock_connection.stop = AsyncMock()

    with patch("dbt_mcp.lsp.tools._lsp_connection", mock_connection):
        await cleanup_lsp_connection()
        mock_connection.stop.assert_called_once()


@pytest.mark.asyncio
async def test_cleanup_lsp_connection_no_connection() -> None:
    """Test that cleanup_lsp_connection handles no connection gracefully."""
    with patch("dbt_mcp.lsp.tools._lsp_connection", None):
        # Should not raise any exceptions
        await cleanup_lsp_connection()


@pytest.mark.asyncio
async def test_cleanup_lsp_connection_error() -> None:
    """Test that cleanup_lsp_connection handles errors gracefully."""
    mock_connection = AsyncMock()
    mock_connection.stop = AsyncMock(side_effect=Exception("Stop failed"))

    with patch("dbt_mcp.lsp.tools._lsp_connection", mock_connection):
        # Should not raise the exception, but log it
        await cleanup_lsp_connection()
        mock_connection.stop.assert_called_once()


@pytest.mark.asyncio
async def test_register_lsp_tools_idempotent(
    test_mcp_server: FastMCP, lsp_config: LspConfig
) -> None:
    """Test that registering LSP tools twice doesn't create duplicate connections."""
    import dbt_mcp.lsp.tools as tools_module

    lsp_connection_mock = AsyncMock()
    lsp_connection_mock.start = AsyncMock()
    lsp_connection_mock.initialize = AsyncMock()

    # Reset the module-level connection
    tools_module._lsp_connection = None

    try:
        with (
            patch(
                "dbt_mcp.lsp.tools.dbt_lsp_binary_info",
                return_value=LspBinaryInfo(path="/path/to/lsp", version="1.0.0"),
            ),
            patch(
                "dbt_mcp.lsp.tools.LSPConnection", return_value=lsp_connection_mock
            ) as connection_constructor,
        ):
            # Register twice
            await register_lsp_tools(test_mcp_server, lsp_config)
            await register_lsp_tools(test_mcp_server, lsp_config)

            # Connection should only be created once (idempotent)
            assert connection_constructor.call_count == 1
    finally:
        # Clean up module state
        tools_module._lsp_connection = None


@pytest.mark.asyncio
async def test_get_column_lineage_success() -> None:
    """Test successful column lineage retrieval."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.get_column_lineage = AsyncMock(
        return_value={"nodes": [{"id": "model.project.table", "column": "id"}]}
    )

    result = await get_column_lineage(mock_lsp_client, "model.project.table", "id")

    assert "nodes" in result
    assert len(result["nodes"]) == 1
    assert result["nodes"][0]["id"] == "model.project.table"
    mock_lsp_client.get_column_lineage.assert_called_once_with(
        model_id="model.project.table", column_name="id"
    )


@pytest.mark.asyncio
async def test_get_column_lineage_lsp_error() -> None:
    """Test column lineage with LSP error response."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.get_column_lineage = AsyncMock(
        return_value={"error": "Model not found"}
    )

    result = await get_column_lineage(mock_lsp_client, "invalid_model", "column")

    assert "error" in result
    assert "LSP error: Model not found" in result["error"]


@pytest.mark.asyncio
async def test_get_column_lineage_no_results() -> None:
    """Test column lineage when no lineage is found."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.get_column_lineage = AsyncMock(return_value={"nodes": []})

    result = await get_column_lineage(mock_lsp_client, "model.project.table", "column")

    assert "error" in result
    assert "No column lineage found" in result["error"]


@pytest.mark.asyncio
async def test_get_column_lineage_timeout() -> None:
    """Test column lineage with timeout error."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.get_column_lineage = AsyncMock(side_effect=TimeoutError())

    result = await get_column_lineage(mock_lsp_client, "model.project.table", "column")

    assert "error" in result
    assert "Timeout waiting for column lineage" in result["error"]


@pytest.mark.asyncio
async def test_get_column_lineage_generic_exception() -> None:
    """Test column lineage with generic exception."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.get_column_lineage = AsyncMock(
        side_effect=Exception("Connection lost")
    )

    result = await get_column_lineage(mock_lsp_client, "model.project.table", "column")

    assert "error" in result
    assert "Failed to get column lineage" in result["error"]
    assert "Connection lost" in result["error"]


@pytest.mark.asyncio
async def test_rename_model_success() -> None:
    """Test successful model rename."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.rename_model = AsyncMock(
        return_value={
            "renamed": True,
            "old_path": "/path/to/old_model.sql",
            "new_path": "/path/to/new_model.sql",
            "files_updated": ["/path/to/ref_file.sql"],
        }
    )

    old_uri = "file:///path/to/old_model.sql"
    new_uri = "file:///path/to/new_model.sql"
    result = await rename_model(mock_lsp_client, old_uri, new_uri)

    assert result["renamed"] is True
    assert result["old_path"] == "/path/to/old_model.sql"
    assert result["new_path"] == "/path/to/new_model.sql"
    assert len(result["files_updated"]) == 1
    mock_lsp_client.rename_model.assert_called_once_with(
        old_uri=old_uri, new_uri=new_uri, apply_edits=True
    )


@pytest.mark.asyncio
async def test_rename_model_with_multiple_updates() -> None:
    """Test model rename that updates multiple files."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.rename_model = AsyncMock(
        return_value={
            "renamed": True,
            "old_path": "/path/to/old_model.sql",
            "new_path": "/path/to/new_model.sql",
            "files_updated": [
                "/path/to/ref_file1.sql",
                "/path/to/ref_file2.sql",
                "/path/to/ref_file3.sql",
            ],
        }
    )

    old_uri = "file:///path/to/old_model.sql"
    new_uri = "file:///path/to/new_model.sql"
    result = await rename_model(mock_lsp_client, old_uri, new_uri)

    assert result["renamed"] is True
    assert len(result["files_updated"]) == 3


@pytest.mark.asyncio
async def test_rename_model_error() -> None:
    """Test model rename with error response."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.rename_model = AsyncMock(return_value={"error": "Model not found"})

    result = await rename_model(
        mock_lsp_client,
        "file:///invalid/old_model.sql",
        "file:///invalid/new_model.sql",
    )

    assert "error" in result
    assert "Model not found" in result["error"]


@pytest.mark.asyncio
async def test_rename_model_timeout() -> None:
    """Test model rename with timeout error."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.rename_model = AsyncMock(side_effect=TimeoutError())

    result = await rename_model(
        mock_lsp_client,
        "file:///path/to/old_model.sql",
        "file:///path/to/new_model.sql",
    )

    assert "error" in result
    assert "Timeout waiting for model rename" in result["error"]


@pytest.mark.asyncio
async def test_rename_model_generic_exception() -> None:
    """Test model rename with generic exception."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.rename_model = AsyncMock(side_effect=Exception("Connection lost"))

    result = await rename_model(
        mock_lsp_client,
        "file:///path/to/old_model.sql",
        "file:///path/to/new_model.sql",
    )

    assert "error" in result
    assert "Failed to rename model" in result["error"]
    assert "Connection lost" in result["error"]
