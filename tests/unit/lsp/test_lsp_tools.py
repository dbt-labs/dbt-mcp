from unittest.mock import AsyncMock, MagicMock

import pytest

from dbt_mcp.lsp.providers.lsp_client_provider import LSPClientProvider
from dbt_mcp.lsp.tools import (
    get_column_lineage,
    register_lsp_tools,
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
def lsp_client() -> LSPClient:
    """Create a test LSP client."""
    return MagicMock(spec=LSPClient)


class MockLSPClientProvider(LSPClientProvider):
    """Mock implementation of LSPClientProvider for testing."""

    def __init__(self, lsp_client: LSPClient):
        self.lsp_client = lsp_client

    async def get_client(self) -> LSPClient:
        return self.lsp_client


@pytest.fixture
def lsp_client_provider(lsp_client: LSPClient) -> LSPClientProvider:
    """Create a test LSP client provider."""
    return MockLSPClientProvider(lsp_client)


@pytest.mark.asyncio
async def test_register_lsp_tools_success(
    test_mcp_server: FastMCP, lsp_client_provider: LSPClientProvider
) -> None:
    """Test successful registration of LSP tools."""

    await register_lsp_tools(test_mcp_server, lsp_client_provider)

    # Verify correct tools were registered
    tool_names = [tool.name for tool in await test_mcp_server.list_tools()]
    assert ToolName.GET_COLUMN_LINEAGE.value in tool_names


@pytest.mark.asyncio
async def test_get_column_lineage_success() -> None:
    """Test successful column lineage retrieval."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.get_column_lineage = AsyncMock(
        return_value={"nodes": [{"id": "model.project.table", "column": "id"}]}
    )
    mock_provider = MockLSPClientProvider(mock_lsp_client)

    result = await get_column_lineage(mock_provider, "model.project.table", "id")

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
    mock_provider = MockLSPClientProvider(mock_lsp_client)

    result = await get_column_lineage(mock_provider, "invalid_model", "column")

    assert "error" in result
    assert "LSP error: Model not found" in result["error"]


@pytest.mark.asyncio
async def test_get_column_lineage_no_results() -> None:
    """Test column lineage when no lineage is found."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.get_column_lineage = AsyncMock(return_value={"nodes": []})
    mock_provider = MockLSPClientProvider(mock_lsp_client)

    result = await get_column_lineage(mock_provider, "model.project.table", "column")

    assert "error" in result
    assert "No column lineage found" in result["error"]


@pytest.mark.asyncio
async def test_get_column_lineage_timeout() -> None:
    """Test column lineage with timeout error."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.get_column_lineage = AsyncMock(side_effect=TimeoutError())
    mock_provider = MockLSPClientProvider(mock_lsp_client)

    result = await get_column_lineage(mock_provider, "model.project.table", "column")

    assert "error" in result
    assert "Timeout waiting for column lineage" in result["error"]


@pytest.mark.asyncio
async def test_get_column_lineage_generic_exception() -> None:
    """Test column lineage with generic exception."""
    mock_lsp_client = AsyncMock(spec=LSPClient)
    mock_lsp_client.get_column_lineage = AsyncMock(
        side_effect=Exception("Connection lost")
    )
    mock_provider = MockLSPClientProvider(mock_lsp_client)

    result = await get_column_lineage(mock_provider, "model.project.table", "column")

    assert "error" in result
    assert "Failed to get column lineage" in result["error"]
    assert "Connection lost" in result["error"]
