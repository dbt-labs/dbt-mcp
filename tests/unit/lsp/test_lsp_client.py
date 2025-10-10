"""Tests for the DbtLspClient class."""

from unittest.mock import MagicMock

import pytest

from dbt_mcp.lsp.lsp_client import LSPClient
from dbt_mcp.lsp.lsp_connection import LSPConnection, LspConnectionState


@pytest.fixture
def mock_lsp_connection() -> LSPConnection:
    """Create a mock LSP connection manager."""
    connection = MagicMock(spec=LSPConnection)
    connection.state = LspConnectionState(initialized=True, compiled=True)
    return connection


@pytest.fixture
def lsp_client(mock_lsp_connection: LSPConnection):
    """Create an LSP client with a mock connection manager."""
    return LSPClient(mock_lsp_connection)


@pytest.mark.asyncio
async def test_get_column_lineage_success(lsp_client, mock_lsp_connection):
    """Test successful column lineage request."""
    # Setup mock
    mock_result = {
        "nodes": [
            {"model": "upstream_model", "column": "id"},
            {"model": "current_model", "column": "customer_id"},
        ]
    }

    mock_lsp_connection.send_request.return_value = mock_result

    # Execute
    result = await lsp_client.get_column_lineage(
        model_id="model.my_project.my_model",
        column_name="customer_id",
    )

    # Verify
    assert result == mock_result
    mock_lsp_connection.send_request.assert_called_once_with(
        "workspace/executeCommand",
        {
            "command": "dbt.listNodes",
            "arguments": ["+column:model.my_project.my_model.CUSTOMER_ID+"],
        },
    )


@pytest.mark.asyncio
async def test_list_nodes_success(lsp_client, mock_lsp_connection):
    """Test successful list nodes request."""
    # Setup mock
    mock_result = {
        "nodes": ["model.my_project.upstream1", "model.my_project.upstream2"],
    }

    mock_lsp_connection.send_request.return_value = mock_result

    # Execute
    result = await lsp_client._list_nodes(
        model_selector="+model.my_project.my_model+",
    )

    # Verify
    assert result == mock_result
    mock_lsp_connection.send_request.assert_called_once_with(
        "workspace/executeCommand",
        {"command": "dbt.listNodes", "arguments": ["+model.my_project.my_model+"]},
    )


@pytest.mark.asyncio
async def test_get_column_lineage_error(lsp_client, mock_lsp_connection):
    """Test column lineage request with LSP error."""
    # Setup mock to raise an error
    mock_lsp_connection.send_request.return_value = {"error": "LSP server error"}

    # Execute and verify exception is raised
    result = await lsp_client.get_column_lineage(
        model_id="model.my_project.my_model",
        column_name="customer_id",
    )

    assert result == {"error": "LSP server error"}
