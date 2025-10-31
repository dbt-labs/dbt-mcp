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


@pytest.mark.asyncio
async def test_rename_model_success_with_apply_edits(
    lsp_client, mock_lsp_connection, tmp_path
):
    """Test successful model rename with edits applied."""
    # Create temporary files
    old_file = tmp_path / "old_file.sql"
    new_file = tmp_path / "new_file.sql"
    ref_file = tmp_path / "ref_file.sql"

    old_file.write_text("SELECT * FROM table")
    ref_file.write_text("SELECT * FROM {{ ref('old_file') }}")

    # Setup mock LSP response
    mock_result = {
        "changes": {
            f"file://{ref_file}": [
                {
                    "range": {
                        "start": {"line": 0, "character": 22},
                        "end": {"line": 0, "character": 30},
                    },
                    "newText": "new_file",
                }
            ]
        }
    }
    mock_lsp_connection.send_request.return_value = mock_result

    # Execute
    old_uri = f"file://{old_file}"
    new_uri = f"file://{new_file}"
    result = await lsp_client.rename_model(old_uri=old_uri, new_uri=new_uri)

    # Verify
    assert result["renamed"] is True
    assert result["old_path"] == str(old_file)
    assert result["new_path"] == str(new_file)
    assert str(ref_file) in result["files_updated"]

    # Verify file was renamed
    assert not old_file.exists()
    assert new_file.exists()

    # Verify reference was updated
    assert ref_file.read_text() == "SELECT * FROM {{ ref('new_file') }}"

    # Verify didRenameFiles notification was sent
    mock_lsp_connection.send_notification.assert_called_once()


@pytest.mark.asyncio
async def test_rename_model_without_apply_edits(lsp_client, mock_lsp_connection):
    """Test model rename request without applying edits (dry-run)."""
    # Setup mock
    mock_result = {
        "changes": {
            "file:///path/to/other_file.sql": [
                {
                    "range": {
                        "start": {"line": 0, "character": 0},
                        "end": {"line": 0, "character": 8},
                    },
                    "newText": "new_file",
                }
            ]
        }
    }
    mock_lsp_connection.send_request.return_value = mock_result

    # Execute without applying edits
    old_uri = "file:///path/to/old_model.sql"
    new_uri = "file:///path/to/new_model.sql"
    result = await lsp_client.rename_model(
        old_uri=old_uri, new_uri=new_uri, apply_edits=False
    )

    # Verify - should just return the workspace edits
    assert result == mock_result
    assert "changes" in result
    mock_lsp_connection.send_notification.assert_not_called()


@pytest.mark.asyncio
async def test_rename_model_error(lsp_client, mock_lsp_connection):
    """Test model rename request with LSP error."""
    # Setup mock to return an error
    mock_lsp_connection.send_request.return_value = {"error": "Model not found"}

    # Execute
    result = await lsp_client.rename_model(
        old_uri="file:///path/to/old_model.sql",
        new_uri="file:///path/to/new_model.sql",
    )

    # Verify
    assert result == {"error": "Model not found"}


@pytest.mark.asyncio
async def test_rename_model_source_not_exists(
    lsp_client, mock_lsp_connection, tmp_path
):
    """Test model rename when source file doesn't exist."""
    # Setup mock
    mock_lsp_connection.send_request.return_value = {"changes": {}}

    # Execute with non-existent file
    old_uri = f"file://{tmp_path / 'nonexistent.sql'}"
    new_uri = f"file://{tmp_path / 'new.sql'}"
    result = await lsp_client.rename_model(old_uri=old_uri, new_uri=new_uri)

    # Verify
    assert "error" in result
    assert "does not exist" in result["error"]


@pytest.mark.asyncio
async def test_rename_model_destination_exists(
    lsp_client, mock_lsp_connection, tmp_path
):
    """Test model rename when destination file already exists."""
    # Create both files
    old_file = tmp_path / "old.sql"
    new_file = tmp_path / "new.sql"
    old_file.write_text("content")
    new_file.write_text("existing")

    # Setup mock
    mock_lsp_connection.send_request.return_value = {"changes": {}}

    # Execute
    old_uri = f"file://{old_file}"
    new_uri = f"file://{new_file}"
    result = await lsp_client.rename_model(old_uri=old_uri, new_uri=new_uri)

    # Verify
    assert "error" in result
    assert "already exists" in result["error"]


@pytest.mark.asyncio
async def test_rename_model_none_response(lsp_client, mock_lsp_connection, tmp_path):
    """Test model rename when LSP returns None response."""
    # Create the source file
    old_file = tmp_path / "old.sql"
    new_file = tmp_path / "new.sql"
    old_file.write_text("content")

    # Setup mock to return None (simulating the user's issue)
    mock_lsp_connection.send_request.return_value = None

    # Execute - should not raise "argument of type 'NoneType' is not iterable"
    old_uri = f"file://{old_file}"
    new_uri = f"file://{new_file}"
    result = await lsp_client.rename_model(old_uri=old_uri, new_uri=new_uri)

    # Verify - should succeed with empty files_updated
    assert result["renamed"] is True
    assert result["old_path"] == str(old_file)
    assert result["new_path"] == str(new_file)
    assert result["files_updated"] == []

    # Verify file was renamed
    assert not old_file.exists()
    assert new_file.exists()
