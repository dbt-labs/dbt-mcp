import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from dbt_mcp.lsp.lsp_client import LSPClient
from dbt_mcp.lsp.lsp_connection import (
    JsonRpcMessage,
    StdioLSPConnection,
)
from dbt_mcp.lsp.providers.lsp_connection_provider import LspEventName
from dbt_mcp.lsp.providers.lsp_connection_provider import (
    LSPConnectionProviderProtocol,
)


def make_connection() -> MagicMock:
    connection = MagicMock(spec=LSPConnectionProviderProtocol)
    connection.compiled.return_value = True
    connection.send_request = AsyncMock()
    connection.send_notification = MagicMock()
    return connection


@pytest.mark.asyncio
async def test_client_resolves_definition_from_project_relative_path(tmp_path) -> None:
    connection = make_connection()
    connection.send_request.return_value = [{"uri": "file:///models/base.sql"}]
    client = LSPClient(connection, project_dir=str(tmp_path))

    result = await client.get_definition("models/orders.sql", 2, 4)

    assert result == {"locations": [{"uri": "file:///models/base.sql"}]}
    uri = (tmp_path / "models/orders.sql").resolve().as_uri()
    connection.send_request.assert_awaited_once_with(
        "textDocument/definition",
        {
            "textDocument": {"uri": uri},
            "position": {"line": 2, "character": 4},
        },
    )


@pytest.mark.asyncio
async def test_client_normalizes_diagnostics(tmp_path) -> None:
    connection = make_connection()
    connection.send_request.return_value = {
        "kind": "full",
        "resultId": "42",
        "items": [{"message": "invalid ref"}],
    }
    client = LSPClient(connection, project_dir=str(tmp_path))

    result = await client.get_diagnostics("models/orders.sql")

    assert result["items"] == [{"message": "invalid ref"}]
    assert result["result_id"] == "42"
    assert result["report"]["kind"] == "full"


@pytest.mark.asyncio
async def test_diagnostics_preserve_transport_errors(tmp_path) -> None:
    connection = make_connection()
    connection.send_request.return_value = {"error": "server unavailable"}
    client = LSPClient(connection, project_dir=str(tmp_path))

    assert await client.get_diagnostics("models/orders.sql") == {
        "error": "server unavailable"
    }


@pytest.mark.asyncio
async def test_update_document_uses_open_then_incremental_change(tmp_path) -> None:
    connection = make_connection()
    connection.is_document_open.side_effect = [False, True]
    connection.document_version.return_value = 1
    client = LSPClient(connection, project_dir=str(tmp_path))

    first = await client.update_document(
        "models/orders.sql", "select 1", wait_for_compile=False
    )
    second = await client.update_document(
        "models/orders.sql", "select 2", wait_for_compile=False
    )

    assert first["method"] == "textDocument/didOpen"
    assert second["method"] == "textDocument/didChange"
    assert second["version"] == 2
    assert connection.send_notification.call_count == 2
    first_call = connection.send_notification.call_args_list[0]
    second_call = connection.send_notification.call_args_list[1]
    assert first_call.args[0] == "textDocument/didOpen"
    assert first_call.args[1]["textDocument"]["text"] == "select 1"
    assert second_call.args[0] == "textDocument/didChange"
    assert second_call.args[1]["contentChanges"] == [{"text": "select 2"}]


@pytest.mark.asyncio
async def test_stdio_initialize_advertises_agent_capabilities(tmp_path) -> None:
    connection = cast(Any, StdioLSPConnection(["dbt", "lsp"], str(tmp_path)))
    connection.send_request = AsyncMock(return_value={"capabilities": {}})
    connection.send_notification = MagicMock()

    await connection.initialize()

    params = connection.send_request.call_args.args[1]
    assert params["rootUri"] == tmp_path.resolve().as_uri()
    assert params["workspaceFolders"][0]["uri"] == tmp_path.resolve().as_uri()
    assert params["capabilities"]["workspace"]["configuration"] is True
    assert (
        "codeActionLiteralSupport"
        in params["capabilities"]["textDocument"]["codeAction"]
    )
    connection.send_notification.assert_called_once_with("initialized", {})


def test_stdio_answers_workspace_configuration_request(tmp_path) -> None:
    connection = cast(Any, StdioLSPConnection(["dbt", "lsp"], str(tmp_path)))
    connection._send_message = MagicMock()

    connection._handle_incoming_message(
        JsonRpcMessage(
            id=27,
            method="workspace/configuration",
            params={"items": [{"section": "dbt"}]},
        )
    )

    response = connection._send_message.call_args.args[0]
    assert response.id == 27
    assert response.result == [connection.client_configuration]


def test_document_notifications_track_versions(tmp_path) -> None:
    connection = cast(Any, StdioLSPConnection(["dbt", "lsp"], str(tmp_path)))
    connection.process = MagicMock(returncode=None)
    connection._send_message = MagicMock()
    uri = (tmp_path / "models/orders.sql").resolve().as_uri()

    connection.send_notification(
        "textDocument/didOpen",
        {"textDocument": {"uri": uri, "version": 1}},
    )
    assert connection.is_document_open(uri)
    assert connection.document_version(uri) == 1

    connection.send_notification(
        "textDocument/didChange",
        {"textDocument": {"uri": uri, "version": 2}},
    )
    assert connection.document_version(uri) == 2

    connection.send_notification(
        "textDocument/didClose", {"textDocument": {"uri": uri}}
    )
    assert not connection.is_document_open(uri)


@pytest.mark.asyncio
async def test_code_actions_include_current_diagnostics(tmp_path) -> None:
    connection = make_connection()
    connection.send_request.side_effect = [
        {"items": [{"code": "dbt-fmt"}]},
        [{"title": "Apply formatting"}],
    ]
    client = LSPClient(connection, project_dir=str(tmp_path))

    result = await client.get_code_actions("models/orders.sql", 0, 0, 1, 0)

    assert result == {"actions": [{"title": "Apply formatting"}]}
    code_action_params = connection.send_request.await_args_list[1].args[1]
    assert code_action_params["context"]["diagnostics"] == [{"code": "dbt-fmt"}]


@pytest.mark.asyncio
async def test_rename_preview_stops_when_prepare_returns_null(tmp_path) -> None:
    connection = make_connection()
    connection.send_request.return_value = None
    client = LSPClient(connection, project_dir=str(tmp_path))

    result = await client.get_rename_preview("models/orders.sql", 0, 0, "new_name")

    assert result == {"prepare": None, "edit": None}
    connection.send_request.assert_awaited_once()


@pytest.mark.asyncio
async def test_late_notification_ignores_cancelled_waiter(tmp_path) -> None:
    connection = StdioLSPConnection(["dbt", "lsp"], str(tmp_path))
    waiter = asyncio.get_running_loop().create_future()
    waiter.cancel()
    connection.state.pending_notifications[LspEventName.backgroundCompileComplete] = [
        waiter
    ]

    connection._handle_incoming_message(
        JsonRpcMessage(
            method="dbt/lspBackgroundCompileComplete",
            params={"status": "ok"},
        )
    )
    await asyncio.sleep(0)

    assert waiter.cancelled()


@pytest.mark.asyncio
async def test_navigation_and_actions_normalize_empty_results(tmp_path) -> None:
    connection = make_connection()
    connection.send_request.return_value = None
    client = LSPClient(connection, project_dir=str(tmp_path))

    assert await client.get_definition("models/orders.sql", 0, 0) == {"locations": []}
    assert await client.get_references("models/orders.sql", 0, 0) == {"locations": []}
    assert await client.get_code_actions("models/orders.sql", 0, 0, 0, 0) == {
        "actions": []
    }
