"""Unit tests for the LSP connection module."""

import asyncio
import socket
import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.lsp.lsp_connection import (
    LSPConnection,
    LspConnectionState,
    LspEventName,
    JsonRpcMessage,
    event_name_from_string,
)


class TestJsonRpcMessage:
    """Test JsonRpcMessage dataclass."""

    def test_to_dict_with_request(self):
        """Test converting a request message to dictionary."""
        msg = JsonRpcMessage(id=1, method="initialize", params={"processId": None})
        result = msg.to_dict()

        assert result == {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"processId": None},
        }

    def test_to_dict_with_response(self):
        """Test converting a response message to dictionary."""
        msg = JsonRpcMessage(id=1, result={"capabilities": {}})
        result = msg.to_dict()

        assert result == {"jsonrpc": "2.0", "id": 1, "result": {"capabilities": {}}}

    def test_to_dict_with_error(self):
        """Test converting an error message to dictionary."""
        msg = JsonRpcMessage(
            id=1, error={"code": -32601, "message": "Method not found"}
        )
        result = msg.to_dict()

        assert result == {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32601, "message": "Method not found"},
        }

    def test_to_dict_notification(self):
        """Test converting a notification message to dictionary."""
        msg = JsonRpcMessage(
            method="window/logMessage", params={"type": 3, "message": "Server started"}
        )
        result = msg.to_dict()

        assert result == {
            "jsonrpc": "2.0",
            "method": "window/logMessage",
            "params": {"type": 3, "message": "Server started"},
        }

    def test_from_dict(self):
        """Test creating message from dictionary."""
        data = {
            "jsonrpc": "2.0",
            "id": 42,
            "method": "textDocument/completion",
            "params": {"textDocument": {"uri": "file:///test.sql"}},
        }
        msg = JsonRpcMessage(**data)

        assert msg.jsonrpc == "2.0"
        assert msg.id == 42
        assert msg.method == "textDocument/completion"
        assert msg.params == {"textDocument": {"uri": "file:///test.sql"}}


class TestLspEventName:
    """Test LspEventName enum and helpers."""

    def test_event_name_from_string_valid(self):
        """Test converting valid string to event name."""
        assert (
            event_name_from_string("dbt/lspCompileComplete")
            == LspEventName.compileComplete
        )
        assert event_name_from_string("window/logMessage") == LspEventName.logMessage
        assert event_name_from_string("$/progress") == LspEventName.progress

    def test_event_name_from_string_invalid(self):
        """Test converting invalid string returns None."""
        assert event_name_from_string("invalid/event") is None
        assert event_name_from_string("") is None


class TestLspConnectionState:
    """Test LspConnectionState dataclass."""

    def test_initial_state(self):
        """Test initial state values."""
        state = LspConnectionState()

        assert state.initialized is False
        assert state.shutting_down is False
        assert state.capabilities is not None
        assert len(state.capabilities) == 0
        assert state.pending_requests == {}
        assert state.pending_notifications == {}
        assert state.compiled is False

    def test_get_next_request_id(self):
        """Test request ID generation."""
        state = LspConnectionState()

        # Should start at 20 to avoid collisions
        id1 = state.get_next_request_id()
        id2 = state.get_next_request_id()
        id3 = state.get_next_request_id()

        assert id1 == 20
        assert id2 == 21
        assert id3 == 22


class TestLSPConnectionInitialization:
    """Test LSP connection initialization and validation."""

    def test_init_valid_binary(self, tmp_path):
        """Test initialization with valid binary path."""
        # Create a dummy binary file
        binary_path = tmp_path / "lsp-server"
        binary_path.touch()

        conn = LSPConnection(
            binary_path=str(binary_path),
            cwd="/test/dir",
            args=["--arg1", "--arg2"],
            connection_timeout=15,
            default_request_timeout=60,
        )

        assert conn.binary_path == binary_path
        assert conn.cwd == "/test/dir"
        assert conn.args == ["--arg1", "--arg2"]
        assert conn.host == "127.0.0.1"
        assert conn.port == 0
        assert conn.connection_timeout == 15
        assert conn.default_request_timeout == 60
        assert conn.process is None
        assert isinstance(conn.state, LspConnectionState)


class TestSocketSetup:
    """Test socket setup and lifecycle."""

    def test_setup_socket_success(self, tmp_path):
        """Test successful socket setup."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        with patch("socket.socket") as mock_socket_class:
            mock_socket = MagicMock()
            mock_socket.getsockname.return_value = ("127.0.0.1", 54321)
            mock_socket_class.return_value = mock_socket

            conn.setup_socket()

            # Verify socket setup
            mock_socket_class.assert_called_once_with(
                socket.AF_INET, socket.SOCK_STREAM
            )
            mock_socket.setsockopt.assert_called_once_with(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )
            mock_socket.bind.assert_called_once_with(("127.0.0.1", 0))
            mock_socket.listen.assert_called_once_with(1)

            assert conn.port == 54321
            assert conn._socket == mock_socket


class TestProcessLaunching:
    """Test LSP process launching and termination."""

    @pytest.mark.asyncio
    async def test_launch_lsp_process_success(self, tmp_path):
        """Test successful process launch."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test/dir")
        conn.port = 12345

        with patch("asyncio.create_subprocess_exec") as mock_create_subprocess:
            mock_process = MagicMock()
            mock_process.pid = 9999
            mock_create_subprocess.return_value = mock_process

            await conn.launch_lsp_process()

            # Verify process was started with correct arguments
            mock_create_subprocess.assert_called_once_with(
                str(binary_path), "--socket", "12345", "--project-dir", "/test/dir"
            )

            assert conn.process == mock_process


class TestStartStop:
    """Test start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_success(self, tmp_path):
        """Test successful server start."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Mock socket setup
        mock_socket = MagicMock()
        mock_connection = MagicMock()
        mock_socket.getsockname.return_value = ("127.0.0.1", 54321)

        # Mock process
        mock_process = MagicMock()
        mock_process.pid = 9999

        with (
            patch("socket.socket", return_value=mock_socket),
            patch("asyncio.create_subprocess_exec", return_value=mock_process),
            patch.object(conn, "_read_loop", new_callable=AsyncMock),
            patch.object(conn, "_write_loop", new_callable=AsyncMock),
        ):
            # Mock socket accept
            async def mock_accept_wrapper():
                return mock_connection, ("127.0.0.1", 12345)

            with patch("asyncio.get_running_loop") as mock_loop:
                mock_loop.return_value.run_in_executor.return_value = (
                    mock_accept_wrapper()
                )
                mock_loop.return_value.create_task.side_effect = (
                    lambda coro: asyncio.create_task(coro)
                )

                await conn.start()

                assert conn.process == mock_process
                assert conn._connection == mock_connection
                assert conn._reader_task is not None
                assert conn._writer_task is not None

    @pytest.mark.asyncio
    async def test_start_already_running(self, tmp_path):
        """Test starting when already running."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        conn.process = MagicMock()  # Simulate already running

        with (
            patch("socket.socket"),
            patch("asyncio.create_subprocess_exec") as mock_create_subprocess,
        ):
            await conn.start()

            # Should not create a new process
            mock_create_subprocess.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_timeout(self, tmp_path):
        """Test start timeout when server doesn't connect."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test", connection_timeout=0.1)

        mock_socket = MagicMock()
        mock_socket.getsockname.return_value = ("127.0.0.1", 54321)
        mock_process = MagicMock()

        with (
            patch("socket.socket", return_value=mock_socket),
            patch("asyncio.create_subprocess_exec", return_value=mock_process),
        ):
            # Simulate timeout in socket.accept
            mock_socket.accept.side_effect = TimeoutError

            with patch("asyncio.get_running_loop") as mock_loop:
                mock_loop.return_value.run_in_executor.side_effect = TimeoutError

                with pytest.raises(
                    RuntimeError, match="Timeout waiting for LSP server to connect"
                ):
                    await conn.start()

    @pytest.mark.asyncio
    async def test_stop_complete_cleanup(self, tmp_path):
        """Test complete cleanup on stop."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Setup mocks for running state
        conn.process = MagicMock()
        conn.process.terminate = MagicMock()
        conn.process.wait = AsyncMock()
        conn.process.kill = MagicMock()

        conn._socket = MagicMock()
        conn._connection = MagicMock()

        # Create mock tasks with proper async behavior
        async def mock_task():
            pass

        conn._reader_task = asyncio.create_task(mock_task())
        conn._writer_task = asyncio.create_task(mock_task())

        # Let tasks complete
        await asyncio.sleep(0.01)

        # Store references before they are set to None
        mock_connection = conn._connection
        mock_socket = conn._socket
        mock_process = conn.process

        with patch.object(conn, "_send_shutdown_request") as mock_shutdown:
            await conn.stop()

            # Verify cleanup methods were called
            mock_shutdown.assert_called_once()
            mock_connection.close.assert_called_once()
            mock_socket.close.assert_called_once()
            mock_process.terminate.assert_called_once()

            # Verify everything was set to None
            assert conn.process is None
            assert conn._socket is None
            assert conn._connection is None

    @pytest.mark.asyncio
    async def test_stop_force_kill(self, tmp_path):
        """Test force kill when process doesn't terminate."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Setup mock process that doesn't terminate
        mock_process = MagicMock()
        mock_process.terminate = MagicMock()
        mock_process.wait = AsyncMock(
            side_effect=[subprocess.TimeoutExpired("cmd", 1), None]
        )
        mock_process.kill = MagicMock()
        conn.process = mock_process

        await conn.stop()

        # Verify force kill was called
        mock_process.terminate.assert_called_once()
        mock_process.kill.assert_called_once()


class TestInitializeMethod:
    """Test LSP initialization handshake."""

    @pytest.mark.asyncio
    async def test_initialize_success(self, tmp_path):
        """Test successful initialization."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        conn.process = MagicMock()  # Simulate running

        # Mock send_request to return capabilities
        mock_result = {
            "capabilities": {
                "textDocumentSync": 2,
                "completionProvider": {"triggerCharacters": [".", ":"]},
            }
        }

        with (
            patch.object(
                conn, "send_request", new_callable=AsyncMock
            ) as mock_send_request,
            patch.object(conn, "send_notification") as mock_send_notification,
        ):
            mock_send_request.return_value = mock_result

            await conn.initialize(root_uri="file:///workspace", timeout=5)

            # Verify initialize request was sent
            mock_send_request.assert_called_once()
            call_args = mock_send_request.call_args
            assert call_args[0][0] == "initialize"
            assert call_args[1]["timeout"] == 5

            params = call_args[0][1]
            assert params["rootUri"] == "file:///workspace"
            assert params["clientInfo"]["name"] == "dbt-mcp"

            # Verify initialized notification was sent
            mock_send_notification.assert_called_once_with("initialized", {})

            # Verify state was updated
            assert conn.state.initialized is True
            assert conn.state.capabilities == mock_result["capabilities"]

    @pytest.mark.asyncio
    async def test_initialize_already_initialized(self, tmp_path):
        """Test initialization when already initialized."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        conn.process = MagicMock()
        conn.state.initialized = True

        with pytest.raises(RuntimeError, match="LSP server is already initialized"):
            await conn.initialize()


class TestMessageParsing:
    """Test JSON-RPC message parsing."""

    def test_parse_message_complete(self, tmp_path):
        """Test parsing a complete message."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Create a valid LSP message
        content = '{"jsonrpc":"2.0","id":1,"result":{"test":true}}'
        header = f"Content-Length: {len(content)}\r\n\r\n"
        buffer = (header + content).encode("utf-8")

        message, remaining = conn._parse_message(buffer)

        assert message is not None
        assert message.id == 1
        assert message.result == {"test": True}
        assert remaining == b""

    def test_parse_message_incomplete_header(self, tmp_path):
        """Test parsing with incomplete header."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        buffer = b"Content-Length: 50\r\n"  # Missing \r\n\r\n

        message, remaining = conn._parse_message(buffer)

        assert message is None
        assert remaining == buffer

    def test_parse_message_incomplete_content(self, tmp_path):
        """Test parsing with incomplete content."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        content = '{"jsonrpc":"2.0","id":1,"result":{"test":true}}'
        header = f"Content-Length: {len(content)}\r\n\r\n"
        # Only include part of the content
        buffer = (header + content[:10]).encode("utf-8")

        message, remaining = conn._parse_message(buffer)

        assert message is None
        assert remaining == buffer

    def test_parse_message_invalid_json(self, tmp_path):
        """Test parsing with invalid JSON content."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        content = '{"invalid json'
        header = f"Content-Length: {len(content)}\r\n\r\n"
        buffer = (header + content).encode("utf-8")

        message, remaining = conn._parse_message(buffer)

        assert message is None
        assert remaining == b""  # Invalid message is discarded

    def test_parse_message_missing_content_length(self, tmp_path):
        """Test parsing with missing Content-Length header."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        buffer = b'Some-Header: value\r\n\r\n{"test":true}'

        message, remaining = conn._parse_message(buffer)

        assert message is None
        assert remaining == b'{"test":true}'  # Header consumed, content remains

    def test_parse_message_multiple_messages(self, tmp_path):
        """Test parsing multiple messages from buffer."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Create two messages
        content1 = '{"jsonrpc":"2.0","id":1,"result":true}'
        content2 = '{"jsonrpc":"2.0","id":2,"result":false}'
        header1 = f"Content-Length: {len(content1)}\r\n\r\n"
        header2 = f"Content-Length: {len(content2)}\r\n\r\n"
        buffer = (header1 + content1 + header2 + content2).encode("utf-8")

        # Parse first message
        message1, remaining1 = conn._parse_message(buffer)
        assert message1 is not None
        assert message1.id == 1
        assert message1.result is True

        # Parse second message
        message2, remaining2 = conn._parse_message(remaining1)
        assert message2 is not None
        assert message2.id == 2
        assert message2.result is False
        assert remaining2 == b""


class TestMessageHandling:
    """Test incoming message handling."""

    def test_handle_response_message(self, tmp_path):
        """Test handling response to a request."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Create a pending request
        future = asyncio.Future()
        conn.state.pending_requests[42] = future

        # Handle response message
        message = JsonRpcMessage(id=42, result={"success": True})

        with patch.object(future, "get_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop

            conn._handle_incoming_message(message)

            # Verify future was resolved
            mock_loop.call_soon_threadsafe.assert_called_once()
            args = mock_loop.call_soon_threadsafe.call_args[0]
            assert args[0] == future.set_result
            assert args[1] == {"success": True}

            # Verify request was removed from pending
            assert 42 not in conn.state.pending_requests

    def test_handle_error_response(self, tmp_path):
        """Test handling error response."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Create a pending request
        future = asyncio.Future()
        conn.state.pending_requests[42] = future

        # Handle error response
        message = JsonRpcMessage(
            id=42, error={"code": -32601, "message": "Method not found"}
        )

        with patch.object(future, "get_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop

            conn._handle_incoming_message(message)

            # Verify future was rejected
            mock_loop.call_soon_threadsafe.assert_called_once()
            args = mock_loop.call_soon_threadsafe.call_args[0]
            assert args[0] == future.set_exception

    def test_handle_unknown_response(self, tmp_path):
        """Test handling response for unknown request ID."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Handle response with unknown ID
        message = JsonRpcMessage(id=999, result={"test": True})

        with patch.object(conn, "_send_message") as mock_send:
            conn._handle_incoming_message(message)

            # Should send empty response back
            mock_send.assert_called_once()
            sent_msg = mock_send.call_args[0][0]
            assert isinstance(sent_msg, JsonRpcMessage)
            assert sent_msg.id == 999
            assert sent_msg.result is None

    def test_handle_notification(self, tmp_path):
        """Test handling notification messages."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Create futures waiting for compile complete event
        future1 = asyncio.Future()
        future2 = asyncio.Future()
        conn.state.pending_notifications[LspEventName.compileComplete] = [
            future1,
            future2,
        ]

        # Handle compile complete notification
        message = JsonRpcMessage(
            method="dbt/lspCompileComplete", params={"success": True}
        )

        with (
            patch.object(future1, "get_loop") as mock_get_loop1,
            patch.object(future2, "get_loop") as mock_get_loop2,
        ):
            mock_loop1 = MagicMock()
            mock_loop2 = MagicMock()
            mock_get_loop1.return_value = mock_loop1
            mock_get_loop2.return_value = mock_loop2

            conn._handle_incoming_message(message)

            # Verify futures were resolved
            mock_loop1.call_soon_threadsafe.assert_called_once_with(
                future1.set_result, {"success": True}
            )
            mock_loop2.call_soon_threadsafe.assert_called_once_with(
                future2.set_result, {"success": True}
            )

            # Verify compile state was set
            assert conn.state.compiled is True

    def test_handle_unknown_notification(self, tmp_path):
        """Test handling unknown notification."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Handle unknown notification
        message = JsonRpcMessage(method="unknown/notification", params={"data": "test"})

        # Should not raise, just log
        conn._handle_incoming_message(message)


class TestSendRequest:
    """Test sending requests to LSP server."""

    @pytest.mark.asyncio
    async def test_send_request_success(self, tmp_path):
        """Test successful request sending."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        conn.process = MagicMock()  # Simulate running

        with (
            patch.object(conn, "_send_message") as mock_send,
            patch("asyncio.wait_for", new_callable=AsyncMock) as mock_wait_for,
        ):
            mock_wait_for.return_value = {"result": "success"}

            result = await conn.send_request(
                "testMethod", {"param": "value"}, timeout=5
            )

            # Verify message was sent
            mock_send.assert_called_once()
            sent_msg = mock_send.call_args[0][0]
            assert isinstance(sent_msg, JsonRpcMessage)
            assert sent_msg.method == "testMethod"
            assert sent_msg.params == {"param": "value"}
            assert sent_msg.id is not None

            # Verify result
            assert result == {"result": "success"}

    @pytest.mark.asyncio
    async def test_send_request_not_running(self, tmp_path):
        """Test sending request when server not running."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        # process is None - not running

        with pytest.raises(RuntimeError, match="LSP server is not running"):
            await conn.send_request("testMethod")

    @pytest.mark.asyncio
    async def test_send_request_timeout(self, tmp_path):
        """Test request timeout."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test", default_request_timeout=1)
        conn.process = MagicMock()

        with patch.object(conn, "_send_message"):
            # Create a future that never resolves
            future = asyncio.Future()
            conn.state.pending_requests[20] = future

            # Use real wait_for to test timeout
            result = await conn.send_request("testMethod", timeout=0.01)

            assert "error" in result


class TestSendNotification:
    """Test sending notifications to LSP server."""

    def test_send_notification_success(self, tmp_path):
        """Test successful notification sending."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        conn.process = MagicMock()

        with patch.object(conn, "_send_message") as mock_send:
            conn.send_notification(
                "window/showMessage", {"type": 3, "message": "Hello"}
            )

            # Verify message was sent
            mock_send.assert_called_once()
            sent_msg = mock_send.call_args[0][0]
            assert isinstance(sent_msg, JsonRpcMessage)
            assert sent_msg.method == "window/showMessage"
            assert sent_msg.params == {"type": 3, "message": "Hello"}
            assert sent_msg.id is None  # Notifications have no ID

    def test_send_notification_not_running(self, tmp_path):
        """Test sending notification when server not running."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        # process is None - not running

        with pytest.raises(RuntimeError, match="LSP server is not running"):
            conn.send_notification("testMethod")


class TestWaitForNotification:
    """Test waiting for notifications."""

    def test_wait_for_notification(self, tmp_path):
        """Test registering to wait for a notification."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_future = MagicMock()
            mock_loop.create_future.return_value = mock_future
            mock_get_loop.return_value = mock_loop

            result = conn.wait_for_notification(LspEventName.compileComplete)

            # Verify future was created and registered
            assert result == mock_future
            assert LspEventName.compileComplete in conn.state.pending_notifications
            assert (
                mock_future
                in conn.state.pending_notifications[LspEventName.compileComplete]
            )


class TestSendMessage:
    """Test low-level message sending."""

    def test_send_message_with_jsonrpc_message(self, tmp_path):
        """Test sending JsonRpcMessage."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        conn._outgoing_queue = MagicMock()

        message = JsonRpcMessage(id=1, method="test", params={"key": "value"})

        conn._send_message(message)

        # Verify message was queued
        conn._outgoing_queue.put_nowait.assert_called_once()
        data = conn._outgoing_queue.put_nowait.call_args[0][0]

        # Parse the data to verify format
        assert b"Content-Length:" in data
        assert b"\r\n\r\n" in data
        # JSON might have spaces after colons, check for both variants
        assert b'"jsonrpc"' in data and b'"2.0"' in data
        assert b'"method"' in data and b'"test"' in data


class TestShutdown:
    """Test shutdown sequence."""

    def test_send_shutdown_request(self, tmp_path):
        """Test sending shutdown and exit messages."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        with patch.object(conn, "_send_message") as mock_send:
            conn._send_shutdown_request()

            # Verify two messages were sent
            assert mock_send.call_count == 2

            # First should be shutdown request
            shutdown_msg = mock_send.call_args_list[0][0][0]
            assert isinstance(shutdown_msg, JsonRpcMessage)
            assert shutdown_msg.method == "shutdown"
            assert shutdown_msg.id is not None

            # Second should be exit notification
            exit_msg = mock_send.call_args_list[1][0][0]
            assert isinstance(exit_msg, JsonRpcMessage)
            assert exit_msg.method == "exit"
            assert exit_msg.id is None


class TestIsRunning:
    """Test is_running method."""

    def test_is_running_true(self, tmp_path):
        """Test when process is running."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        conn.process = MagicMock()
        conn.process.returncode = None

        assert conn.is_running() is True

    def test_is_running_false_no_process(self, tmp_path):
        """Test when no process."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        assert conn.is_running() is False

    def test_is_running_false_process_exited(self, tmp_path):
        """Test when process has exited."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        conn.process = MagicMock()
        conn.process.returncode = 0

        assert conn.is_running() is False


class TestReadWriteLoops:
    """Test async I/O loops."""

    @pytest.mark.asyncio
    async def test_read_loop_processes_messages(self, tmp_path):
        """Test read loop processes incoming messages."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Setup mock connection
        mock_connection = MagicMock()
        conn._connection = mock_connection

        # Create test data
        content = '{"jsonrpc":"2.0","id":1,"result":true}'
        header = f"Content-Length: {len(content)}\r\n\r\n"
        test_data = (header + content).encode("utf-8")

        # Mock recv to return data once then empty
        recv_calls = [test_data, b""]

        async def mock_recv_wrapper(size):
            if recv_calls:
                return recv_calls.pop(0)
            return b""

        with (
            patch("asyncio.get_running_loop") as mock_get_loop,
            patch.object(conn, "_handle_incoming_message") as mock_handle,
        ):
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            mock_loop.run_in_executor.side_effect = (
                lambda _, func, *args: mock_recv_wrapper(*args)
            )

            # Run read loop (will exit when recv returns empty)
            await conn._read_loop()

            # Verify message was handled
            mock_handle.assert_called_once()
            handled_msg = mock_handle.call_args[0][0]
            assert handled_msg.id == 1
            assert handled_msg.result is True

    @pytest.mark.asyncio
    async def test_write_loop_sends_messages(self, tmp_path):
        """Test write loop sends queued messages."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Setup mock connection
        mock_connection = MagicMock()
        conn._connection = mock_connection

        # Queue test data
        test_data = b"test message data"
        conn._outgoing_queue.put_nowait(test_data)

        # Set stop event after first iteration
        async def stop_after_one():
            await asyncio.sleep(0.01)
            conn._stop_event.set()

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            mock_loop.run_in_executor.return_value = asyncio.sleep(0)

            # Run both coroutines
            await asyncio.gather(
                conn._write_loop(), stop_after_one(), return_exceptions=True
            )

            # Verify data was sent
            mock_loop.run_in_executor.assert_called()
            call_args = mock_loop.run_in_executor.call_args_list[-1]
            assert call_args[0][1] == mock_connection.sendall
            assert call_args[0][2] == test_data


class TestEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_concurrent_requests(self, tmp_path):
        """Test handling concurrent requests."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        conn.process = MagicMock()

        # Track sent messages
        sent_messages = []

        def track_message(msg):
            sent_messages.append(msg)

        with patch.object(conn, "_send_message", side_effect=track_message):
            # Create futures for multiple requests
            future1 = asyncio.create_task(
                conn.send_request("method1", JsonRpcMessage(id=1))
            )
            future2 = asyncio.create_task(
                conn.send_request("method2", JsonRpcMessage(id=2))
            )
            future3 = asyncio.create_task(
                conn.send_request("method3", JsonRpcMessage(id=3))
            )

            # Let tasks start
            await asyncio.sleep(0.01)

            # Verify all messages were sent with unique IDs
            assert len(sent_messages) == 3
            ids = [msg.id for msg in sent_messages]
            assert len(set(ids)) == 3  # All IDs are unique

            # Simulate responses
            for msg in sent_messages:
                if msg.id in conn.state.pending_requests:
                    future = conn.state.pending_requests[msg.id]
                    future.set_result({"response": msg.id})

            # Wait for all requests
            results = await asyncio.gather(future1, future2, future3)

            # Verify each got correct response
            assert all("response" in r for r in results)

    @pytest.mark.asyncio
    async def test_stop_with_pending_requests(self, tmp_path):
        """Test stopping with pending requests."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")
        conn.process = MagicMock()
        conn.process.terminate = MagicMock()
        conn.process.wait = AsyncMock()

        # Add pending requests
        future1 = asyncio.Future()
        future2 = asyncio.Future()
        conn.state.pending_requests[1] = future1
        conn.state.pending_requests[2] = future2

        await conn.stop()

        # Verify state was cleared
        assert len(conn.state.pending_requests) == 0

    def test_message_with_unicode(self, tmp_path):
        """Test handling messages with unicode content."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = LSPConnection(str(binary_path), "/test")

        # Create message with unicode
        content = '{"jsonrpc":"2.0","method":"test","params":{"text":"Hello 世界 🚀"}}'
        header = f"Content-Length: {len(content.encode('utf-8'))}\r\n\r\n"
        buffer = header.encode("utf-8") + content.encode("utf-8")

        message, remaining = conn._parse_message(buffer)

        assert message is not None
        assert message.method == "test"
        assert message.params["text"] == "Hello 世界 🚀"
        assert remaining == b""
