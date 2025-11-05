"""Integration-style tests for LSP connection using real instances instead of mocks.

These tests use real sockets, asyncio primitives, and actual data flow
to provide more realistic test coverage compared to heavily mocked unit tests.
"""

import asyncio
import json
import socket

import pytest

from dbt_mcp.lsp.lsp_connection import (
    SocketLSPConnection,
    LspEventName,
    JsonRpcMessage,
)


class TestRealSocketOperations:
    """Tests using real sockets to verify actual network communication."""

    def test_setup_socket_real(self, tmp_path):
        """Test socket setup with real socket binding."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Use real socket
        conn.setup_socket()

        try:
            # Verify real socket was created and bound
            assert conn._socket is not None
            assert isinstance(conn._socket, socket.socket)
            assert conn.port > 0  # OS assigned a port
            assert conn.host == "127.0.0.1"

            # Verify socket is actually listening
            sockname = conn._socket.getsockname()
            assert sockname[0] == "127.0.0.1"
            assert sockname[1] == conn.port

        finally:
            # Cleanup
            if conn._socket:
                conn._socket.close()

    def test_socket_reuse_address(self, tmp_path):
        """Test that SO_REUSEADDR is set on real socket."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")
        conn.setup_socket()

        try:
            # Verify SO_REUSEADDR is set (value varies by platform, just check it's non-zero)
            reuse = conn._socket.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR)
            assert reuse != 0

        finally:
            if conn._socket:
                conn._socket.close()

    @pytest.mark.asyncio
    async def test_socket_accept_with_real_client(self, tmp_path):
        """Test socket accept with real client connection."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test", connection_timeout=2.0)
        conn.setup_socket()

        try:
            # Create real client socket that connects to the server
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            async def client_connect():
                await asyncio.sleep(0.1)  # Let server start listening
                await asyncio.get_running_loop().run_in_executor(
                    None, client_socket.connect, (conn.host, conn.port)
                )

            async def server_accept():
                conn._socket.settimeout(conn.connection_timeout)
                connection, addr = await asyncio.get_running_loop().run_in_executor(
                    None, conn._socket.accept
                )
                return connection, addr

            # Run both concurrently
            client_task = asyncio.create_task(client_connect())
            server_result = await server_accept()

            await client_task

            connection, client_addr = server_result
            assert connection is not None
            assert client_addr[0] in ("127.0.0.1", "::1")  # IPv4 or IPv6 localhost

            # Cleanup
            connection.close()
            client_socket.close()

        finally:
            if conn._socket:
                conn._socket.close()


class TestRealAsyncioQueues:
    """Tests using real asyncio queues to verify message queueing."""

    def test_send_message_with_real_queue(self, tmp_path):
        """Test message sending with real asyncio queue."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # The _outgoing_queue is already a real asyncio.Queue
        assert isinstance(conn._outgoing_queue, asyncio.Queue)
        assert conn._outgoing_queue.empty()

        # Send a message
        message = JsonRpcMessage(id=1, method="test", params={"key": "value"})
        conn._send_message(message)

        # Verify message was actually queued
        assert not conn._outgoing_queue.empty()
        data = conn._outgoing_queue.get_nowait()

        # Verify LSP protocol format
        assert isinstance(data, bytes)
        assert b"Content-Length:" in data
        assert b"\r\n\r\n" in data
        assert b'"jsonrpc"' in data
        assert b'"2.0"' in data
        assert b'"test"' in data

    def test_multiple_messages_queue_order(self, tmp_path):
        """Test that multiple messages maintain FIFO order in real queue."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Send multiple messages
        msg1 = JsonRpcMessage(id=1, method="first")
        msg2 = JsonRpcMessage(id=2, method="second")
        msg3 = JsonRpcMessage(id=3, method="third")

        conn._send_message(msg1)
        conn._send_message(msg2)
        conn._send_message(msg3)

        # Verify queue size
        assert conn._outgoing_queue.qsize() == 3

        # Verify FIFO order
        data1 = conn._outgoing_queue.get_nowait()
        data2 = conn._outgoing_queue.get_nowait()
        data3 = conn._outgoing_queue.get_nowait()

        assert b'"first"' in data1
        assert b'"second"' in data2
        assert b'"third"' in data3

        # Queue should be empty
        assert conn._outgoing_queue.empty()


class TestRealAsyncioFutures:
    """Tests using real asyncio futures to verify async behavior."""

    @pytest.mark.asyncio
    async def test_handle_response_with_real_future(self, tmp_path):
        """Test handling response with real asyncio future."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Create real future in current event loop
        future = asyncio.get_running_loop().create_future()
        conn.state.pending_requests[42] = future

        # Handle response in the same loop
        message = JsonRpcMessage(id=42, result={"success": True, "data": "test"})
        conn._handle_incoming_message(message)

        # Wait for future to be resolved (should be immediate via call_soon_threadsafe)
        result = await asyncio.wait_for(future, timeout=1.0)

        assert result == {"success": True, "data": "test"}
        assert 42 not in conn.state.pending_requests

    @pytest.mark.asyncio
    async def test_handle_error_with_real_future(self, tmp_path):
        """Test handling error response with real asyncio future."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Create real future
        future = asyncio.get_running_loop().create_future()
        conn.state.pending_requests[42] = future

        # Handle error response
        message = JsonRpcMessage(
            id=42, error={"code": -32601, "message": "Method not found"}
        )
        conn._handle_incoming_message(message)

        # Future should be rejected with exception
        with pytest.raises(RuntimeError, match="LSP error"):
            await asyncio.wait_for(future, timeout=1.0)

        assert 42 not in conn.state.pending_requests

    @pytest.mark.asyncio
    async def test_notification_futures_real(self, tmp_path):
        """Test waiting for notifications with real futures."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Register to wait for a notification
        future = conn.wait_for_notification(LspEventName.compileComplete)

        # Verify it's a real future
        assert isinstance(future, asyncio.Future)
        assert not future.done()

        # Simulate receiving the notification
        message = JsonRpcMessage(
            method="dbt/lspCompileComplete", params={"success": True, "errors": []}
        )
        conn._handle_incoming_message(message)

        # Wait for notification
        result = await asyncio.wait_for(future, timeout=1.0)

        assert result == {"success": True, "errors": []}
        assert conn.state.compiled is True


class TestRealSocketCommunication:
    """Tests using real socket pairs to verify end-to-end communication."""

    @pytest.mark.asyncio
    async def test_socket_pair_communication(self, tmp_path):
        """Test bidirectional communication using socketpair."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Create a real socket pair (connected sockets)
        server_socket, client_socket = socket.socketpair()
        conn._connection = server_socket

        try:
            # Send a message through the connection
            message = JsonRpcMessage(id=1, method="test", params={"foo": "bar"})
            conn._send_message(message)

            # Get the data from the queue
            data = conn._outgoing_queue.get_nowait()

            # Actually send it through the socket
            await asyncio.get_running_loop().run_in_executor(
                None, server_socket.sendall, data
            )

            # Read it back on the client side
            received_data = await asyncio.get_running_loop().run_in_executor(
                None, client_socket.recv, 4096
            )

            # Verify we got the complete LSP message
            assert b"Content-Length:" in received_data
            assert b"\r\n\r\n" in received_data
            assert b'"test"' in received_data
            assert b'"foo"' in received_data
            assert b'"bar"' in received_data

        finally:
            server_socket.close()
            client_socket.close()

    @pytest.mark.asyncio
    async def test_message_roundtrip_real(self, tmp_path):
        """Test complete message send and parse roundtrip."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Create socket pair
        server_socket, client_socket = socket.socketpair()
        conn._connection = server_socket

        try:
            # Original message
            original_message = JsonRpcMessage(
                id=123,
                method="textDocument/completion",
                params={
                    "textDocument": {"uri": "file:///test.sql"},
                    "position": {"line": 10, "character": 5},
                },
            )

            # Send through connection
            conn._send_message(original_message)
            data = conn._outgoing_queue.get_nowait()
            await asyncio.get_running_loop().run_in_executor(
                None, server_socket.sendall, data
            )

            # Receive on client side
            received_data = await asyncio.get_running_loop().run_in_executor(
                None, client_socket.recv, 4096
            )

            # Parse it back
            parsed_message, remaining = conn._parse_message(received_data)

            # Verify roundtrip integrity
            assert parsed_message is not None
            assert parsed_message.id == original_message.id
            assert parsed_message.method == original_message.method
            assert parsed_message.params == original_message.params
            assert remaining == b""

        finally:
            server_socket.close()
            client_socket.close()

    @pytest.mark.asyncio
    async def test_multiple_messages_streaming(self, tmp_path):
        """Test streaming multiple messages through real socket."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Create socket pair
        server_socket, client_socket = socket.socketpair()
        conn._connection = server_socket

        try:
            # Set non-blocking for client to avoid hangs
            client_socket.setblocking(False)

            # Send multiple messages
            messages = [
                JsonRpcMessage(id=1, method="initialize"),
                JsonRpcMessage(method="initialized", params={}),
                JsonRpcMessage(id=2, method="textDocument/didOpen"),
            ]

            for msg in messages:
                conn._send_message(msg)
                data = conn._outgoing_queue.get_nowait()
                await asyncio.get_running_loop().run_in_executor(
                    None, server_socket.sendall, data
                )

            # Receive all data on client side with timeout
            received_data = b""
            client_socket.setblocking(True)
            client_socket.settimeout(1.0)

            try:
                while True:
                    chunk = await asyncio.get_running_loop().run_in_executor(
                        None, client_socket.recv, 4096
                    )
                    if not chunk:
                        break
                    received_data += chunk

                    # Try to parse - if we have all 3 messages, we're done
                    temp_buffer = received_data
                    temp_count = 0
                    while True:
                        msg, temp_buffer = conn._parse_message(temp_buffer)
                        if msg is None:
                            break
                        temp_count += 1

                    if temp_count >= 3:
                        break
            except TimeoutError:
                pass  # Expected when all data is received

            # Parse all messages
            buffer = received_data
            parsed_messages = []

            while buffer:
                msg, buffer = conn._parse_message(buffer)
                if msg is None:
                    break
                parsed_messages.append(msg)

            # Verify all messages were received and parsed correctly
            assert len(parsed_messages) == 3
            assert parsed_messages[0].id == 1
            assert parsed_messages[0].method == "initialize"
            assert parsed_messages[1].method == "initialized"
            assert parsed_messages[2].id == 2

        finally:
            server_socket.close()
            client_socket.close()


class TestRealMessageParsing:
    """Tests parsing with real byte streams."""

    def test_parse_real_lsp_message(self, tmp_path):
        """Test parsing a real LSP protocol message."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Create a real LSP message exactly as it would be sent
        content = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "capabilities": {
                        "textDocumentSync": 2,
                        "completionProvider": {"triggerCharacters": ["."]},
                    }
                },
            }
        )
        content_bytes = content.encode("utf-8")
        header = f"Content-Length: {len(content_bytes)}\r\n\r\n"
        full_message = header.encode("utf-8") + content_bytes

        # Parse it
        message, remaining = conn._parse_message(full_message)

        assert message is not None
        assert message.id == 1
        assert "capabilities" in message.result
        assert message.result["capabilities"]["textDocumentSync"] == 2
        assert remaining == b""

    def test_parse_chunked_message_real(self, tmp_path):
        """Test parsing message that arrives in multiple chunks."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Create a message
        content = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "test"})
        content_bytes = content.encode("utf-8")
        header = f"Content-Length: {len(content_bytes)}\r\n\r\n"
        full_message = header.encode("utf-8") + content_bytes

        # Split into chunks (simulate network chunking)
        chunk1 = full_message[:20]
        chunk2 = full_message[20:40]
        chunk3 = full_message[40:]

        # Parse first chunk - should be incomplete
        msg1, buffer = conn._parse_message(chunk1)
        assert msg1 is None
        assert buffer == chunk1

        # Add second chunk - still incomplete
        buffer += chunk2
        msg2, buffer = conn._parse_message(buffer)
        assert msg2 is None

        # Add final chunk - should complete
        buffer += chunk3
        msg3, buffer = conn._parse_message(buffer)
        assert msg3 is not None
        assert msg3.id == 1
        assert msg3.method == "test"
        assert buffer == b""


class TestRealConcurrentOperations:
    """Tests with real concurrent async operations."""

    @pytest.mark.asyncio
    async def test_concurrent_request_futures(self, tmp_path):
        """Test handling multiple concurrent requests with real futures."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Create multiple real futures for concurrent requests
        futures = {}
        for i in range(10):
            future = asyncio.get_running_loop().create_future()
            futures[i] = future
            conn.state.pending_requests[i] = future

        # Simulate responses arriving concurrently
        async def respond(request_id: int, delay: float):
            await asyncio.sleep(delay)
            message = JsonRpcMessage(id=request_id, result={"request_id": request_id})
            conn._handle_incoming_message(message)

        # Start all responses with random delays
        response_tasks = [asyncio.create_task(respond(i, i * 0.01)) for i in range(10)]

        # Wait for all futures to resolve
        results = await asyncio.gather(*[futures[i] for i in range(10)])

        # Verify all completed correctly
        assert len(results) == 10
        for i, result in enumerate(results):
            assert result["request_id"] == i

        # All requests should be removed
        assert len(conn.state.pending_requests) == 0

        # Cleanup
        await asyncio.gather(*response_tasks)

    @pytest.mark.asyncio
    async def test_concurrent_notifications_real(self, tmp_path):
        """Test multiple futures waiting for the same notification."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Create multiple waiters for the same event
        future1 = conn.wait_for_notification(LspEventName.compileComplete)
        future2 = conn.wait_for_notification(LspEventName.compileComplete)
        future3 = conn.wait_for_notification(LspEventName.compileComplete)

        # All should be real futures
        assert all(isinstance(f, asyncio.Future) for f in [future1, future2, future3])

        # Send the notification
        message = JsonRpcMessage(
            method="dbt/lspCompileComplete", params={"status": "success"}
        )
        conn._handle_incoming_message(message)

        # All futures should resolve
        results = await asyncio.wait_for(
            asyncio.gather(future1, future2, future3), timeout=1.0
        )

        assert all(r == {"status": "success"} for r in results)


class TestRealStateManagement:
    """Tests using real state objects."""

    def test_real_state_initialization(self, tmp_path):
        """Test that connection uses real LspConnectionState."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Verify state is a real instance
        from dbt_mcp.lsp.lsp_connection import LspConnectionState

        assert isinstance(conn.state, LspConnectionState)
        assert conn.state.initialized is False
        assert conn.state.compiled is False
        assert isinstance(conn.state.pending_requests, dict)
        assert isinstance(conn.state.pending_notifications, dict)

    def test_real_request_id_generation(self, tmp_path):
        """Test real request ID counter."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Get sequential IDs
        ids = [conn.state.get_next_request_id() for _ in range(100)]

        # Verify they're sequential (starting point may vary if other tests ran)
        # Just verify they are sequential and unique
        first_id = ids[0]
        assert ids[-1] == first_id + 99
        assert ids == list(range(first_id, first_id + 100))
        assert len(set(ids)) == 100  # All unique

    def test_real_state_updates(self, tmp_path):
        """Test that state updates work with real instances."""
        binary_path = tmp_path / "lsp"
        binary_path.touch()

        conn = SocketLSPConnection(str(binary_path), "/test")

        # Update state
        conn.state.initialized = True
        conn.state.capabilities = {"test": True}
        conn.state.compiled = True

        # Verify updates persist
        assert conn.state.initialized is True
        assert conn.state.capabilities == {"test": True}
        assert conn.state.compiled is True
