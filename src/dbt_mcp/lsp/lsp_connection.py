"""LSP Connection Manager for dbt Fusion LSP.

This module manages the lifecycle of LSP processes and handles JSON-RPC
communication according to the Language Server Protocol specification.
"""

import asyncio
import itertools
import json
import logging
import socket
import subprocess
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import uuid
from dataclasses import asdict

from dbt_mcp.lsp.providers.lsp_connection_provider import (
    LSPConnectionProviderProtocol,
    LspEventName,
)

logger = logging.getLogger(__name__)


def event_name_from_string(string: str) -> LspEventName | None:
    """Create an LSP event name from a string."""
    try:
        return LspEventName(string)
    except ValueError:
        return None


@dataclass
class JsonRpcMessage:
    """Represents a JSON-RPC 2.0 message."""

    jsonrpc: str = "2.0"
    id: int | str | None = None
    method: str | None = None
    params: dict[str, Any] | list[Any] | None = None
    result: Any = None
    error: dict[str, Any] | None = None

    def to_dict(self, none_values: bool = False) -> dict[str, Any]:
        """Convert the message to a dictionary."""

        def dict_factory(x: list[tuple[str, Any]]) -> dict[str, Any]:
            return dict(x) if none_values else {k: v for k, v in x if v is not None}

        return asdict(self, dict_factory=dict_factory)


@dataclass
class LspConnectionState:
    """Tracks the state of an LSP connection."""

    initialized: bool = False
    shutting_down: bool = False
    capabilities: dict[str, Any] = field(default_factory=dict)
    pending_requests: dict[int | str, asyncio.Future] = field(default_factory=dict)
    pending_notifications: dict[LspEventName, list[asyncio.Future]] = field(
        default_factory=dict
    )
    compiled: bool = False
    # start at 20 to avoid collisions between ids of requests we are waiting for and the lsp server requests from us
    request_id_counter: Iterator[int] = field(
        default_factory=lambda: itertools.count(20)
    )

    def get_next_request_id(self) -> int:
        return next(self.request_id_counter)


class SocketLSPConnection(LSPConnectionProviderProtocol):
    """LSP process lifecycle and communication via socket.

    This class handles:
    - Starting and stopping LSP server processes
    - Socket-based JSON-RPC communication
    - Request/response correlation
    - Error handling and cleanup
    """

    def __init__(
        self,
        binary_path: str,
        cwd: str,
        args: Sequence[str] | None = None,
        connection_timeout: float = 10,
        default_request_timeout: float = 60,
    ):
        """Initialize the LSP connection manager.

        Args:
            binary_path: Path to the LSP server binary
            cwd: Working directory for the LSP process
            args: Optional command-line arguments for the LSP server
            connection_timeout: Timeout in seconds for establishing the initial socket
                              connection (default: 10). Used during server startup.
            default_request_timeout: Default timeout in seconds for LSP request operations
                                   (default: 60). Used when no timeout is specified for
                                   individual requests.
        """
        self.binary_path = Path(binary_path)
        self.args = list(args) if args else []
        self.cwd = cwd
        self.host = "127.0.0.1"
        self.port = 0

        self.process: asyncio.subprocess.Process | None = None
        self.state = LspConnectionState()

        # Socket components
        self._socket: socket.socket | None = None
        self._connection: socket.socket | None = None

        # Asyncio components for I/O
        self._reader_task: asyncio.Task | None = None
        self._writer_task: asyncio.Task | None = None
        self._stdout_reader_task: asyncio.Task | None = None
        self._stderr_reader_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._outgoing_queue: asyncio.Queue[bytes] = asyncio.Queue()

        # Timeouts
        self.connection_timeout = connection_timeout
        self.default_request_timeout = default_request_timeout

        logger.debug(f"LSP Connection initialized with binary: {self.binary_path}")

    def compiled(self) -> bool:
        return self.state.compiled

    def initialized(self) -> bool:
        return self.state.initialized

    async def start(self) -> None:
        """Start the LSP server process and socket communication tasks."""
        if self.process is not None:
            logger.warning("LSP process is already running")
            return

        try:
            self.setup_socket()

            await self.launch_lsp_process()

            # Wait for connection with timeout (run socket.accept in executor)
            if self._socket:
                self._socket.settimeout(self.connection_timeout)
                try:
                    (
                        self._connection,
                        client_addr,
                    ) = await asyncio.get_running_loop().run_in_executor(
                        None, self._socket.accept
                    )
                    if self._connection:
                        self._connection.settimeout(
                            None
                        )  # Set to blocking for read/write
                    logger.debug(f"LSP server connected from {client_addr}")
                except TimeoutError:
                    raise RuntimeError("Timeout waiting for LSP server to connect")

            # Start I/O tasks
            self._stop_event.clear()
            self._reader_task = asyncio.get_running_loop().create_task(
                self._read_loop()
            )
            self._writer_task = asyncio.get_running_loop().create_task(
                self._write_loop()
            )

        except Exception as e:
            logger.error(f"Failed to start LSP server: {e}")
            await self.stop()
            raise

    def setup_socket(self) -> None:
        """Set up the socket for LSP server communication.

        Creates a TCP socket, binds it to the configured host and port,
        and starts listening for incoming connections. If port is 0,
        the OS will auto-assign an available port.
        """
        # Create socket and bind
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, self.port))
        self._socket.listen(1)

        # Get the actual port if auto-assigned
        _, actual_port = self._socket.getsockname()
        self.port = actual_port
        logger.debug(f"Socket listening on {self.host}:{self.port}")

    async def launch_lsp_process(self) -> None:
        """Launch the LSP server process.

        Starts the LSP server as a subprocess with socket communication enabled.
        The process is started with stdout and stderr capture for monitoring.
        The server will connect back to the socket set up by setup_socket().
        """
        # Prepare command with socket info
        cmd = [
            str(self.binary_path),
            "--socket",
            f"{self.port}",
            "--project-dir",
            self.cwd,
            *self.args,
        ]

        logger.debug(f"Starting LSP server: {' '.join(cmd)}")
        self.process = await asyncio.create_subprocess_exec(*cmd)

        logger.info(f"LSP server started with PID: {self.process.pid}")

    async def stop(self) -> None:
        """Stop the LSP server process and cleanup resources."""
        logger.info("Stopping LSP server...")

        # Signal tasks to stop
        self._stop_event.set()

        # Cancel I/O tasks
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass

        if self._writer_task and not self._writer_task.done():
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass

        # Cancel stdout/stderr reader tasks
        if self._stdout_reader_task and not self._stdout_reader_task.done():
            self._stdout_reader_task.cancel()
            try:
                await self._stdout_reader_task
            except asyncio.CancelledError:
                pass

        if self._stderr_reader_task and not self._stderr_reader_task.done():
            self._stderr_reader_task.cancel()
            try:
                await self._stderr_reader_task
            except asyncio.CancelledError:
                pass

        # Send shutdown request if initialized
        if self.process and not self.state.shutting_down:
            self.state.shutting_down = True
            try:
                self._send_shutdown_request()
                await asyncio.sleep(0.5)  # Give server time to process shutdown
            except Exception as e:
                logger.warning(f"Error sending shutdown request: {e}")

        # Close socket connection
        if self._connection:
            try:
                self._connection.close()
            except Exception as e:
                logger.error(f"Error closing socket connection: {e}")
            finally:
                self._connection = None

        # Close listening socket
        if self._socket:
            try:
                self._socket.close()
            except Exception as e:
                logger.error(f"Error closing socket: {e}")
            finally:
                self._socket = None

        # Terminate the process
        if self.process:
            try:
                self.process.terminate()
                try:
                    await self.process.wait()
                except subprocess.TimeoutExpired:
                    logger.warning("LSP process didn't terminate, killing...")
                    self.process.kill()
                    await self.process.wait()
            except Exception as e:
                logger.error(f"Error terminating LSP process: {e}")
            finally:
                self.process = None

        # Clear state
        self.state = LspConnectionState()

        logger.info("LSP server stopped")

    async def initialize(self, timeout: float | None = None) -> None:
        """Initialize the LSP connection.

        Sends the initialize request to the LSP server and waits for the response.
        The server capabilities are stored in the connection state.

        Args:
            root_uri: The root URI of the workspace (optional)
            timeout: Timeout in seconds for the initialize request (default: 10)
        """
        if self.state.initialized:
            raise RuntimeError("LSP server is already initialized")

        params = {
            "processId": None,
            "rootUri": None,
            "clientInfo": {
                "name": "dbt-mcp",
                "version": "1.0.0",
            },
            "capabilities": {},
            "initializationOptions": {
                "project-dir": "file:///",
                "command-prefix": str(uuid.uuid4()),
            },
        }

        # Send initialize request
        result = await self.send_request(
            "initialize", params, timeout=timeout or self.default_request_timeout
        )

        # Store capabilities
        self.state.capabilities = result.get("capabilities", {})
        self.state.initialized = True

        # Send initialized notification
        self.send_notification("initialized", {})

        logger.info("LSP server initialized successfully")

    async def _read_loop(self) -> None:
        """Background task that reads messages from the LSP server via socket."""
        if not self._connection:
            logger.warning("LSP server socket is not available")
            return

        buffer = b""

        while not self._stop_event.is_set():
            try:
                # Read data from socket (run in executor to avoid blocking)
                self._connection.settimeout(0.1)  # Short timeout to check stop event
                try:
                    chunk = await asyncio.get_running_loop().run_in_executor(
                        None, self._connection.recv, 4096
                    )
                except TimeoutError:
                    continue

                if not chunk:
                    logger.warning("LSP server socket closed")
                    break

                buffer += chunk

                # Try to parse messages from buffer
                while True:
                    message, remaining = self._parse_message(buffer)
                    if message is None:
                        break

                    buffer = remaining

                    # Process the message
                    self._handle_incoming_message(message)

            except asyncio.CancelledError:
                # Task was cancelled, exit cleanly
                break
            except Exception as e:
                if not self._stop_event.is_set():
                    logger.error(f"Error in reader task: {e}")
                break

    async def _write_loop(self) -> None:
        """Background task that writes messages to the LSP server via socket."""
        if not self._connection:
            return

        while not self._stop_event.is_set():
            try:
                # Get message from queue (with timeout to check stop event)
                try:
                    data = await asyncio.wait_for(
                        self._outgoing_queue.get(), timeout=0.1
                    )
                except TimeoutError:
                    continue

                # Write to socket (run in executor to avoid blocking)
                await asyncio.get_running_loop().run_in_executor(
                    None, self._connection.sendall, data
                )

            except asyncio.CancelledError:
                # Task was cancelled, exit cleanly
                break
            except Exception as e:
                if not self._stop_event.is_set():
                    logger.error(f"Error in writer task: {e}")
                break

    def _parse_message(self, buffer: bytes) -> tuple[JsonRpcMessage | None, bytes]:
        """Parse a JSON-RPC message from the buffer.

        LSP uses HTTP-like headers followed by JSON content:
        Content-Length: <length>\\r\\n
        \\r\\n
        <json-content>
        """
        # Look for Content-Length header
        header_end = buffer.find(b"\r\n\r\n")
        if header_end == -1:
            return None, buffer

        # Parse headers
        headers = buffer[:header_end].decode("utf-8")
        content_length = None

        for line in headers.split("\r\n"):
            if line.startswith("Content-Length:"):
                try:
                    content_length = int(line.split(":")[1].strip())
                except (IndexError, ValueError):
                    logger.error(f"Invalid Content-Length header: {line}")
                    return None, buffer[header_end + 4 :]

        if content_length is None:
            logger.error("Missing Content-Length header")
            return None, buffer[header_end + 4 :]

        # Check if we have the full message
        content_start = header_end + 4
        content_end = content_start + content_length

        if len(buffer) < content_end:
            return None, buffer

        # Parse JSON content
        try:
            content = buffer[content_start:content_end].decode("utf-8")
            data = json.loads(content)
            message = JsonRpcMessage(**data)

            return message, buffer[content_end:]

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to parse message: {e}")
            return None, buffer[content_end:]

    def _handle_incoming_message(self, message: JsonRpcMessage) -> None:
        """Handle an incoming message from the LSP server."""

        # Handle responses to requests
        if message.id is not None:
            # Thread-safe: pop with default avoids race condition between check and pop
            future = self.state.pending_requests.pop(message.id, None)
            if future is not None:
                logger.debug(f"Received response for request {message.to_dict()}")

                # Use call_soon_threadsafe to safely resolve futures across event loop contexts
                # This prevents "Task got Future attached to a different loop" errors when
                # the future was created in one loop but is being resolved from another loop
                # Get the loop from the future itself to ensure we schedule on the correct loop
                future_loop = future.get_loop()
                if message.error:
                    future_loop.call_soon_threadsafe(
                        future.set_exception,
                        RuntimeError(f"LSP error: {message.error}"),
                    )
                else:
                    future_loop.call_soon_threadsafe(future.set_result, message.result)
                return
            else:
                # it's an unknown request, we respond with an empty result
                logger.debug(f"LSP request {message.to_dict()}")
                self._send_message(
                    JsonRpcMessage(id=message.id, result=None), none_values=True
                )

        if message.method is None:
            return

        # it's a known event type we want to explicitly handle
        if lsp_event_name := event_name_from_string(message.method):
            # Check if this is an event we're waiting for
            # Thread-safe: pop with default avoids race condition
            futures = self.state.pending_notifications.pop(lsp_event_name, None)
            if futures is not None:
                logger.debug(f"Received event {lsp_event_name} - {message.to_dict()}")
                # Use call_soon_threadsafe for notification futures as well
                for future in futures:
                    future_loop = future.get_loop()
                    future_loop.call_soon_threadsafe(future.set_result, message.params)

            match lsp_event_name:
                case LspEventName.compileComplete:
                    logger.info("Recorded compile complete event")
                    self.state.compiled = True
                case _:
                    logger.debug(f"LSP event {message.method}")
                    pass

        else:
            # it's an unknown notification, log it and move on
            logger.debug(f"LSP event {message.method}")

    async def send_request(
        self,
        method: str,
        params: dict[str, Any] | list[Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Send a request to the LSP server.

        Args:
            method: The JSON-RPC method name
            params: Optional parameters for the method
            timeout: Timeout in seconds for this request. If not specified, uses
                    default_request_timeout from the connection configuration.

        Returns:
            A dictionary containing the response result or error information
        """
        if not self.process:
            raise RuntimeError("LSP server is not running")

        # Create request message
        request_id = self.state.get_next_request_id()
        message = JsonRpcMessage(
            id=request_id,
            method=method,
            params=params,
        )

        # Create future for response using the current running loop
        # This prevents "Task got Future attached to a different loop" errors
        # when send_request is called from a different loop context than where
        # the connection was initialized
        future = asyncio.get_running_loop().create_future()
        self.state.pending_requests[request_id] = future

        # Send the message
        self._send_message(message)

        try:
            return await asyncio.wait_for(
                future, timeout=timeout or self.default_request_timeout
            )
        except Exception as e:
            return {"error": str(e)}

    def send_notification(
        self,
        method: str,
        params: dict[str, Any] | list[Any] | None = None,
    ) -> None:
        """Send a notification to the LSP server.

        Args:
            method: The JSON-RPC method name
            params: Optional parameters for the method
        """
        if not self.process:
            raise RuntimeError("LSP server is not running")

        # Create notification message (no ID)
        message = JsonRpcMessage(
            method=method,
            params=params,
        )

        # Send the message
        self._send_message(message)

    def wait_for_notification(
        self, event_name: LspEventName
    ) -> asyncio.Future[dict[str, Any]]:
        """Wait for a notification from the LSP server.

        Args:
            event_name: The LSP event name to wait for

        Returns:
            A Future that will be resolved with the notification params when received
        """
        future = asyncio.get_running_loop().create_future()
        self.state.pending_notifications.setdefault(event_name, []).append(future)

        return future

    def _send_message(self, message: JsonRpcMessage, none_values: bool = False) -> None:
        """Send a message to the LSP server."""
        # Serialize message
        content = json.dumps(message.to_dict(none_values=none_values))
        content_bytes = content.encode("utf-8")

        # Create LSP message with headers
        header = f"Content-Length: {len(content_bytes)}\r\n\r\n"
        header_bytes = header.encode("utf-8")

        data = header_bytes + content_bytes

        logger.debug(f"Sending message: {content}")

        # Queue for sending (put_nowait is safe from sync context)
        self._outgoing_queue.put_nowait(data)

    def _send_shutdown_request(self) -> None:
        """Send shutdown request to the LSP server."""
        try:
            # Send shutdown request
            message = JsonRpcMessage(
                id=self.state.get_next_request_id(),
                method="shutdown",
            )
            self._send_message(message)

            # Send exit notification
            exit_message = JsonRpcMessage(
                method="exit",
            )
            self._send_message(exit_message)

        except Exception as e:
            logger.error(f"Error sending shutdown: {e}")

    def is_running(self) -> bool:
        """Check if the LSP server is running."""
        return self.process is not None and self.process.returncode is None
