"""LSP Connection Provider Protocols for dbt Fusion LSP.

This module defines the protocols for managing LSP server connections using
the Language Server Protocol (LSP) over sockets.

The provider pattern enables:
1. Lazy connection initialization (only connect when needed)
2. Connection lifecycle management (start, stop, cleanup)
3. Singleton behavior (reuse existing connections)
4. Clean separation between connection management and client operations
"""

import asyncio
from enum import Enum
from typing import Any, Protocol

import logging

logger = logging.getLogger(__name__)


class LspEventName(str, Enum):
    """LSP notification event names.

    These events are sent from the LSP server to the client as notifications
    (no response expected) to inform about state changes and progress.
    """

    compileComplete = "dbt/lspCompileComplete"
    logMessage = "window/logMessage"
    progress = "$/progress"
    workspaceDiagnostics = "workspace/diagnostics"
    fileDiagnostics = "textDocument/publishDiagnostics"


class LSPConnectionProviderProtocol(Protocol):
    """Protocol defining the interface for LSP connection objects.

    This protocol represents a low-level connection to an LSP server process,
    handling socket communication, process lifecycle, and JSON-RPC messaging.

    Implementations typically wrap subprocess management and async socket I/O.
    """

    async def start(self) -> None:
        """Start the LSP server process and establish socket connection."""
        ...

    async def stop(self) -> None:
        """Stop the LSP server process and cleanup resources."""
        ...

    async def initialize(self, timeout: float | None = None) -> None:
        """Send LSP initialize request and wait for server capabilities."""
        ...

    def compiled(self) -> bool:
        """Check if the dbt project has been compiled via LSP."""
        ...

    def initialized(self) -> bool:
        """Check if the LSP connection has been initialized."""
        ...

    async def send_request(
        self,
        method: str,
        params: dict[str, Any] | list[Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Send a JSON-RPC request and wait for response."""
        ...

    def send_notification(
        self, method: str, params: dict[str, Any] | list[Any] | None = None
    ) -> None:
        """Send a JSON-RPC notification (no response expected)."""
        ...

    def wait_for_notification(
        self, event_name: LspEventName
    ) -> asyncio.Future[dict[str, Any]]:
        """Create a future that resolves when a specific notification arrives."""
        ...

    def is_running(self) -> bool:
        """Check if the LSP server process is currently running."""
        ...


class LSPConnectionProvider(Protocol):
    """Protocol for objects that provide LSP connection instances.

    This is the provider protocol for connection management. It handles:
    - Lazy connection initialization
    - Connection singleton behavior (reuse existing connections)
    - Cleanup and lifecycle management
    """

    async def get_connection(self) -> LSPConnectionProviderProtocol:
        """Get or create an LSP connection instance.

        This method implements lazy initialization - the connection is only
        established when first requested, which typically happens after the
        MCP server is already listening for connections.

        Returns:
            An object implementing LSPConnectionProviderProtocol (typically SocketLSPConnection)
        """
        ...

    async def cleanup_connection(self) -> None:
        """Cleanup and close the LSP connection if it exists.

        This method is called during server shutdown to gracefully stop
        the LSP server process and cleanup resources. It should handle
        the case where no connection was ever established.
        """
        ...
