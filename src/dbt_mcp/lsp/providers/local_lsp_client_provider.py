"""Local LSP Client Provider Implementation.

This module provides the concrete implementation of LSPClientProvider for
local LSP connections (as opposed to remote or pooled connections).

Design:
- Each call to get_client() creates a NEW LSPClient instance
- However, all clients share the SAME underlying connection (via the connection provider)
- This allows multiple concurrent operations while maintaining a single LSP server process
- The connection is lazily initialized when first needed
"""

from dbt_mcp.lsp.providers.lsp_client_provider import (
    LSPClientProvider,
    LSPClientProtocol,
)
from dbt_mcp.lsp.providers.lsp_connection_provider import LSPConnectionProvider
from dbt_mcp.lsp.lsp_client import LSPClient


class LocalLSPClientProvider(LSPClientProvider):
    """Provider for creating LSPClient instances with a local LSP server connection.

    This provider creates fresh LSPClient instances on each call, but all clients
    share the same underlying connection managed by the connection provider.

    Attributes:
        lsp_connection_provider: Provider managing the underlying socket connection
        timeout: Default timeout in seconds for LSP operations (or None for default)
    """

    def __init__(
        self,
        lsp_connection_provider: LSPConnectionProvider,
        timeout: float | None = None,
    ):
        """Initialize the local LSP client provider.

        Args:
            lsp_connection_provider: Provider that manages the LSP socket connection
            timeout: Optional default timeout for LSP operations
        """
        self.lsp_connection_provider = lsp_connection_provider
        self.timeout = timeout

    async def get_client(self, timeout: float | None = None) -> LSPClientProtocol:
        """Create a new LSPClient instance using the managed connection.

        Creates a new LSPClient instance that wraps the shared connection.
        The connection is obtained from the connection provider, which handles
        lazy initialization and singleton behavior.

        Returns:
            A new LSPClient instance configured with the shared connection

        Raises:
            RuntimeError: If the connection provider fails to establish a connection
        """
        # Get or create the shared connection (lazy initialization)
        # The connection provider ensures only one connection is created
        connection = await self.lsp_connection_provider.get_connection()

        # Create a new client instance wrapping the shared connection
        # Multiple clients can coexist, all using the same underlying connection
        return LSPClient(
            lsp_connection=connection,
            timeout=timeout or self.timeout,
        )
