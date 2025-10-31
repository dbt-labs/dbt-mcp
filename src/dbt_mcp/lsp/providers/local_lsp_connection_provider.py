"""Local LSP Connection Provider Implementation.

This module provides the concrete implementation of LSPConnectionProvider for
managing socket-based connections to a local LSP server process.
"""

import logging


from dbt_mcp.lsp.lsp_binary_manager import LspBinaryInfo
from dbt_mcp.lsp.lsp_connection import SocketLSPConnection
from dbt_mcp.lsp.providers.lsp_connection_provider import (
    LSPConnectionProvider,
    LSPConnectionProviderProtocol,
)

logger = logging.getLogger(__name__)


class LocalLSPConnectionProvider(LSPConnectionProvider):
    """Provider for managing a local LSP server connection.

    This provider implements a singleton pattern for LSP connections:
    - Only one connection is created and reused across all requests
    - Connection is lazily initialized on first use
    - Connection lifecycle is tied to the MCP server lifecycle

    The provider is owned by the DbtMCP server instance and is cleaned up
    during server shutdown, ensuring no orphaned LSP server processes.

    Attributes:
        lsp_connection: The singleton connection instance (None until first use)
        lsp_binary_info: Information about the LSP binary (path and version)
        project_dir: dbt project directory for the LSP server
    """

    def __init__(self, lsp_binary_info: LspBinaryInfo, project_dir: str):
        """Initialize the local LSP connection provider.

        Note: This does NOT create the connection - it only stores the
        configuration needed to create it later (lazy initialization).

        Args:
            lsp_binary_info: Information about the LSP binary to use
            project_dir: Path to the dbt project directory
        """
        self.lsp_connection: LSPConnectionProviderProtocol | None = None
        self.lsp_binary_info = lsp_binary_info
        self.project_dir = project_dir

    async def _new_connection(self) -> LSPConnectionProviderProtocol:
        """Create and initialize a new LSP connection.

        This is an internal method that handles the actual connection creation
        and initialization sequence:
        1. Create SocketLSPConnection with binary info
        2. Start the LSP server process and establish socket connection
        3. Send LSP initialize request and wait for server capabilities

        Returns:
            A fully initialized LSP connection

        Raises:
            RuntimeError: If connection creation or initialization fails
        """
        # Defensive check: This shouldn't happen due to get_connection() logic,
        # but included for clarity and safety
        if self.lsp_connection is not None:
            return self.lsp_connection

        try:
            logger.info(
                f"Using LSP binary in {self.lsp_binary_info.path} with version {self.lsp_binary_info.version}"
            )

            # Create the connection wrapper (doesn't start the process yet)
            lsp_connection = SocketLSPConnection(
                binary_path=self.lsp_binary_info.path,
                args=[],
                cwd=self.project_dir,
            )

            # Start the LSP server process and establish socket connection
            # This is when the actual subprocess is spawned
            await lsp_connection.start()
            logger.info("LSP connection started successfully")

            # Send the LSP initialize request to exchange capabilities
            # The server won't accept other requests until this completes
            await lsp_connection.initialize()
            logger.info("LSP connection initialized successfully")

            return lsp_connection
        except Exception as e:
            logger.error(f"Error starting LSP connection: {e}")
            # Clean up any partial state to ensure clean retry
            self.lsp_connection = None
            raise RuntimeError("Error: Failed to establish LSP connection")

    async def get_connection(self) -> LSPConnectionProviderProtocol:
        """Get the LSP connection, creating it if needed (lazy initialization).

        This implements the singleton pattern:
        - First call: Creates, starts, and initializes the connection
        - Subsequent calls: Returns the existing connection

        The connection is only created when actually needed (typically when
        a tool is first invoked), not during provider or server initialization.

        Returns:
            The singleton LSP connection instance

        Raises:
            RuntimeError: If connection creation fails
        """
        if self.lsp_connection is None:
            # Lazy initialization: Create the connection on first use
            # This happens AFTER the MCP server is listening for requests
            self.lsp_connection = await self._new_connection()
        return self.lsp_connection

    async def cleanup_connection(self) -> None:
        """Cleanup and stop the LSP connection if it exists.

        This method is called during MCP server shutdown via app_lifespan.
        It gracefully stops the LSP server process and cleans up resources.

        The method is idempotent and safe to call multiple times or when
        no connection was ever created (handles None case).

        Note: Exceptions during cleanup are caught and logged but not re-raised
        to ensure shutdown proceeds even if cleanup fails.
        """
        if self.lsp_connection:
            try:
                logger.info("Cleaning up LSP connection")
                # Stop the LSP server process and close socket
                await self.lsp_connection.stop()
            except Exception as e:
                # Log but don't re-raise - we want shutdown to continue
                logger.error(f"Error cleaning up LSP connection: {e}")
            finally:
                # Always clear the reference, even if stop() failed
                # This ensures we don't try to reuse a failed connection
                self.lsp_connection = None
