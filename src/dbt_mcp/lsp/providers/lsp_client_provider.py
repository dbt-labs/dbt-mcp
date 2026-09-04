"""LSP Client Provider Protocols for dbt Fusion LSP.

This module defines the protocols for LSP client management using dependency injection.

Protocol Naming Convention:
- LSPClientProtocol: Defines the interface for LSPClient objects (the actual client)
- LSPClientProvider: Defines the interface for provider objects that create LSPClient instances

This separation allows for:
1. Testing by mocking either the client or the provider
2. Different provider implementations (local, remote, pooled, etc.)
3. Lazy initialization of expensive LSP connections
"""

import logging
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class LSPClientProtocol(Protocol):
    """Protocol defining the interface for LSP client objects.

    This protocol matches the LSPClient class interface, allowing tools to
    depend on the protocol rather than concrete implementation.

    Note: Despite the name containing "Provider", this is actually the protocol
    for the CLIENT itself, not the provider. It defines the operations that
    LSP client implementations must support.
    """

    async def compile(self, timeout: float | None = None) -> dict[str, Any]:
        """Compile the dbt project via LSP."""
        ...

    async def get_column_lineage(
        self, model_id: str, column_name: str, timeout: float | None = None
    ) -> dict[str, Any]:
        """Get column-level lineage information."""
        ...

    async def get_model_lineage(
        self, model_selector: str, timeout: float | None = None
    ) -> dict[str, Any]:
        """Get model-level lineage information."""
        ...

    async def get_project_info(self, timeout: float | None = None) -> dict[str, Any]:
        """Return dbt project metadata."""
        ...

    async def get_node(self, path: str, timeout: float | None = None) -> dict[str, Any]:
        """Return the node associated with a project file."""
        ...

    async def get_diagnostics(
        self, path: str, timeout: float | None = None
    ) -> dict[str, Any]:
        """Return compiler diagnostics for a project file."""
        ...

    async def get_definition(
        self, path: str, line: int, character: int, timeout: float | None = None
    ) -> dict[str, Any]:
        """Resolve a symbol definition."""
        ...

    async def get_references(
        self,
        path: str,
        line: int,
        character: int,
        include_declaration: bool = True,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Find references to a symbol."""
        ...

    async def get_code_actions(
        self,
        path: str,
        start_line: int,
        start_character: int,
        end_line: int,
        end_character: int,
        only: list[str] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Return safe code actions."""
        ...

    async def get_rename_preview(
        self,
        path: str,
        line: int,
        character: int,
        new_name: str,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Return a rename workspace edit without applying it."""
        ...

    async def compile_file(
        self, path: str, timeout: float | None = None
    ) -> dict[str, Any]:
        """Compile one project file."""
        ...

    async def update_document(
        self,
        path: str,
        text: str,
        wait_for_compile: bool = True,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Update an in-memory project document incrementally."""
        ...

    async def close_document(self, path: str) -> dict[str, Any]:
        """Close an in-memory project document."""
        ...


class LSPClientProvider(Protocol):
    """Protocol for objects that provide LSPClient instances.

    This is the actual "provider" protocol - it defines how to obtain
    an LSPClient instance. Implementations can handle connection pooling,
    lazy initialization, lifecycle management, etc.
    """

    async def get_client(self) -> LSPClientProtocol:
        """Get or create an LSPClient instance.

        Returns:
            An object implementing LSPClientProtocol (typically LSPClient)
        """
        ...
