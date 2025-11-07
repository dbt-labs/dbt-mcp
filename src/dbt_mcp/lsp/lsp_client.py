"""LSP Client for dbt Fusion.

This module provides a high-level client interface for interacting with the
dbt Fusion LSP server, wrapping low-level JSON-RPC communication with
typed methods for dbt-specific operations.
"""

import asyncio
import logging
from typing import Any

from dbt_mcp.lsp.lsp_connection import LspEventName
from dbt_mcp.lsp.providers.lsp_connection_provider import (
    LSPConnectionProviderProtocol,
)
from dbt_mcp.lsp.providers.lsp_client_provider import LSPClientProtocol


logger = logging.getLogger(__name__)

# Default timeout for LSP operations (in seconds)
DEFAULT_LSP_TIMEOUT = 30


class LSPClient(LSPClientProtocol):
    """High-level client for dbt Fusion LSP operations.

    This class provides typed methods for dbt-specific LSP operations
    such as column lineage, model references, and more.
    """

    def __init__(
        self,
        lsp_connection: LSPConnectionProviderProtocol,
        timeout: float | None = None,
    ):
        """Initialize the dbt LSP client.

        Args:
            lsp_connection: The LSP connection to use
            timeout: Default timeout for LSP operations in seconds. If not specified,
                    uses DEFAULT_LSP_TIMEOUT (30 seconds).
        """
        self.lsp_connection = lsp_connection
        self.timeout = timeout if timeout is not None else DEFAULT_LSP_TIMEOUT

    async def compile(self, timeout: float | None = None) -> dict[str, Any]:
        """Compile the dbt project.

        Returns the compilation log as dictionary.
        """
        # Register for the notification BEFORE sending the command to avoid race conditions
        compile_complete_future = self.lsp_connection.wait_for_notification(
            LspEventName.compileComplete
        )

        async with asyncio.timeout(timeout or self.timeout):
            await self.lsp_connection.send_request(
                "workspace/executeCommand",
                {"command": "dbt.compileLsp", "arguments": []},
            )

            # wait for complation to complete
            result = await compile_complete_future

            if "error" in result and result["error"] is not None:
                return {"error": result["error"]}

            if "log" in result and result["log"] is not None:
                return {"log": result["log"]}

            return result

    async def get_column_lineage(
        self,
        model_id: str,
        column_name: str,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Get column lineage information for a specific model column.

        Args:
            model_id: The dbt model identifier
            column_name: The column name to trace lineage for

        Returns:
            Dictionary containing lineage information with 'nodes' key
        """

        if not self.lsp_connection.compiled():
            await self.compile()

        logger.info(f"Requesting column lineage for {model_id}.{column_name}")

        selector = f"+column:{model_id}.{column_name.upper()}+"
        async with asyncio.timeout(timeout or self.timeout):
            result = await self.lsp_connection.send_request(
                "workspace/executeCommand",
                {"command": "dbt.listNodes", "arguments": [selector]},
            )
            if not result:
                return {"error": "No result from LSP"}

            if "error" in result and result["error"] is not None:
                return {"error": result["error"]}

            if "nodes" in result and result["nodes"] is not None:
                return {"nodes": result["nodes"]}

            return result

    async def get_model_lineage(
        self, model_selector: str, timeout: float | None = None
    ) -> dict[str, Any]:
        nodes = []
        response = await self._list_nodes(model_selector)

        if not response:
            return {"error": "No result from LSP"}

        if "error" in response and response["error"] is not None:
            return {"error": response["error"]}

        if "nodes" in response and response["nodes"] is not None:
            for node in response["nodes"]:
                nodes.append(
                    {
                        "depends_on": node["depends_on"],
                        "name": node["name"],
                        "unique_id": node["unique_id"],
                        "path": node["path"],
                    }
                )

        return {"nodes": nodes}

    async def _list_nodes(
        self, model_selector: str, timeout: float | None = None
    ) -> dict[str, Any]:
        """List nodes in the dbt project."""

        if not self.lsp_connection.compiled():
            await self.compile()

        logger.info("Listing nodes", extra={"model_selector": model_selector})
        async with asyncio.timeout(timeout or self.timeout):
            result = await self.lsp_connection.send_request(
                "workspace/executeCommand",
                {"command": "dbt.listNodes", "arguments": [model_selector]},
            )

            if not result:
                return {"error": "No result from LSP"}

            if "error" in result and result["error"] is not None:
                return {"error": result["error"]}

            if "nodes" in result and result["nodes"] is not None:
                return {"nodes": result["nodes"]}

            return result
