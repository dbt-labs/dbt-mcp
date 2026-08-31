"""LSP Client for dbt Fusion.

This module provides a high-level client interface for interacting with the
dbt Fusion LSP server, wrapping low-level JSON-RPC communication with
typed methods for dbt-specific operations.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import url2pathname

from dbt_mcp.errors import InvalidParameterError
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
        project_dir: str | None = None,
    ):
        """Initialize the dbt LSP client.

        Args:
            lsp_connection: The LSP connection to use
            timeout: Default timeout for LSP operations in seconds. If not specified,
                    uses DEFAULT_LSP_TIMEOUT (30 seconds).
        """
        self.lsp_connection = lsp_connection
        self.timeout = timeout if timeout is not None else DEFAULT_LSP_TIMEOUT
        self.project_dir = Path(project_dir).resolve() if project_dir else None

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

            if not isinstance(result, dict):
                return {"result": result}

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

        Raises:
            ValueError: If model_id or column_name is empty or invalid
        """
        if not isinstance(model_id, str) or not model_id:
            raise InvalidParameterError(
                f"model_id must be a non-empty string, got: {model_id!r}"
            )
        if not isinstance(column_name, str) or not column_name:
            raise InvalidParameterError(
                f"column_name must be a non-empty string, got: {column_name!r}"
            )

        if not self.lsp_connection.compiled():
            await self.compile()

        logger.info(f"Requesting column lineage for {model_id}.{column_name}")

        model_selector = f"@{model_id}"
        column_selector = f"+column:{model_id}.{column_name.upper()}+"
        async with asyncio.timeout(timeout or self.timeout):
            result = await self.lsp_connection.send_request(
                "workspace/executeCommand",
                {
                    "command": "dbt.listNodes",
                    "arguments": [model_selector, column_selector],
                },
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

    def _path(self, path: str) -> Path:
        """Resolve an agent-supplied path against the dbt project."""
        candidate = Path(path)
        if self.project_dir is None:
            raise InvalidParameterError(
                "A project directory is required for path-based LSP operations"
            )
        if not candidate.is_absolute():
            candidate = self.project_dir / candidate
        candidate = candidate.resolve()
        try:
            candidate.relative_to(self.project_dir)
        except ValueError as exc:
            raise InvalidParameterError(
                f"path must resolve inside the dbt project directory: {path!r}"
            ) from exc
        return candidate

    def _uri(self, path: str) -> str:
        return self._path(path).as_uri()

    def _relative_path(self, path: str) -> str:
        candidate = self._path(path)
        if self.project_dir is None:
            raise InvalidParameterError(
                "A project directory is required for path-based LSP operations"
            )
        return candidate.relative_to(self.project_dir).as_posix()

    @staticmethod
    def _as_dict_result(result: Any) -> dict[str, Any]:
        return result if isinstance(result, dict) else {"result": result}

    @staticmethod
    def _as_list_result(result: Any, key: str) -> dict[str, Any]:
        if isinstance(result, list):
            return {key: result}
        if result is None:
            return {key: []}
        return result if isinstance(result, dict) else {"result": result}

    async def _ensure_compiled(
        self, timeout: float | None = None
    ) -> dict[str, Any] | None:
        if self.lsp_connection.compiled():
            return None
        result = await self.compile(timeout=timeout)
        return result if "error" in result else None

    async def get_project_info(self, timeout: float | None = None) -> dict[str, Any]:
        """Return project metadata from the compiler-backed LSP."""
        compile_error = await self._ensure_compiled(timeout)
        if compile_error:
            return compile_error
        async with asyncio.timeout(timeout or self.timeout):
            result = await self.lsp_connection.send_request(
                "workspace/executeCommand",
                {"command": "dbt.getProjectInfo", "arguments": []},
            )
            return self._as_dict_result(result)

    async def get_node(self, path: str, timeout: float | None = None) -> dict[str, Any]:
        """Return the dbt node associated with a model file."""
        if not isinstance(path, str) or not path:
            raise InvalidParameterError("path must be a non-empty string")
        compile_error = await self._ensure_compiled(timeout)
        if compile_error:
            return compile_error
        async with asyncio.timeout(timeout or self.timeout):
            result = await self.lsp_connection.send_request(
                "workspace/executeCommand",
                {
                    "command": "dbt.getCurrentNode",
                    "arguments": [self._relative_path(path)],
                },
            )
            return self._as_dict_result(result)

    async def get_diagnostics(
        self, path: str, timeout: float | None = None
    ) -> dict[str, Any]:
        """Return current compiler diagnostics for a model file."""
        compile_error = await self._ensure_compiled(timeout)
        if compile_error:
            return compile_error
        async with asyncio.timeout(timeout or self.timeout):
            result = await self.lsp_connection.send_request(
                "textDocument/diagnostic",
                {"textDocument": {"uri": self._uri(path)}},
            )
        if isinstance(result, list):
            return {"items": result}
        if isinstance(result, dict):
            if result.get("error") is not None:
                return {"error": result["error"]}
            return {
                "items": result.get("items", []),
                "result_id": result.get("resultId"),
                "report": result,
            }
        return {"items": [], "report": result}

    async def get_definition(
        self,
        path: str,
        line: int,
        character: int,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Resolve the symbol at a position to its dbt definition."""
        compile_error = await self._ensure_compiled(timeout)
        if compile_error:
            return compile_error
        async with asyncio.timeout(timeout or self.timeout):
            result = await self.lsp_connection.send_request(
                "textDocument/definition",
                {
                    "textDocument": {"uri": self._uri(path)},
                    "position": {"line": line, "character": character},
                },
            )
        return self._as_list_result(result, "locations")

    async def get_references(
        self,
        path: str,
        line: int,
        character: int,
        include_declaration: bool = True,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Find references to the symbol at a position."""
        compile_error = await self._ensure_compiled(timeout)
        if compile_error:
            return compile_error
        async with asyncio.timeout(timeout or self.timeout):
            result = await self.lsp_connection.send_request(
                "textDocument/references",
                {
                    "textDocument": {"uri": self._uri(path)},
                    "position": {"line": line, "character": character},
                    "context": {"includeDeclaration": include_declaration},
                },
            )
        return self._as_list_result(result, "locations")

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
        """Return safe LSP code actions without applying them."""
        diagnostics = await self.get_diagnostics(path, timeout=timeout)
        if diagnostics.get("error"):
            return diagnostics
        context: dict[str, Any] = {"diagnostics": diagnostics.get("items", [])}
        if only:
            context["only"] = only
        params = {
            "textDocument": {"uri": self._uri(path)},
            "range": {
                "start": {"line": start_line, "character": start_character},
                "end": {"line": end_line, "character": end_character},
            },
            "context": context,
        }
        async with asyncio.timeout(timeout or self.timeout):
            result = await self.lsp_connection.send_request(
                "textDocument/codeAction", params
            )
        return self._as_list_result(result, "actions")

    async def get_rename_preview(
        self,
        path: str,
        line: int,
        character: int,
        new_name: str,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Return the workspace edit for a rename without writing files."""
        if not new_name:
            raise InvalidParameterError("new_name must be a non-empty string")
        compile_error = await self._ensure_compiled(timeout)
        if compile_error:
            return compile_error
        uri = self._uri(path)
        position = {"line": line, "character": character}
        async with asyncio.timeout(timeout or self.timeout):
            prepare = await self.lsp_connection.send_request(
                "textDocument/prepareRename",
                {"textDocument": {"uri": uri}, "position": position},
            )
            if isinstance(prepare, dict) and "error" in prepare:
                return prepare
            if prepare is None:
                return {"prepare": None, "edit": None}
            edit = await self.lsp_connection.send_request(
                "textDocument/rename",
                {
                    "textDocument": {"uri": uri},
                    "position": position,
                    "newName": new_name,
                },
            )
        return {"prepare": prepare, "edit": edit}

    async def compile_file(
        self, path: str, timeout: float | None = None
    ) -> dict[str, Any]:
        """Compile one file and return its generated SQL, when available."""
        uri = self._uri(path)
        async with asyncio.timeout(timeout or self.timeout):
            result = await self.lsp_connection.send_request(
                "workspace/executeCommand",
                {"command": "dbt.compileFile", "arguments": [uri]},
            )
        if not isinstance(result, dict):
            return {"result": result}
        if result.get("error"):
            return result
        compiled_uri = result.get("file_uri") or result.get("compiled_uri")
        response: dict[str, Any] = {"file_uri": compiled_uri}
        if compiled_uri:
            parsed = urlparse(compiled_uri)
            if parsed.scheme == "file":
                compiled_path = Path(url2pathname(parsed.path))
                if compiled_path.exists():
                    response["compiled_sql"] = compiled_path.read_text()
        return response

    async def update_document(
        self,
        path: str,
        text: str,
        wait_for_compile: bool = True,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Send a full-text document update and wait for incremental compilation."""
        uri = self._uri(path)
        if self.lsp_connection.is_document_open(uri):
            version = (self.lsp_connection.document_version(uri) or 0) + 1
            method = "textDocument/didChange"
            params: dict[str, Any] = {
                "textDocument": {"uri": uri, "version": version},
                "contentChanges": [{"text": text}],
            }
        else:
            version = 1
            method = "textDocument/didOpen"
            params = {
                "textDocument": {
                    "uri": uri,
                    "languageId": (
                        "yaml"
                        if Path(path).suffix.lower() in {".yml", ".yaml"}
                        else "sql"
                    ),
                    "version": version,
                    "text": text,
                }
            }

        compile_future = None
        if wait_for_compile:
            compile_future = self.lsp_connection.wait_for_notification(
                LspEventName.backgroundCompileComplete
            )
        self.lsp_connection.send_notification(method, params)
        result: dict[str, Any] = {
            "uri": uri,
            "version": version,
            "method": method,
        }
        if compile_future is not None:
            try:
                result["compile"] = await asyncio.wait_for(
                    compile_future, timeout=timeout or self.timeout
                )
            except TimeoutError:
                result["compile"] = {
                    "error": "Timed out waiting for incremental compile"
                }
        return result

    async def close_document(self, path: str) -> dict[str, Any]:
        """Close a document held in the LSP's in-memory workspace."""
        uri = self._uri(path)
        self.lsp_connection.send_notification(
            "textDocument/didClose", {"textDocument": {"uri": uri}}
        )
        return {"uri": uri, "closed": True}
