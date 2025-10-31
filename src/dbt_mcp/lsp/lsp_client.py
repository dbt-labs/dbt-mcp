"""LSP Client for dbt Fusion.

This module provides a high-level client interface for interacting with the
dbt Fusion LSP server, wrapping low-level JSON-RPC communication with
typed methods for dbt-specific operations.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from dbt_mcp.lsp.lsp_connection import LSPConnection, LspEventName

logger = logging.getLogger(__name__)

# Default timeout for LSP operations (in seconds)
DEFAULT_LSP_TIMEOUT = 30


class LSPClient:
    """High-level client for dbt Fusion LSP operations.

    This class provides typed methods for dbt-specific LSP operations
    such as column lineage, model references, and more.
    """

    def __init__(self, lsp_connection: LSPConnection, timeout: float | None = None):
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

        if not self.lsp_connection.state.compiled:
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

    async def get_model_lineage(self, model_selector: str) -> dict[str, Any]:
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

        if not self.lsp_connection.state.compiled:
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

    def _uri_to_path(self, uri: str) -> Path:
        """Convert a file:// URI to a Path object."""
        parsed = urlparse(uri)
        # Decode percent-encoded characters and remove the leading slash on Windows
        path_str = unquote(parsed.path)
        return Path(path_str)

    def _apply_single_edit(self, lines: list[str], edit: dict[str, Any]) -> None:
        """Apply a single text edit to a list of lines.

        Args:
            lines: List of lines (with line endings preserved)
            edit: LSP TextEdit object with 'range' and 'newText'
        """
        start_line_idx = edit["range"]["start"]["line"]
        start_char = edit["range"]["start"]["character"]
        end_line_idx = edit["range"]["end"]["line"]
        end_char = edit["range"]["end"]["character"]
        new_text = edit["newText"]

        # Handle single line edit
        if start_line_idx == end_line_idx:
            if start_line_idx < len(lines):
                line = lines[start_line_idx]
                # Strip line ending to work with just the text
                line_ending = ""
                if line.endswith("\r\n"):
                    line_ending = "\r\n"
                    line = line[:-2]
                elif line.endswith("\n"):
                    line_ending = "\n"
                    line = line[:-1]
                elif line.endswith("\r"):
                    line_ending = "\r"
                    line = line[:-1]

                # Apply the edit
                lines[start_line_idx] = (
                    line[:start_char] + new_text + line[end_char:] + line_ending
                )
        else:
            # Multi-line edit
            start_line = lines[start_line_idx]
            end_line = lines[end_line_idx]

            # Strip line endings
            if start_line.endswith("\r\n"):
                start_line = start_line[:-2]
            elif start_line.endswith("\n"):
                start_line = start_line[:-1]
            elif start_line.endswith("\r"):
                start_line = start_line[:-1]

            end_line_ending = ""
            if end_line.endswith("\r\n"):
                end_line_ending = "\r\n"
                end_line = end_line[:-2]
            elif end_line.endswith("\n"):
                end_line_ending = "\n"
                end_line = end_line[:-1]
            elif end_line.endswith("\r"):
                end_line_ending = "\r"
                end_line = end_line[:-1]

            start_line_text = start_line[:start_char]
            end_line_text = end_line[end_char:]
            lines[start_line_idx] = (
                start_line_text + new_text + end_line_text + end_line_ending
            )
            # Remove the lines in between
            del lines[start_line_idx + 1 : end_line_idx + 1]

    def _apply_text_edits(self, file_path: Path, edits: list[dict[str, Any]]) -> None:
        """Apply text edits to a file.

        Args:
            file_path: Path to the file to edit
            edits: List of LSP TextEdit objects with 'range' and 'newText'
        """
        if not file_path.exists():
            logger.warning(f"File not found for edits: {file_path}")
            return

        # Read the file content
        content = file_path.read_text()

        # Sort edits in reverse order (end to start) to avoid offset issues
        # We sort by start position descending so we can apply from end to beginning
        sorted_edits = sorted(
            edits,
            key=lambda e: (
                e["range"]["start"]["line"],
                e["range"]["start"]["character"],
            ),
            reverse=True,
        )

        # Convert content to list of lines for easier manipulation
        lines = content.splitlines(keepends=True)

        # Apply each edit
        for edit in sorted_edits:
            self._apply_single_edit(lines, edit)

        # Write back to file
        file_path.write_text("".join(lines))
        logger.info(f"Applied {len(edits)} edits to {file_path}")

    async def rename_model(
        self,
        old_uri: str,
        new_uri: str,
        apply_edits: bool = True,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Rename a dbt model file and update all references.

        This method:
        1. Asks the LSP server what edits are needed for the rename
        2. Applies those edits to update references in other files
        3. Performs the actual file rename on disk
        4. Notifies the LSP server that the rename is complete

        Args:
            old_uri: The current file URI (e.g., "file:///path/to/model.sql")
            new_uri: The new file URI (e.g., "file:///path/to/new_model.sql")
            apply_edits: Whether to apply workspace edits and perform the rename (default: True)
            timeout: Optional timeout for the request

        Returns:
            Dictionary with:
            - 'renamed': True if model was renamed
            - 'old_path': Original file path
            - 'new_path': New file path
            - 'files_updated': List of files that had references updated
            - 'error': Error message if something failed
        """
        logger.info(f"Renaming model: {old_uri} -> {new_uri}")

        # Step 1: Ask LSP what edits are needed
        params = {
            "files": [
                {
                    "oldUri": old_uri,
                    "newUri": new_uri,
                }
            ]
        }

        try:
            async with asyncio.timeout(timeout or self.timeout):
                result = await self.lsp_connection.send_request(
                    "workspace/willRenameFiles",
                    params,
                )

                # Handle None or empty result
                if result is None:
                    result = {}

                if "error" in result and result["error"] is not None:
                    return {"error": result["error"]}

                if not apply_edits:
                    # Just return the workspace edits without applying them
                    return result

                # Step 2: Apply workspace edits
                files_updated = []
                if result and "changes" in result:
                    # Handle WorkspaceEdit with 'changes' format
                    for file_uri, edits in result["changes"].items():
                        try:
                            file_path = self._uri_to_path(file_uri)
                            self._apply_text_edits(file_path, edits)
                            files_updated.append(str(file_path))
                        except Exception as e:
                            logger.error(f"Failed to apply edits to {file_uri}: {e}")
                            return {
                                "error": f"Failed to apply edits to {file_uri}: {str(e)}"
                            }
                elif result and "documentChanges" in result:
                    # Handle WorkspaceEdit with 'documentChanges' format
                    for change in result["documentChanges"]:
                        if "textDocument" in change and "edits" in change:
                            file_uri = change["textDocument"]["uri"]
                            try:
                                file_path = self._uri_to_path(file_uri)
                                self._apply_text_edits(file_path, change["edits"])
                                files_updated.append(str(file_path))
                            except Exception as e:
                                logger.error(
                                    f"Failed to apply edits to {file_uri}: {e}"
                                )
                                return {
                                    "error": f"Failed to apply edits to {file_uri}: {str(e)}"
                                }

                # Step 3: Perform the actual file rename
                old_path = self._uri_to_path(old_uri)
                new_path = self._uri_to_path(new_uri)

                if not old_path.exists():
                    return {"error": f"Source file does not exist: {old_path}"}

                if new_path.exists():
                    return {"error": f"Destination file already exists: {new_path}"}

                try:
                    # Ensure parent directory exists
                    new_path.parent.mkdir(parents=True, exist_ok=True)
                    old_path.rename(new_path)
                    logger.info(f"Renamed file: {old_path} -> {new_path}")
                except Exception as e:
                    return {"error": f"Failed to rename file: {str(e)}"}

                # Step 4: Notify LSP of completion (didRenameFiles)
                try:
                    self.lsp_connection.send_notification(
                        "workspace/didRenameFiles",
                        {
                            "files": [
                                {
                                    "oldUri": old_uri,
                                    "newUri": new_uri,
                                }
                            ]
                        },
                    )
                except Exception as e:
                    logger.warning(f"Failed to send didRenameFiles notification: {e}")

                return {
                    "renamed": True,
                    "old_path": str(old_path),
                    "new_path": str(new_path),
                    "files_updated": files_updated,
                }

        except TimeoutError:
            return {"error": "Timeout waiting for LSP response"}
        except Exception as e:
            return {"error": f"Failed to rename file: {str(e)}"}
