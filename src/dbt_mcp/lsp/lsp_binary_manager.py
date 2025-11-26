"""Binary detection and management for the dbt Language Server Protocol (LSP).

This module provides utilities to locate and validate the dbt LSP binary across
different operating systems and code editors (VS Code, Cursor, Windsurf). It handles
platform-specific paths and binary naming conventions.
"""

from enum import StrEnum
import os
from pathlib import Path
import platform
import subprocess
from dataclasses import dataclass

import logging

logger = logging.getLogger(__name__)

system = platform.system()
home = Path.home()


@dataclass
class LspBinaryInfo:
    """Information about a detected dbt LSP binary.

    Attributes:
        path: Full filesystem path to the LSP binary executable.
        version: Version string of the LSP binary.
    """

    path: str
    version: str


class CodeEditor(StrEnum):
    """Supported code editors that can install the dbt LSP.

    These editors use similar global storage patterns for VSCode extensions
    and can install the dbt Labs extension with the LSP binary.
    """

    CODE = "code"  # Visual Studio Code
    CURSOR = "cursor"  # Cursor editor
    WINDSURF = "windsurf"  # Windsurf editor


def get_storage_path(editor: CodeEditor) -> Path:
    """Get the storage path for dbt LSP binary based on editor and OS.

    Determines the platform-specific path where code editors store the dbt LSP
    binary. Follows standard conventions for each operating system and editor.

    Platform-specific paths:
        - Windows: %APPDATA%\\{editor}\\User\\globalStorage\\dbtlabsinc.dbt\\bin\\dbt-lsp
        - macOS: ~/Library/Application Support/{editor}/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp
        - Linux: ~/.config/{editor}/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp

    Args:
        editor: The code editor to get the storage path for.

    Returns:
        Path object pointing to the expected location of the dbt-lsp binary.

    Raises:
        ValueError: If the operating system is not supported (Windows, macOS, or Linux).

    Note:
        This function returns the expected path regardless of whether the binary
        actually exists at that location. Use Path.exists() to verify.
    """
    binary_name = "dbt-lsp"

    if system == "Windows":
        appdata = os.environ.get("APPDATA", home / "AppData" / "Roaming")
        base = Path(appdata) / editor.value
        binary_name = "dbt-lsp.exe"

    elif system == "Darwin":  # macOS
        base = home / "Library" / "Application Support" / editor.value

    elif system == "Linux":
        config_home = os.environ.get("XDG_CONFIG_HOME", home / ".config")
        base = Path(config_home) / editor.value

    else:
        raise ValueError(f"Unsupported OS: {system}")

    return Path(base, "User", "globalStorage", "dbtlabsinc.dbt", "bin", binary_name)


def dbt_lsp_binary_info(lsp_path: str | None = None) -> LspBinaryInfo | None:
    """Get dbt LSP binary information from a custom path or auto-detect it.

    Attempts to locate and validate the dbt LSP binary. If a custom path is provided,
    it will be validated first. If the custom path is invalid or not provided, the
    function will attempt to auto-detect the binary in standard editor locations.

    Args:
        lsp_path: Optional custom path to the dbt LSP binary. If provided, this
            path will be validated and used if it exists. If None or invalid,
            auto-detection will be attempted.

    Returns:
        LspBinaryInfo object containing the path and version of the found binary,
        or None if no valid binary could be found.

    Note:
        If a custom path is provided but invalid, a warning will be logged before
        falling back to auto-detection.
    """
    if lsp_path:
        logger.debug(f"Using custom LSP binary path: {lsp_path}")
        if Path(lsp_path).exists() and Path(lsp_path).is_file():
            version = get_lsp_binary_version(lsp_path)
            return LspBinaryInfo(path=lsp_path, version=version)
        logger.warning(
            f"Provided LSP binary path {lsp_path} does not exist or is not a file, falling back to detecting LSP binary"
        )
    return detect_lsp_binary()


def detect_lsp_binary() -> LspBinaryInfo | None:
    """Auto-detect dbt LSP binary in standard code editor locations.

    Searches through all supported code editors (VS Code, Cursor, Windsurf) to find
    an installed dbt LSP binary. Returns the first valid binary found.

    Returns:
        LspBinaryInfo object containing the path and version of the first found binary,
        or None if no binary is found in any of the standard locations.

    Note:
        The detection checks editors in the order defined by the CodeEditor enum.
        Debug logging is used to track the search process.
    """
    for editor in CodeEditor:
        path = get_storage_path(editor)
        logger.debug(f"Checking for LSP binary in {path}")
        if path.exists() and path.is_file():
            version = get_lsp_binary_version(path.as_posix())
            logger.debug(f"Found LSP binary in {path} with version {version}")
            return LspBinaryInfo(path=path.as_posix(), version=version)

    return None


def get_lsp_binary_version(path: str) -> str:
    """Extract the version string from a dbt LSP binary.

    Retrieves the version of the dbt LSP binary using one of two methods:
    1. If a .version file exists in the same directory as the binary, read from it
    2. Otherwise, execute the binary with --version flag

    Args:
        path: Full filesystem path to the dbt LSP binary.

    Returns:
        Version string of the binary (whitespace stripped).

    Raises:
        subprocess.SubprocessError: If the binary execution fails when .version file
            doesn't exist.

    Note:
        The .version file is expected to be in the same directory as the binary
        and should be named '.version'. This fallback behavior allows the function
        to work with both standard dbt-lsp installations and custom LSP binaries.
    """
    version_file = Path(path).parent / ".version"
    if version_file.exists():
        return version_file.read_text().strip()
    else:
        return subprocess.run(
            [path, "--version"], capture_output=True, text=True
        ).stdout.strip()
