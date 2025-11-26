"""Unit tests for the LSP binary detection and management module."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from dbt_mcp.lsp.lsp_binary_manager import (
    CodeEditor,
    LspBinaryInfo,
    dbt_lsp_binary_info,
    detect_lsp_binary,
    get_lsp_binary_version,
    get_storage_path,
)


class TestGetStoragePath:
    """Tests for get_storage_path function."""

    @patch("dbt_mcp.lsp.lsp_binary_manager.system", "Windows")
    @patch("dbt_mcp.lsp.lsp_binary_manager.home", Path("C:/Users/TestUser"))
    def test_windows_vscode(self, monkeypatch):
        """Test storage path for VS Code on Windows."""
        monkeypatch.setenv("APPDATA", "C:/Users/TestUser/AppData/Roaming")

        result = get_storage_path(CodeEditor.CODE)

        expected = Path(
            "C:/Users/TestUser/AppData/Roaming/code/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp.exe"
        )
        assert result == expected

    @patch("dbt_mcp.lsp.lsp_binary_manager.system", "Windows")
    @patch("dbt_mcp.lsp.lsp_binary_manager.home", Path("C:/Users/TestUser"))
    def test_windows_cursor(self, monkeypatch):
        """Test storage path for Cursor on Windows."""
        monkeypatch.setenv("APPDATA", "C:/Users/TestUser/AppData/Roaming")

        result = get_storage_path(CodeEditor.CURSOR)

        expected = Path(
            "C:/Users/TestUser/AppData/Roaming/cursor/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp.exe"
        )
        assert result == expected

    @patch("dbt_mcp.lsp.lsp_binary_manager.system", "Windows")
    @patch("dbt_mcp.lsp.lsp_binary_manager.home", Path("C:/Users/TestUser"))
    def test_windows_windsurf(self, monkeypatch):
        """Test storage path for Windsurf on Windows."""
        monkeypatch.setenv("APPDATA", "C:/Users/TestUser/AppData/Roaming")

        result = get_storage_path(CodeEditor.WINDSURF)

        expected = Path(
            "C:/Users/TestUser/AppData/Roaming/windsurf/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp.exe"
        )
        assert result == expected

    @patch("dbt_mcp.lsp.lsp_binary_manager.system", "Windows")
    @patch("dbt_mcp.lsp.lsp_binary_manager.home", Path("C:/Users/TestUser"))
    def test_windows_no_appdata_env(self, monkeypatch):
        """Test storage path on Windows when APPDATA env var is not set."""
        monkeypatch.delenv("APPDATA", raising=False)

        result = get_storage_path(CodeEditor.CODE)

        expected = Path(
            "C:/Users/TestUser/AppData/Roaming/code/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp.exe"
        )
        assert result == expected

    @patch("dbt_mcp.lsp.lsp_binary_manager.system", "Darwin")
    @patch("dbt_mcp.lsp.lsp_binary_manager.home", Path("/Users/testuser"))
    def test_macos_vscode(self):
        """Test storage path for VS Code on macOS."""
        result = get_storage_path(CodeEditor.CODE)

        expected = Path(
            "/Users/testuser/Library/Application Support/code/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp"
        )
        assert result == expected

    @patch("dbt_mcp.lsp.lsp_binary_manager.system", "Darwin")
    @patch("dbt_mcp.lsp.lsp_binary_manager.home", Path("/Users/testuser"))
    def test_macos_cursor(self):
        """Test storage path for Cursor on macOS."""
        result = get_storage_path(CodeEditor.CURSOR)

        expected = Path(
            "/Users/testuser/Library/Application Support/cursor/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp"
        )
        assert result == expected

    @patch("dbt_mcp.lsp.lsp_binary_manager.system", "Linux")
    @patch("dbt_mcp.lsp.lsp_binary_manager.home", Path("/home/testuser"))
    def test_linux_vscode(self, monkeypatch):
        """Test storage path for VS Code on Linux."""
        monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)

        result = get_storage_path(CodeEditor.CODE)

        expected = Path(
            "/home/testuser/.config/code/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp"
        )
        assert result == expected

    @patch("dbt_mcp.lsp.lsp_binary_manager.system", "Linux")
    @patch("dbt_mcp.lsp.lsp_binary_manager.home", Path("/home/testuser"))
    def test_linux_cursor_with_xdg_config(self, monkeypatch):
        """Test storage path for Cursor on Linux with XDG_CONFIG_HOME set."""
        monkeypatch.setenv("XDG_CONFIG_HOME", "/home/testuser/.custom-config")

        result = get_storage_path(CodeEditor.CURSOR)

        expected = Path(
            "/home/testuser/.custom-config/cursor/User/globalStorage/dbtlabsinc.dbt/bin/dbt-lsp"
        )
        assert result == expected

    @patch("dbt_mcp.lsp.lsp_binary_manager.system", "SunOS")
    def test_unsupported_os(self):
        """Test that unsupported OS raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported OS: SunOS"):
            get_storage_path(CodeEditor.CODE)


class TestGetLspBinaryVersion:
    """Tests for get_lsp_binary_version function."""

    def test_dbt_lsp_version_from_file(self, tmp_path):
        """Test reading version from .version file for dbt-lsp binary."""
        # Create a fake dbt-lsp binary and .version file
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        lsp_binary = bin_dir / "dbt-lsp"
        lsp_binary.touch()
        version_file = bin_dir / ".version"
        version_file.write_text("  1.2.3\n")

        result = get_lsp_binary_version(str(lsp_binary))

        assert result == "1.2.3"

    def test_dbt_lsp_exe_version_from_file(self, tmp_path):
        """Test reading version from .version file for dbt-lsp.exe binary (Windows)."""
        # Create a fake dbt-lsp.exe binary and .version file
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        lsp_binary = bin_dir / "dbt-lsp.exe"
        lsp_binary.touch()
        version_file = bin_dir / ".version"
        version_file.write_text("2.0.0-rc1  \n")

        result = get_lsp_binary_version(str(lsp_binary))

        assert result == "2.0.0-rc1"

    @patch("dbt_mcp.lsp.lsp_binary_manager.subprocess.run")
    def test_version_file_not_found_falls_back_to_command(self, mock_run, tmp_path):
        """Test that when .version file is missing, it falls back to running --version."""
        # Create a fake binary without .version file
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        lsp_binary = bin_dir / "dbt-lsp"
        lsp_binary.touch()

        # Mock the subprocess call
        mock_run.return_value = Mock(stdout="1.5.0\n")

        result = get_lsp_binary_version(str(lsp_binary))

        assert result == "1.5.0"
        mock_run.assert_called_once_with(
            [str(lsp_binary), "--version"],
            capture_output=True,
            text=True,
        )

    @patch("dbt_mcp.lsp.lsp_binary_manager.subprocess.run")
    def test_custom_binary_version_from_command(self, mock_run):
        """Test getting version from custom binary using --version flag."""
        mock_run.return_value = Mock(stdout="custom-lsp version 3.4.5\n")

        result = get_lsp_binary_version("/usr/local/bin/custom-lsp")

        assert result == "custom-lsp version 3.4.5"
        mock_run.assert_called_once_with(
            ["/usr/local/bin/custom-lsp", "--version"],
            capture_output=True,
            text=True,
        )

    @patch("dbt_mcp.lsp.lsp_binary_manager.subprocess.run")
    def test_custom_binary_with_whitespace_in_version(self, mock_run):
        """Test that version output is properly stripped of whitespace."""
        mock_run.return_value = Mock(stdout="  4.0.0  \n")

        result = get_lsp_binary_version("/opt/my-lsp")

        assert result == "4.0.0"


class TestDetectLspBinary:
    """Tests for detect_lsp_binary function."""

    @patch("dbt_mcp.lsp.lsp_binary_manager.get_storage_path")
    @patch("dbt_mcp.lsp.lsp_binary_manager.get_lsp_binary_version")
    def test_detect_first_available_binary(self, mock_get_version, mock_get_path):
        """Test detecting the first available LSP binary."""
        # Mock paths for different editors
        vscode_path = MagicMock(spec=Path)
        vscode_path.exists.return_value = False
        vscode_path.is_file.return_value = False

        cursor_path = MagicMock(spec=Path)
        cursor_path.exists.return_value = True
        cursor_path.is_file.return_value = True
        cursor_path.as_posix.return_value = "/path/to/cursor/dbt-lsp"

        windsurf_path = MagicMock(spec=Path)
        windsurf_path.exists.return_value = True
        windsurf_path.is_file.return_value = True

        mock_get_path.side_effect = [vscode_path, cursor_path, windsurf_path]
        mock_get_version.return_value = "1.5.0"

        result = detect_lsp_binary()

        assert result is not None
        assert result.path == "/path/to/cursor/dbt-lsp"
        assert result.version == "1.5.0"
        assert mock_get_path.call_count == 2  # Called for CODE and CURSOR

    @patch("dbt_mcp.lsp.lsp_binary_manager.get_storage_path")
    def test_detect_no_binary_found(self, mock_get_path):
        """Test that None is returned when no binary is found."""
        # All paths don't exist
        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = False
        mock_get_path.return_value = mock_path

        result = detect_lsp_binary()

        assert result is None
        assert mock_get_path.call_count == 3  # Called for all editors

    @patch("dbt_mcp.lsp.lsp_binary_manager.get_storage_path")
    @patch("dbt_mcp.lsp.lsp_binary_manager.get_lsp_binary_version")
    def test_detect_binary_directory_not_file(self, mock_get_version, mock_get_path):
        """Test that directories are skipped when looking for binary file."""
        # Path exists but is a directory, not a file
        vscode_path = MagicMock(spec=Path)
        vscode_path.exists.return_value = True
        vscode_path.is_file.return_value = False

        cursor_path = MagicMock(spec=Path)
        cursor_path.exists.return_value = False

        windsurf_path = MagicMock(spec=Path)
        windsurf_path.exists.return_value = False

        mock_get_path.side_effect = [vscode_path, cursor_path, windsurf_path]

        result = detect_lsp_binary()

        assert result is None
        mock_get_version.assert_not_called()

    @patch("dbt_mcp.lsp.lsp_binary_manager.get_storage_path")
    @patch("dbt_mcp.lsp.lsp_binary_manager.get_lsp_binary_version")
    def test_detect_windsurf_binary(self, mock_get_version, mock_get_path):
        """Test detecting binary in Windsurf location."""
        # Only Windsurf has the binary
        vscode_path = MagicMock(spec=Path)
        vscode_path.exists.return_value = False

        cursor_path = MagicMock(spec=Path)
        cursor_path.exists.return_value = False

        windsurf_path = MagicMock(spec=Path)
        windsurf_path.exists.return_value = True
        windsurf_path.is_file.return_value = True
        windsurf_path.as_posix.return_value = "/path/to/windsurf/dbt-lsp"

        mock_get_path.side_effect = [vscode_path, cursor_path, windsurf_path]
        mock_get_version.return_value = "2.0.0"

        result = detect_lsp_binary()

        assert result is not None
        assert result.path == "/path/to/windsurf/dbt-lsp"
        assert result.version == "2.0.0"


class TestDbtLspBinaryInfo:
    """Tests for dbt_lsp_binary_info function."""

    def test_custom_path_valid_file(self, tmp_path):
        """Test using a valid custom LSP binary path."""
        lsp_binary = tmp_path / "custom-lsp"
        lsp_binary.touch()
        version_file = tmp_path / ".version"
        version_file.write_text("1.0.0")

        with patch(
            "dbt_mcp.lsp.lsp_binary_manager.get_lsp_binary_version"
        ) as mock_version:
            mock_version.return_value = "1.0.0"

            result = dbt_lsp_binary_info(str(lsp_binary))

            assert result is not None
            assert result.path == str(lsp_binary)
            assert result.version == "1.0.0"

    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_lsp_binary")
    def test_custom_path_invalid_falls_back_to_detection(self, mock_detect):
        """Test that invalid custom path falls back to auto-detection."""
        mock_detect.return_value = LspBinaryInfo(
            path="/auto/detected/path", version="2.0.0"
        )

        result = dbt_lsp_binary_info("/nonexistent/path")

        assert result is not None
        assert result.path == "/auto/detected/path"
        assert result.version == "2.0.0"
        mock_detect.assert_called_once()

    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_lsp_binary")
    def test_custom_path_empty_string_uses_detection(self, mock_detect):
        """Test that empty string path triggers auto-detection."""
        mock_detect.return_value = LspBinaryInfo(
            path="/auto/detected/path", version="3.0.0"
        )

        result = dbt_lsp_binary_info("")

        assert result is not None
        assert result.path == "/auto/detected/path"
        mock_detect.assert_called_once()

    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_lsp_binary")
    def test_no_custom_path_uses_detection(self, mock_detect):
        """Test that None path uses auto-detection."""
        mock_detect.return_value = LspBinaryInfo(
            path="/auto/detected/path", version="4.0.0"
        )

        result = dbt_lsp_binary_info(None)

        assert result is not None
        assert result.path == "/auto/detected/path"
        mock_detect.assert_called_once()

    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_lsp_binary")
    def test_detection_returns_none(self, mock_detect):
        """Test that None is returned when detection fails."""
        mock_detect.return_value = None

        result = dbt_lsp_binary_info(None)

        assert result is None

    def test_custom_path_directory_not_file(self, tmp_path):
        """Test that directory path (not file) falls back to detection."""
        lsp_dir = tmp_path / "lsp"
        lsp_dir.mkdir()

        with patch("dbt_mcp.lsp.lsp_binary_manager.detect_lsp_binary") as mock_detect:
            mock_detect.return_value = LspBinaryInfo(
                path="/detected/path", version="5.0.0"
            )

            result = dbt_lsp_binary_info(str(lsp_dir))

            assert result is not None
            assert result.path == "/detected/path"
            mock_detect.assert_called_once()


class TestLspBinaryInfo:
    """Tests for LspBinaryInfo dataclass."""

    def test_create_lsp_binary_info(self):
        """Test creating LspBinaryInfo instance."""
        info = LspBinaryInfo(path="/path/to/lsp", version="1.2.3")

        assert info.path == "/path/to/lsp"
        assert info.version == "1.2.3"

    def test_lsp_binary_info_equality(self):
        """Test LspBinaryInfo equality comparison."""
        info1 = LspBinaryInfo(path="/path/to/lsp", version="1.2.3")
        info2 = LspBinaryInfo(path="/path/to/lsp", version="1.2.3")
        info3 = LspBinaryInfo(path="/other/path", version="1.2.3")

        assert info1 == info2
        assert info1 != info3


class TestCodeEditor:
    """Tests for CodeEditor enum."""

    def test_code_editor_values(self):
        """Test CodeEditor enum has expected values."""
        assert CodeEditor.CODE == "code"
        assert CodeEditor.CURSOR == "cursor"
        assert CodeEditor.WINDSURF == "windsurf"

    def test_code_editor_iteration(self):
        """Test iterating over CodeEditor enum."""
        editors = list(CodeEditor)

        assert len(editors) == 3
        assert CodeEditor.CODE in editors
        assert CodeEditor.CURSOR in editors
        assert CodeEditor.WINDSURF in editors
