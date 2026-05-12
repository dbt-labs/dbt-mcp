"""Unit tests for the LSP binary detection and management module."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from dbt_mcp.lsp.lsp_binary_manager import (
    CodeEditor,
    LspBinaryInfo,
    dbt_lsp_binary_info,
    detect_fusion_lsp,
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
        assert result.cmd == ["/path/to/cursor/dbt-lsp"]
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
        assert result.cmd == ["/path/to/windsurf/dbt-lsp"]
        assert result.version == "2.0.0"


class TestDetectFusionLsp:
    """Tests for detect_fusion_lsp function."""

    @patch("dbt_mcp.lsp.lsp_binary_manager.subprocess.run")
    @patch("dbt_mcp.lsp.lsp_binary_manager.shutil.which")
    def test_fusion_available_returns_binary_info(self, mock_which, mock_run):
        """Test that Fusion LSP is detected when dbt supports lsp subcommand."""
        mock_which.return_value = "/usr/local/bin/dbt"
        mock_run.side_effect = [
            Mock(returncode=0),  # dbt lsp --help
            Mock(returncode=0, stdout="dbt-fusion 1.9.0\n"),  # dbt --version
        ]

        result = detect_fusion_lsp("dbt")

        assert result is not None
        assert result.cmd == ["dbt", "lsp"]
        assert result.version == "dbt-fusion 1.9.0"

    @patch("dbt_mcp.lsp.lsp_binary_manager.shutil.which")
    def test_dbt_not_in_path_returns_none(self, mock_which):
        """Test that None is returned when dbt is not in PATH."""
        mock_which.return_value = None

        result = detect_fusion_lsp("dbt")

        assert result is None

    @patch("dbt_mcp.lsp.lsp_binary_manager.subprocess.run")
    @patch("dbt_mcp.lsp.lsp_binary_manager.shutil.which")
    def test_dbt_lsp_not_supported_returns_none(self, mock_which, mock_run):
        """Test that None is returned when dbt does not support the lsp subcommand."""
        mock_which.return_value = "/usr/bin/dbt"
        mock_run.return_value = Mock(returncode=1)  # dbt lsp --help fails

        result = detect_fusion_lsp("dbt")

        assert result is None

    @patch("dbt_mcp.lsp.lsp_binary_manager.subprocess.run")
    @patch("dbt_mcp.lsp.lsp_binary_manager.shutil.which")
    def test_custom_dbt_path_used_in_cmd(self, mock_which, mock_run):
        """Test that a custom dbt_path is reflected in the returned cmd."""
        mock_which.return_value = "/custom/path/dbt"
        mock_run.side_effect = [
            Mock(returncode=0),
            Mock(stdout="2.0.0\n"),
        ]

        result = detect_fusion_lsp("/custom/path/dbt")

        assert result is not None
        assert result.cmd == ["/custom/path/dbt", "lsp"]

    @patch("dbt_mcp.lsp.lsp_binary_manager.subprocess.run")
    @patch("dbt_mcp.lsp.lsp_binary_manager.shutil.which")
    def test_subprocess_timeout_returns_none(self, mock_which, mock_run):
        """Test that a subprocess timeout is handled gracefully."""
        import subprocess as sp

        mock_which.return_value = "/usr/bin/dbt"
        mock_run.side_effect = sp.TimeoutExpired(cmd="dbt", timeout=5)

        result = detect_fusion_lsp("dbt")

        assert result is None

    @patch("dbt_mcp.lsp.lsp_binary_manager.subprocess.run")
    @patch("dbt_mcp.lsp.lsp_binary_manager.shutil.which")
    def test_oserror_on_probe_returns_none(self, mock_which, mock_run):
        """Test that an OSError (e.g. exec format error) is handled gracefully."""
        mock_which.return_value = "/usr/bin/dbt"
        mock_run.side_effect = OSError("Exec format error")

        result = detect_fusion_lsp("dbt")

        assert result is None

    @patch("dbt_mcp.lsp.lsp_binary_manager.subprocess.run")
    @patch("dbt_mcp.lsp.lsp_binary_manager.shutil.which")
    def test_version_failure_still_returns_binary_info(self, mock_which, mock_run):
        """Test that a failed dbt --version still returns LspBinaryInfo with empty version."""
        mock_which.return_value = "/usr/bin/dbt"
        mock_run.side_effect = [
            Mock(returncode=0),  # dbt lsp --help succeeds
            Mock(returncode=1),  # dbt --version fails
        ]

        result = detect_fusion_lsp("dbt")

        assert result is not None
        assert result.cmd == ["dbt", "lsp"]
        assert result.version == ""


class TestDbtLspBinaryInfo:
    """Tests for dbt_lsp_binary_info function."""

    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_fusion_lsp")
    def test_fusion_takes_priority_over_custom_path(self, mock_fusion, tmp_path):
        """Test that Fusion is preferred over a custom LSP path."""
        mock_fusion.return_value = LspBinaryInfo(cmd=["dbt", "lsp"], version="2.0.0")
        lsp_binary = tmp_path / "custom-lsp"
        lsp_binary.touch()

        result = dbt_lsp_binary_info(lsp_path=str(lsp_binary), dbt_path="dbt")

        assert result is not None
        assert result.cmd == ["dbt", "lsp"]
        mock_fusion.assert_called_once_with("dbt")

    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_fusion_lsp")
    def test_custom_path_used_when_no_fusion(self, mock_fusion, tmp_path):
        """Test that custom LSP path is used when Fusion is not available."""
        mock_fusion.return_value = None
        lsp_binary = tmp_path / "custom-lsp"
        lsp_binary.touch()

        with patch("dbt_mcp.lsp.lsp_binary_manager.get_lsp_binary_version") as mock_v:
            mock_v.return_value = "1.0.0"
            result = dbt_lsp_binary_info(lsp_path=str(lsp_binary), dbt_path="dbt")

        assert result is not None
        assert result.cmd == [str(lsp_binary)]
        assert result.version == "1.0.0"

    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_lsp_binary")
    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_fusion_lsp")
    def test_editor_paths_fallback_when_no_fusion_no_custom_path(
        self, mock_fusion, mock_detect
    ):
        """Test that editor storage paths are used as last resort."""
        mock_fusion.return_value = None
        mock_detect.return_value = LspBinaryInfo(
            cmd=["/path/to/editor/dbt-lsp"], version="1.5.0"
        )

        result = dbt_lsp_binary_info(lsp_path=None, dbt_path="dbt")

        assert result is not None
        assert result.cmd == ["/path/to/editor/dbt-lsp"]
        mock_detect.assert_called_once()

    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_lsp_binary")
    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_fusion_lsp")
    def test_returns_none_when_all_detection_fails(self, mock_fusion, mock_detect):
        """Test that None is returned when all detection methods fail."""
        mock_fusion.return_value = None
        mock_detect.return_value = None

        result = dbt_lsp_binary_info(lsp_path=None, dbt_path="dbt")

        assert result is None

    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_lsp_binary")
    @patch("dbt_mcp.lsp.lsp_binary_manager.detect_fusion_lsp")
    def test_invalid_custom_path_falls_back_to_editor(self, mock_fusion, mock_detect):
        """Test that invalid custom path falls back to editor storage detection."""
        mock_fusion.return_value = None
        mock_detect.return_value = LspBinaryInfo(
            cmd=["/editor/dbt-lsp"], version="3.0.0"
        )

        result = dbt_lsp_binary_info(lsp_path="/nonexistent/path", dbt_path="dbt")

        assert result is not None
        assert result.cmd == ["/editor/dbt-lsp"]
        mock_detect.assert_called_once()

    def test_custom_path_directory_not_file(self, tmp_path):
        """Test that directory path (not file) falls back to detection."""
        lsp_dir = tmp_path / "lsp"
        lsp_dir.mkdir()

        with (
            patch("dbt_mcp.lsp.lsp_binary_manager.detect_fusion_lsp") as mock_fusion,
            patch("dbt_mcp.lsp.lsp_binary_manager.detect_lsp_binary") as mock_detect,
        ):
            mock_fusion.return_value = None
            mock_detect.return_value = LspBinaryInfo(
                cmd=["/detected/path"], version="5.0.0"
            )

            result = dbt_lsp_binary_info(lsp_path=str(lsp_dir), dbt_path="dbt")

            assert result is not None
            assert result.cmd == ["/detected/path"]
            mock_detect.assert_called_once()


class TestLspBinaryInfo:
    """Tests for LspBinaryInfo dataclass."""

    def test_create_lsp_binary_info(self):
        """Test creating LspBinaryInfo instance."""
        info = LspBinaryInfo(cmd=["/path/to/lsp"], version="1.2.3")

        assert info.cmd == ["/path/to/lsp"]
        assert info.version == "1.2.3"

    def test_create_fusion_lsp_binary_info(self):
        """Test creating LspBinaryInfo for Fusion (multi-element cmd)."""
        info = LspBinaryInfo(cmd=["dbt", "lsp"], version="1.5.0")

        assert info.cmd == ["dbt", "lsp"]
        assert info.version == "1.5.0"

    def test_lsp_binary_info_equality(self):
        """Test LspBinaryInfo equality comparison."""
        info1 = LspBinaryInfo(cmd=["/path/to/lsp"], version="1.2.3")
        info2 = LspBinaryInfo(cmd=["/path/to/lsp"], version="1.2.3")
        info3 = LspBinaryInfo(cmd=["/other/path"], version="1.2.3")

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
