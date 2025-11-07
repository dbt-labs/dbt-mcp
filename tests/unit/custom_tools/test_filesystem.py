"""Tests for filesystem abstraction."""

import pytest

from dbt_mcp.custom_tools.filesystem import (
    FileSystemProvider,
    LocalFileSystemProvider,
)


class TestLocalFileSystemProvider:
    """Tests for LocalFileSystemProvider."""

    def test_exists_with_real_file(self, tmp_path):
        """Test exists() with a real file."""
        # Create a test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        fs = LocalFileSystemProvider()
        assert fs.exists(str(test_file)) is True
        assert fs.exists(str(tmp_path / "nonexistent.txt")) is False

    def test_read_text_with_real_file(self, tmp_path):
        """Test read_text() with a real file."""
        # Create a test file
        test_file = tmp_path / "test.txt"
        expected_content = "Hello, World!"
        test_file.write_text(expected_content)

        fs = LocalFileSystemProvider()
        content = fs.read_text(str(test_file))
        assert content == expected_content

    def test_read_text_nonexistent_file(self):
        """Test read_text() with nonexistent file raises error."""
        fs = LocalFileSystemProvider()
        with pytest.raises(FileNotFoundError):
            fs.read_text("/nonexistent/path/file.txt")

    def test_join_path(self):
        """Test join_path() creates correct paths."""
        fs = LocalFileSystemProvider()

        # Test basic joining
        result = fs.join_path("/base", "dir1", "file.txt")
        assert result == "/base/dir1/file.txt"

        # Test with trailing slash
        result = fs.join_path("/base/", "dir1/", "file.txt")
        assert "file.txt" in result

        # Test with single component
        result = fs.join_path("/base", "file.txt")
        assert result == "/base/file.txt"


class MockFileSystemProvider(FileSystemProvider):
    """Mock filesystem provider for testing."""

    def __init__(self, files: dict[str, str] | None = None):
        self.files = files or {}
        self.exists_calls = []
        self.read_text_calls = []

    def exists(self, path: str) -> bool:
        self.exists_calls.append(path)
        return path in self.files

    def read_text(self, path: str) -> str:
        self.read_text_calls.append(path)
        if path not in self.files:
            raise FileNotFoundError(f"File not found: {path}")
        return self.files[path]

    def join_path(self, base: str, *parts: str) -> str:
        result = base.rstrip("/")
        for part in parts:
            result = f"{result}/{part.lstrip('/')}"
        return result


class TestMockFileSystemProvider:
    """Tests demonstrating mock filesystem usage."""

    def test_mock_filesystem_exists(self):
        """Test mocked exists() method."""
        fs = MockFileSystemProvider({"/project/file.sql": "SELECT * FROM users"})

        assert fs.exists("/project/file.sql") is True
        assert fs.exists("/project/missing.sql") is False

        # Verify calls were tracked
        assert len(fs.exists_calls) == 2
        assert "/project/file.sql" in fs.exists_calls

    def test_mock_filesystem_read_text(self):
        """Test mocked read_text() method."""
        expected_content = "SELECT * FROM {{ ref('users') }}"
        fs = MockFileSystemProvider({"/project/model.sql": expected_content})

        content = fs.read_text("/project/model.sql")
        assert content == expected_content

        # Verify calls were tracked
        assert len(fs.read_text_calls) == 1
        assert fs.read_text_calls[0] == "/project/model.sql"

    def test_mock_filesystem_read_text_missing_file(self):
        """Test mocked read_text() raises error for missing files."""
        fs = MockFileSystemProvider({})

        with pytest.raises(FileNotFoundError, match="File not found"):
            fs.read_text("/missing/file.sql")

    def test_mock_filesystem_join_path(self):
        """Test mocked join_path() method."""
        fs = MockFileSystemProvider()

        result = fs.join_path("/base", "models", "tools", "file.sql")
        assert result == "/base/models/tools/file.sql"


class TestFileSystemProviderIntegration:
    """Integration tests showing how filesystem abstraction is used."""

    def test_discover_models_with_mock_filesystem(self):
        """
        Demonstrate how mock filesystem enables testing model discovery.

        This test shows how you can test model discovery logic without
        needing actual files on disk.
        """
        # Setup mock filesystem with test files
        fs = MockFileSystemProvider(
            {
                "/project/models/tools/customer_lookup.sql": """
                    SELECT * FROM {{ ref('customers') }}
                    WHERE customer_id = {{ var('customer_id') }}
                """,
                "/project/models/tools/order_report.sql": """
                    SELECT * FROM {{ ref('orders') }}
                    WHERE order_date >= {{ var('start_date') }}
                    AND order_date <= {{ var('end_date', 'CURRENT_DATE') }}
                """,
            }
        )

        # Test that we can read the files
        content = fs.read_text("/project/models/tools/customer_lookup.sql")
        assert "customer_id" in content
        assert "customers" in content

        content = fs.read_text("/project/models/tools/order_report.sql")
        assert "start_date" in content
        assert "end_date" in content

    def test_filesystem_abstraction_allows_different_backends(self):
        """
        Demonstrate that the same code works with different backends.

        This shows the power of the abstraction: the same logic can work
        with local files, S3, HTTP endpoints, or any other storage.
        """
        # Create multiple providers
        mock_fs = MockFileSystemProvider({"/path/file.txt": "mock content"})

        # Both providers implement the same interface
        def read_file_safely(fs: FileSystemProvider, path: str) -> str | None:
            """Helper that works with any filesystem provider."""
            if fs.exists(path):
                return fs.read_text(path)
            return None

        # Test with mock provider
        content = read_file_safely(mock_fs, "/path/file.txt")
        assert content == "mock content"

        content = read_file_safely(mock_fs, "/path/missing.txt")
        assert content is None
