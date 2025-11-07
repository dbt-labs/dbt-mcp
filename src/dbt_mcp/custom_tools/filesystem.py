"""File system abstraction for custom tool model discovery."""

from abc import ABC, abstractmethod
from pathlib import Path


class FileSystemProvider(ABC):
    """Abstract base class for file system operations."""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check if a file exists at the given path.

        Args:
            path: The file path to check

        Returns:
            True if file exists, False otherwise
        """
        pass

    @abstractmethod
    def read_text(self, path: str) -> str:
        """
        Read the contents of a text file.

        Args:
            path: The file path to read

        Returns:
            The file contents as a string

        Raises:
            FileNotFoundError: If the file does not exist
            OSError: If there's an error reading the file
        """
        pass

    @abstractmethod
    def join_path(self, base: str, *parts: str) -> str:
        """
        Join path components together.

        Args:
            base: The base path
            *parts: Additional path components to join

        Returns:
            The joined path as a string
        """
        pass


class LocalFileSystemProvider(FileSystemProvider):
    """Default file system provider using local pathlib operations."""

    def exists(self, path: str) -> bool:
        """Check if a file exists on the local filesystem."""
        return Path(path).exists()

    def read_text(self, path: str) -> str:
        """Read text from a local file."""
        return Path(path).read_text()

    def join_path(self, base: str, *parts: str) -> str:
        """Join path components using pathlib."""
        path = Path(base)
        for part in parts:
            path = path / part
        return str(path)
