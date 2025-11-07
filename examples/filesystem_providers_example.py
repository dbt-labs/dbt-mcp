"""
Example implementations of custom FileSystemProvider classes.

This module demonstrates how to create custom file system providers
for different storage backends like S3, HTTP APIs, or in-memory storage.
"""

from pathlib import Path

from dbt_mcp.custom_tools.filesystem import FileSystemProvider


class InMemoryFileSystemProvider(FileSystemProvider):
    """
    In-memory file system provider for testing and development.

    This provider stores files in a dictionary, making it useful for:
    - Unit testing without file I/O
    - Rapid prototyping
    - Mocking file systems in tests
    """

    def __init__(self, files: dict[str, str] | None = None):
        """
        Initialize with optional pre-populated files.

        Args:
            files: Dictionary mapping file paths to their contents
        """
        self.files = files or {}

    def add_file(self, path: str, content: str) -> None:
        """Add a file to the in-memory filesystem."""
        self.files[path] = content

    def exists(self, path: str) -> bool:
        """Check if a file exists in memory."""
        return path in self.files

    def read_text(self, path: str) -> str:
        """Read file contents from memory."""
        if path not in self.files:
            raise FileNotFoundError(f"File not found: {path}")
        return self.files[path]

    def join_path(self, base: str, *parts: str) -> str:
        """Join path components."""
        path = Path(base)
        for part in parts:
            path = path / part
        return str(path)


class HTTPFileSystemProvider(FileSystemProvider):
    """
    HTTP-based file system provider for remote files.

    This provider fetches files over HTTP(S), useful for:
    - Reading from remote Git repositories
    - Accessing files from web APIs
    - Reading from CDNs or cloud storage with HTTP endpoints
    """

    def __init__(self, base_url: str, session=None):
        """
        Initialize with base URL for file access.

        Args:
            base_url: Base URL for file access (e.g., 'https://api.github.com')
            session: Optional requests.Session for connection pooling
        """
        self.base_url = base_url.rstrip("/")
        self.session = session

    def exists(self, path: str) -> bool:
        """Check if a file exists via HEAD request."""
        import requests

        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            session = self.session or requests
            response = session.head(url, timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def read_text(self, path: str) -> str:
        """Read file contents via GET request."""
        import requests

        url = f"{self.base_url}/{path.lstrip('/')}"
        session = self.session or requests
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response.text

    def join_path(self, base: str, *parts: str) -> str:
        """Join URL path components."""
        path = base.rstrip("/")
        for part in parts:
            path = f"{path}/{part.lstrip('/')}"
        return path


class S3FileSystemProvider(FileSystemProvider):
    """
    AWS S3-based file system provider.

    This provider reads files from S3 buckets, useful for:
    - Reading from S3-stored dbt projects
    - Accessing files in cloud storage
    - Working with distributed teams using S3

    Requires boto3: pip install boto3
    """

    def __init__(self, bucket_name: str, s3_client=None, prefix: str = ""):
        """
        Initialize with S3 bucket details.

        Args:
            bucket_name: Name of the S3 bucket
            s3_client: Optional boto3 S3 client
            prefix: Optional prefix for all paths
        """
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip("/")

        if s3_client is None:
            import boto3

            self.s3_client = boto3.client("s3")
        else:
            self.s3_client = s3_client

    def _get_full_key(self, path: str) -> str:
        """Get the full S3 key including prefix."""
        path = path.lstrip("/")
        if self.prefix:
            return f"{self.prefix}/{path}"
        return path

    def exists(self, path: str) -> bool:
        """Check if a file exists in S3."""
        try:
            key = self._get_full_key(path)
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except Exception:
            return False

    def read_text(self, path: str) -> str:
        """Read file contents from S3."""
        key = self._get_full_key(path)
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        return response["Body"].read().decode("utf-8")

    def join_path(self, base: str, *parts: str) -> str:
        """Join S3 path components."""
        path = base.rstrip("/")
        for part in parts:
            path = f"{path}/{part.lstrip('/')}"
        return path


class CachedFileSystemProvider(FileSystemProvider):
    """
    Caching wrapper for any FileSystemProvider.

    This provider adds caching to another provider, useful for:
    - Reducing repeated file reads
    - Improving performance for remote providers
    - Reducing API calls and costs
    """

    def __init__(self, wrapped_provider: FileSystemProvider):
        """
        Initialize with a provider to wrap.

        Args:
            wrapped_provider: The FileSystemProvider to add caching to
        """
        self.provider = wrapped_provider
        self._exists_cache: dict[str, bool] = {}
        self._content_cache: dict[str, str] = {}

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._exists_cache.clear()
        self._content_cache.clear()

    def exists(self, path: str) -> bool:
        """Check if file exists, using cache if available."""
        if path not in self._exists_cache:
            self._exists_cache[path] = self.provider.exists(path)
        return self._exists_cache[path]

    def read_text(self, path: str) -> str:
        """Read file contents, using cache if available."""
        if path not in self._content_cache:
            self._content_cache[path] = self.provider.read_text(path)
        return self._content_cache[path]

    def join_path(self, base: str, *parts: str) -> str:
        """Join path components (delegated to wrapped provider)."""
        return self.provider.join_path(base, *parts)


# Example usage
if __name__ == "__main__":
    # Example 1: In-Memory Provider
    print("=== In-Memory Provider ===")
    mem_fs = InMemoryFileSystemProvider()
    mem_fs.add_file("/project/models/tools/example.sql", "SELECT * FROM users")

    print(f"File exists: {mem_fs.exists('/project/models/tools/example.sql')}")
    print(f"Content: {mem_fs.read_text('/project/models/tools/example.sql')}")

    # Example 2: Cached Provider
    print("\n=== Cached Provider ===")
    cached_fs = CachedFileSystemProvider(mem_fs)

    # First read (cache miss)
    content1 = cached_fs.read_text("/project/models/tools/example.sql")
    print(f"First read: {content1}")

    # Second read (cache hit)
    content2 = cached_fs.read_text("/project/models/tools/example.sql")
    print(f"Second read (cached): {content2}")

    # Example 3: S3 Provider (commented out, requires AWS credentials)
    # print("\n=== S3 Provider ===")
    # s3_fs = S3FileSystemProvider(
    #     bucket_name="my-dbt-bucket",
    #     prefix="dbt-projects/production"
    # )
    # if s3_fs.exists("models/tools/example.sql"):
    #     content = s3_fs.read_text("models/tools/example.sql")
    #     print(f"S3 Content: {content}")

    print("\n✅ All examples completed!")

