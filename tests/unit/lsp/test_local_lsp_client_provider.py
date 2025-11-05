"""Unit tests for LocalLSPClientProvider class."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from dbt_mcp.lsp.lsp_client import LSPClient
from dbt_mcp.lsp.providers.local_lsp_client_provider import LocalLSPClientProvider
from dbt_mcp.lsp.providers.lsp_connection_provider import (
    LSPConnectionProvider,
    LSPConnectionProviderProtocol,
)


class MockLSPConnectionProvider(LSPConnectionProvider):
    """Mock implementation of LSPConnectionProvider for testing."""

    def __init__(self, mock_connection: LSPConnectionProviderProtocol | None = None):
        self.mock_connection = mock_connection or MagicMock()
        self.get_connection_call_count = 0
        self.cleanup_connection_call_count = 0
        self.should_raise_on_get_connection: Exception | None = None

    async def get_connection(self) -> LSPConnectionProviderProtocol:
        """Return the mock connection."""
        self.get_connection_call_count += 1
        if self.should_raise_on_get_connection:
            raise self.should_raise_on_get_connection
        return self.mock_connection

    async def cleanup_connection(self) -> None:
        """Track cleanup calls."""
        self.cleanup_connection_call_count += 1


@pytest.fixture
def mock_connection_provider() -> MockLSPConnectionProvider:
    """Create a test LSP connection provider."""
    return MockLSPConnectionProvider()


class TestLocalLSPClientProvider:
    """Test LocalLSPClientProvider class."""

    @pytest.mark.asyncio
    async def test_get_client_returns_lsp_client(
        self, mock_connection_provider: MockLSPConnectionProvider
    ) -> None:
        """Test that get_client returns an LSPClient instance."""
        provider = LocalLSPClientProvider(mock_connection_provider)

        # Get client
        client = await provider.get_client()

        # Verify client is LSPClient instance
        assert isinstance(client, LSPClient)

        # Verify connection provider was called
        assert mock_connection_provider.get_connection_call_count == 1

        # Verify client has the correct connection
        assert client.lsp_connection is mock_connection_provider.mock_connection

    @pytest.mark.asyncio
    async def test_get_client_with_custom_timeout(
        self, mock_connection_provider: MockLSPConnectionProvider
    ) -> None:
        """Test that get_client passes custom timeout to LSPClient."""
        custom_timeout = 120.0
        provider = LocalLSPClientProvider(
            mock_connection_provider, timeout=custom_timeout
        )

        # Get client
        client = await provider.get_client()

        # Verify client has the custom timeout
        assert isinstance(client, LSPClient)
        assert client.timeout == custom_timeout

    @pytest.mark.asyncio
    async def test_get_client_with_default_timeout(
        self, mock_connection_provider: MockLSPConnectionProvider
    ) -> None:
        """Test that get_client uses default timeout when not specified."""
        provider = LocalLSPClientProvider(mock_connection_provider)

        # Get client
        client = await provider.get_client()

        # Verify client uses the default timeout from LSPClient
        from dbt_mcp.lsp.lsp_client import DEFAULT_LSP_TIMEOUT

        assert isinstance(client, LSPClient)
        assert client.timeout == DEFAULT_LSP_TIMEOUT

    @pytest.mark.asyncio
    async def test_get_client_with_none_timeout(
        self, mock_connection_provider: MockLSPConnectionProvider
    ) -> None:
        """Test that get_client handles None timeout correctly."""
        provider = LocalLSPClientProvider(mock_connection_provider, timeout=None)

        # Get client
        client = await provider.get_client()

        # Verify client uses the default timeout
        from dbt_mcp.lsp.lsp_client import DEFAULT_LSP_TIMEOUT

        assert isinstance(client, LSPClient)
        assert client.timeout == DEFAULT_LSP_TIMEOUT

    @pytest.mark.asyncio
    async def test_get_client_multiple_calls_create_new_clients(
        self, mock_connection_provider: MockLSPConnectionProvider
    ) -> None:
        """Test that multiple calls to get_client create new client instances."""
        provider = LocalLSPClientProvider(mock_connection_provider)

        # Get multiple clients
        client1 = await provider.get_client()
        client2 = await provider.get_client()
        client3 = await provider.get_client()

        # Each call should create a new LSPClient instance
        assert isinstance(client1, LSPClient)
        assert isinstance(client2, LSPClient)
        assert isinstance(client3, LSPClient)
        assert client1 is not client2
        assert client2 is not client3
        assert client1 is not client3

        # But all should use the same connection (from the provider)
        mock_connection = mock_connection_provider.mock_connection
        assert client1.lsp_connection is mock_connection
        assert client2.lsp_connection is mock_connection
        assert client3.lsp_connection is mock_connection

        # Connection provider should be called each time
        assert mock_connection_provider.get_connection_call_count == 3

    @pytest.mark.asyncio
    async def test_get_client_propagates_connection_provider_errors(
        self, mock_connection_provider: MockLSPConnectionProvider
    ) -> None:
        """Test that get_client propagates errors from connection provider."""
        provider = LocalLSPClientProvider(mock_connection_provider)

        # Configure connection provider to raise an error
        mock_connection_provider.should_raise_on_get_connection = RuntimeError(
            "Connection failed"
        )

        # Get client should propagate the error
        with pytest.raises(RuntimeError, match="Connection failed"):
            await provider.get_client()

    @pytest.mark.asyncio
    async def test_client_has_correct_connection(
        self, mock_connection_provider: MockLSPConnectionProvider
    ) -> None:
        """Test that created client has the correct connection reference."""
        provider = LocalLSPClientProvider(mock_connection_provider, timeout=45.0)

        # Get client
        client = await provider.get_client()

        # Verify client configuration
        assert isinstance(client, LSPClient)
        assert client.lsp_connection is mock_connection_provider.mock_connection
        assert client.timeout == 45.0

    @pytest.mark.asyncio
    async def test_integration_with_real_lsp_client_methods(
        self, mock_connection_provider: MockLSPConnectionProvider
    ) -> None:
        """Test that the returned client can call LSPClient methods."""
        # Setup mock connection with required methods
        mock_connection = MagicMock()
        mock_connection.compiled = MagicMock(return_value=True)
        mock_connection.send_request = AsyncMock(
            return_value={"nodes": [{"id": "test"}]}
        )
        mock_connection_provider.mock_connection = mock_connection

        provider = LocalLSPClientProvider(mock_connection_provider, timeout=60.0)

        # Get client
        client = await provider.get_client()

        # Verify we can call LSPClient methods
        assert isinstance(client, LSPClient)
        result = await client.get_column_lineage("model.test.table", "column_name")

        # Verify the method was called on the connection
        mock_connection.send_request.assert_called()
        assert "nodes" in result

    def test_provider_initialization(
        self, mock_connection_provider: MockLSPConnectionProvider
    ) -> None:
        """Test that provider initializes correctly."""
        # With custom timeout
        provider1 = LocalLSPClientProvider(mock_connection_provider, timeout=90.0)
        assert provider1.lsp_connection_provider is mock_connection_provider
        assert provider1.timeout == 90.0

        # Without timeout
        provider2 = LocalLSPClientProvider(mock_connection_provider)
        assert provider2.lsp_connection_provider is mock_connection_provider
        assert provider2.timeout is None

    @pytest.mark.asyncio
    async def test_provider_works_with_different_timeouts(
        self, mock_connection_provider: MockLSPConnectionProvider
    ) -> None:
        """Test that different provider instances can have different timeouts."""
        # Create providers with different timeouts
        provider1 = LocalLSPClientProvider(mock_connection_provider, timeout=30.0)
        provider2 = LocalLSPClientProvider(mock_connection_provider, timeout=60.0)
        provider3 = LocalLSPClientProvider(mock_connection_provider, timeout=120.0)

        # Get clients from each provider
        client1 = await provider1.get_client()
        client2 = await provider2.get_client()
        client3 = await provider3.get_client()

        # Verify each client has its respective timeout
        assert isinstance(client1, LSPClient)
        assert isinstance(client2, LSPClient)
        assert isinstance(client3, LSPClient)
        assert client1.timeout == 30.0
        assert client2.timeout == 60.0
        assert client3.timeout == 120.0
