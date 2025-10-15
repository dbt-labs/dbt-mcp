"""Unit tests for LocalLSPConnectionProvider class."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.lsp.lsp_binary_manager import LspBinaryInfo
from dbt_mcp.lsp.providers.local_lsp_connection_provider import (
    LocalLSPConnectionProvider,
)
from dbt_mcp.lsp.lsp_connection import SocketLSPConnection


@pytest.fixture
def lsp_binary_info(tmp_path) -> LspBinaryInfo:
    """Create a test LSP binary info."""
    binary_path = tmp_path / "dbt-lsp"
    binary_path.touch()
    return LspBinaryInfo(path=str(binary_path), version="1.0.0")


@pytest.fixture
def project_dir(tmp_path) -> str:
    """Create a test project directory."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    return str(project_dir)


class TestLocalLSPConnectionProvider:
    """Test LocalLSPConnectionProvider class."""

    @pytest.mark.asyncio
    async def test_get_connection_creates_connection_on_first_call(
        self, lsp_binary_info: LspBinaryInfo, project_dir: str
    ) -> None:
        """Test that get_connection creates a new connection on first call."""
        provider = LocalLSPConnectionProvider(lsp_binary_info, project_dir)

        # Mock the SocketLSPConnection
        mock_connection = MagicMock(spec=SocketLSPConnection)
        mock_connection.start = AsyncMock()
        mock_connection.initialize = AsyncMock()

        with patch(
            "dbt_mcp.lsp.providers.local_lsp_connection_provider.SocketLSPConnection",
            return_value=mock_connection,
        ) as mock_conn_class:
            connection = await provider.get_connection()

            # Verify connection was created with correct arguments
            mock_conn_class.assert_called_once_with(
                binary_path=lsp_binary_info.path,
                args=[],
                cwd=project_dir,
            )

            # Verify connection lifecycle methods were called
            mock_connection.start.assert_called_once()
            mock_connection.initialize.assert_called_once()

            # Verify the connection is returned
            assert connection == mock_connection
            assert provider.lsp_connection == mock_connection

    @pytest.mark.asyncio
    async def test_get_connection_returns_existing_connection(
        self, lsp_binary_info: LspBinaryInfo, project_dir: str
    ) -> None:
        """Test that get_connection returns the same connection on subsequent calls."""
        provider = LocalLSPConnectionProvider(lsp_binary_info, project_dir)

        # Mock the connection
        mock_connection = MagicMock(spec=SocketLSPConnection)
        mock_connection.start = AsyncMock()
        mock_connection.initialize = AsyncMock()

        with patch(
            "dbt_mcp.lsp.providers.local_lsp_connection_provider.SocketLSPConnection",
            return_value=mock_connection,
        ) as mock_conn_class:
            # First call
            connection1 = await provider.get_connection()

            # Second call
            connection2 = await provider.get_connection()

            # Third call
            connection3 = await provider.get_connection()

            # Connection should only be created once
            mock_conn_class.assert_called_once()
            mock_connection.start.assert_called_once()
            mock_connection.initialize.assert_called_once()

            # All calls should return the same instance
            assert connection1 is connection2
            assert connection2 is connection3
            assert connection1 is mock_connection

    @pytest.mark.asyncio
    async def test_get_connection_handles_start_failure(
        self, lsp_binary_info: LspBinaryInfo, project_dir: str
    ) -> None:
        """Test that get_connection handles connection start failure."""
        provider = LocalLSPConnectionProvider(lsp_binary_info, project_dir)

        # Mock connection that fails to start
        mock_connection = MagicMock(spec=SocketLSPConnection)
        mock_connection.start = AsyncMock(side_effect=RuntimeError("Start failed"))
        mock_connection.initialize = AsyncMock()

        with patch(
            "dbt_mcp.lsp.providers.local_lsp_connection_provider.SocketLSPConnection",
            return_value=mock_connection,
        ):
            with pytest.raises(
                RuntimeError, match="Failed to establish LSP connection"
            ):
                await provider.get_connection()

            # Verify connection was cleaned up
            assert provider.lsp_connection is None

    @pytest.mark.asyncio
    async def test_get_connection_handles_initialize_failure(
        self, lsp_binary_info: LspBinaryInfo, project_dir: str
    ) -> None:
        """Test that get_connection handles connection initialize failure."""
        provider = LocalLSPConnectionProvider(lsp_binary_info, project_dir)

        # Mock connection that fails to initialize
        mock_connection = MagicMock(spec=SocketLSPConnection)
        mock_connection.start = AsyncMock()
        mock_connection.initialize = AsyncMock(
            side_effect=RuntimeError("Initialize failed")
        )

        with patch(
            "dbt_mcp.lsp.providers.local_lsp_connection_provider.SocketLSPConnection",
            return_value=mock_connection,
        ):
            with pytest.raises(
                RuntimeError, match="Failed to establish LSP connection"
            ):
                await provider.get_connection()

            # Verify connection was cleaned up
            assert provider.lsp_connection is None

    @pytest.mark.asyncio
    async def test_cleanup_connection_stops_connection(
        self, lsp_binary_info: LspBinaryInfo, project_dir: str
    ) -> None:
        """Test that cleanup_connection properly stops the connection."""
        provider = LocalLSPConnectionProvider(lsp_binary_info, project_dir)

        # Setup a connection
        mock_connection = MagicMock(spec=SocketLSPConnection)
        mock_connection.start = AsyncMock()
        mock_connection.initialize = AsyncMock()
        mock_connection.stop = AsyncMock()

        with patch(
            "dbt_mcp.lsp.providers.local_lsp_connection_provider.SocketLSPConnection",
            return_value=mock_connection,
        ):
            # Create connection
            await provider.get_connection()
            assert provider.lsp_connection is not None

            # Cleanup
            await provider.cleanup_connection()

            # Verify stop was called
            mock_connection.stop.assert_called_once()

            # Verify connection was set to None
            assert provider.lsp_connection is None

    @pytest.mark.asyncio
    async def test_cleanup_connection_handles_no_connection(
        self, lsp_binary_info: LspBinaryInfo, project_dir: str
    ) -> None:
        """Test that cleanup_connection handles no existing connection gracefully."""
        provider = LocalLSPConnectionProvider(lsp_binary_info, project_dir)

        # No connection has been created
        assert provider.lsp_connection is None

        # Should not raise
        await provider.cleanup_connection()

        assert provider.lsp_connection is None

    @pytest.mark.asyncio
    async def test_cleanup_connection_handles_stop_failure(
        self, lsp_binary_info: LspBinaryInfo, project_dir: str
    ) -> None:
        """Test that cleanup_connection handles stop failure gracefully."""
        provider = LocalLSPConnectionProvider(lsp_binary_info, project_dir)

        # Setup a connection that fails to stop
        mock_connection = MagicMock(spec=SocketLSPConnection)
        mock_connection.start = AsyncMock()
        mock_connection.initialize = AsyncMock()
        mock_connection.stop = AsyncMock(side_effect=RuntimeError("Stop failed"))

        with patch(
            "dbt_mcp.lsp.providers.local_lsp_connection_provider.SocketLSPConnection",
            return_value=mock_connection,
        ):
            # Create connection
            await provider.get_connection()

            # Cleanup should not raise
            await provider.cleanup_connection()

            # Verify stop was attempted
            mock_connection.stop.assert_called_once()

            # Connection should still be set to None
            assert provider.lsp_connection is None

    @pytest.mark.asyncio
    async def test_connection_lifecycle_integration(
        self, lsp_binary_info: LspBinaryInfo, project_dir: str
    ) -> None:
        """Test complete connection lifecycle (create, use, cleanup)."""
        provider = LocalLSPConnectionProvider(lsp_binary_info, project_dir)

        # Mock connection
        mock_connection = MagicMock(spec=SocketLSPConnection)
        mock_connection.start = AsyncMock()
        mock_connection.initialize = AsyncMock()
        mock_connection.stop = AsyncMock()

        with patch(
            "dbt_mcp.lsp.providers.local_lsp_connection_provider.SocketLSPConnection",
            return_value=mock_connection,
        ):
            # Create connection
            connection1 = await provider.get_connection()
            assert connection1 is mock_connection
            assert provider.lsp_connection is mock_connection

            # Get connection again (should return same)
            connection2 = await provider.get_connection()
            assert connection2 is connection1

            # Cleanup
            await provider.cleanup_connection()
            assert provider.lsp_connection is None

            # Get connection again (should create new)
            connection3 = await provider.get_connection()
            assert connection3 is mock_connection

            # Verify lifecycle methods were called correctly
            assert mock_connection.start.call_count == 2
            assert mock_connection.initialize.call_count == 2
            assert mock_connection.stop.call_count == 1
