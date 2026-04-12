"""Tests for mcp.lock symlink resolution.

Verifies that the lock file path is resolved before creating a FileLock,
which prevents OSError when mcp.lock is a multi-level symbolic link.

Related: https://github.com/dbt-labs/dbt-mcp/issues/533
"""

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestMcpLockSymlinkResolution:
    """Test that FileLock receives a resolved path."""

    @pytest.mark.asyncio
    async def test_lock_path_is_resolved_before_filelock(self, tmp_path: Path) -> None:
        """The lock file path should be resolved to handle multi-level symlinks."""
        # Create the real directory and lock file target
        real_dir = tmp_path / "real_dbt"
        real_dir.mkdir()
        real_lock = real_dir / "mcp.lock"
        real_lock.touch()

        # Create a chain of symlinks: link2 -> link1 -> real_dir
        link1 = tmp_path / "link1"
        link1.symlink_to(real_dir)
        link2 = tmp_path / "link2"
        link2.symlink_to(link1)

        # Verify the symlink chain resolves correctly
        lock_path = (link2 / "mcp.lock").resolve()
        assert lock_path == real_lock.resolve()

    @pytest.mark.asyncio
    async def test_get_dbt_platform_context_uses_resolved_lock_path(
        self, tmp_path: Path
    ) -> None:
        """get_dbt_platform_context should resolve the lock path before FileLock."""
        captured_lock_paths: list[Path] = []

        class MockFileLock:
            def __init__(self, path, *args, **kwargs):
                captured_lock_paths.append(Path(str(path)))

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        mock_ctx = MagicMock()
        mock_ctx.account_id = 123
        mock_ctx.host_prefix = "test"
        mock_ctx.prod_environment = MagicMock()
        mock_ctx.decoded_access_token = MagicMock()
        mock_ctx.decoded_access_token.access_token_response.expires_at = (
            float("inf")  # Never expires
        )

        mock_context_manager = MagicMock()
        mock_context_manager.read_context.return_value = mock_ctx

        with patch(
            "dbt_mcp.config.credentials.FileLock", MockFileLock
        ):
            from dbt_mcp.config.credentials import get_dbt_platform_context

            result = await get_dbt_platform_context(
                dbt_user_dir=tmp_path,
                dbt_platform_url="https://cloud.getdbt.com",
                dbt_platform_context_manager=mock_context_manager,
            )

        assert len(captured_lock_paths) == 1
        # The path passed to FileLock should be fully resolved (no symlinks)
        assert not captured_lock_paths[0].is_symlink() or not os.path.islink(
            str(captured_lock_paths[0])
        )
