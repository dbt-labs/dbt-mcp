import asyncio
import time
from typing import Protocol


class RefreshStrategy(Protocol):
    """Protocol for handling token refresh timing and waiting."""

    async def wait_until_refresh_needed(self, expires_at: int) -> None:
        """
        Wait until token refresh is needed, then return.

        Args:
            expires_at: Token expiration time as Unix timestamp
        """
        ...

    async def wait_after_error(self) -> None:
        """
        Wait an appropriate amount of time after an error before retrying.
        """
        ...


class DefaultRefreshStrategy:
    """Default strategy that refreshes tokens with a buffer before expiry."""

    def __init__(self, buffer_seconds: int = 300, error_retry_delay: float = 5.0):
        """
        Initialize with timing configuration.

        Args:
            buffer_seconds: How many seconds before expiry to refresh
                (default: 5 minutes)
            error_retry_delay: How many seconds to wait before retrying after an error
                (default: 5 seconds)
        """
        self.buffer_seconds = buffer_seconds
        self.error_retry_delay = error_retry_delay

    async def wait_until_refresh_needed(self, expires_at: int) -> None:
        """Wait until refresh is needed (buffer seconds before expiry)."""
        current_time = time.time()
        refresh_time = expires_at - self.buffer_seconds
        time_until_refresh = max(refresh_time - current_time, 0)

        if time_until_refresh > 0:
            await asyncio.sleep(time_until_refresh)

    async def wait_after_error(self) -> None:
        """Wait the configured error retry delay before retrying."""
        await asyncio.sleep(self.error_retry_delay)
