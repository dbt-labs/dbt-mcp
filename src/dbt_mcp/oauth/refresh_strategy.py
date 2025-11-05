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


class MockRefreshStrategy:
    """Mock refresh strategy for testing that allows controlling all timing behavior."""

    def __init__(self, wait_seconds: float = 1.0):
        """
        Initialize mock refresh strategy.

        Args:
            wait_seconds: Number of seconds to wait for testing simulations
        """
        self.wait_seconds = wait_seconds
        self.wait_calls: list[int] = []
        self.wait_durations: list[float] = []
        self.error_wait_calls: int = 0

    async def wait_until_refresh_needed(self, expires_at: int) -> None:
        """Record the call and simulate waiting for the configured duration."""
        self.wait_calls.append(expires_at)
        self.wait_durations.append(self.wait_seconds)
        await asyncio.sleep(self.wait_seconds)

    async def wait_after_error(self) -> None:
        """Record the error wait call and simulate waiting for configured duration."""
        self.error_wait_calls += 1
        await asyncio.sleep(self.wait_seconds)

    def reset(self) -> None:
        """Reset all recorded calls."""
        self.wait_calls.clear()
        self.wait_durations.clear()
        self.error_wait_calls = 0

    @property
    def call_count(self) -> int:
        """Get the number of times wait_until_refresh_needed was called."""
        return len(self.wait_calls)
