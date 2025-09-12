"""
Mock refresh strategy for testing OAuth token management.
"""

import asyncio


class MockRefreshStrategy:
    """Mock refresh strategy for testing that allows controlling all timing behavior."""

    def __init__(self, should_wait: bool = True):
        """
        Initialize mock refresh strategy.

        Args:
            should_wait: Whether to simulate waiting or return immediately
        """
        self.should_wait = should_wait
        self.wait_calls: list[int] = []
        self.wait_durations: list[float] = []
        self.error_wait_calls: int = 0

    async def wait_until_refresh_needed(self, expires_at: int) -> None:
        """Record the call and optionally simulate waiting."""
        self.wait_calls.append(expires_at)

        if self.should_wait:
            # Simulate some waiting time for testing
            wait_duration = 0.1  # Short wait for tests
            self.wait_durations.append(wait_duration)
            await asyncio.sleep(wait_duration)
        else:
            # Return immediately for faster tests
            self.wait_durations.append(0.0)

    async def wait_after_error(self) -> None:
        """Record the error wait call and optionally simulate waiting."""
        self.error_wait_calls += 1

        if self.should_wait:
            # Simulate a short sleep for testing
            await asyncio.sleep(0.01)  # Very short sleep for tests

    def reset(self) -> None:
        """Reset all recorded calls."""
        self.wait_calls.clear()
        self.wait_durations.clear()
        self.error_wait_calls = 0

    @property
    def call_count(self) -> int:
        """Get the number of times wait_until_refresh_needed was called."""
        return len(self.wait_calls)
