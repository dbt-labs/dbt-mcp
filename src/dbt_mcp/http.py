"""Bound response bodies before HTTPX decodes or buffers them."""

import asyncio
import logging
import os
import threading
import time
import zlib
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

import httpx

from dbt_mcp.errors import (
    ConfigurationError,
    ResponseLimitError,
    ToolCapacityError,
    UpstreamResponseError,
)

logger = logging.getLogger(__name__)

_WAIT_OBSERVER: ContextVar[Callable[[bool], None] | None] = ContextVar(
    "admission_wait_observer", default=None
)


@contextmanager
def observe_admission_wait(observer: Callable[[bool], None]) -> Iterator[None]:
    """Let a host observe queue entry/exit without coupling the gate to host policy."""
    token = _WAIT_OBSERVER.set(observer)
    try:
        yield
    finally:
        _WAIT_OBSERVER.reset(token)


@dataclass(frozen=True)
class ResponseLimits:
    wire_bytes: int
    decoded_bytes: int


API_RESPONSE_LIMITS = ResponseLimits(4 * 1024 * 1024, 2 * 1024 * 1024)


class AdmissionGate:
    """Process-wide capacity, including a bounded number of waiting callers."""

    def __init__(
        self,
        *,
        active: int,
        pending: int,
        wait_timeout: float = 120,
        name: str = "tool",
        environment_prefix: str | None = None,
    ):
        if environment_prefix:
            try:
                active = int(os.getenv(f"{environment_prefix}_ACTIVE", str(active)))
                pending = int(os.getenv(f"{environment_prefix}_PENDING", str(pending)))
                wait_timeout = float(
                    os.getenv(
                        f"{environment_prefix}_QUEUE_TIMEOUT_SECONDS", str(wait_timeout)
                    )
                )
            except ValueError as error:
                raise ConfigurationError(
                    "Admission budgets must be numeric."
                ) from error
        if active < 1 or pending < 0 or not 0 < wait_timeout < float("inf"):
            raise ConfigurationError(
                "Admission requires active >= 1, pending >= 0 and a finite positive queue timeout."
            )
        self._active_slots = threading.BoundedSemaphore(active)
        self._admitted_slots = threading.BoundedSemaphore(active + pending)
        self._wait_timeout = wait_timeout
        self._log_fields = {
            "admission_gate": name,
            "active_limit": active,
            "pending_limit": pending,
        }

    def _observe(self, event: str, wait_seconds: float = 0) -> None:
        logger.info(
            "Tool admission",
            extra=self._log_fields
            | {"admission_event": event, "wait_seconds": wait_seconds},
        )

    @asynccontextmanager
    async def enter(self) -> AsyncIterator[None]:
        # Thread-safe primitives keep the process-wide gate usable from multiple
        # event loops. Nonblocking acquisition avoids orphaned thread waiters on cancellation.
        if not self._admitted_slots.acquire(blocking=False):
            self._observe("rejected")
            raise ToolCapacityError(
                "Tool capacity is temporarily exhausted; retry later."
            )
        acquired = False
        start = time.monotonic()
        try:
            acquired = self._active_slots.acquire(blocking=False)
            if not acquired:
                self._observe("queued")
                observer = _WAIT_OBSERVER.get()
                if observer:
                    observer(True)
                try:
                    async with asyncio.timeout(self._wait_timeout):
                        while not acquired:
                            await asyncio.sleep(0.02)
                            acquired = self._active_slots.acquire(blocking=False)
                except TimeoutError as error:
                    self._observe("queue_timeout", time.monotonic() - start)
                    raise ToolCapacityError(
                        "Tool capacity is temporarily exhausted; retry later."
                    ) from error
                finally:
                    if observer:
                        observer(False)
            self._observe("started", time.monotonic() - start)
            yield
        finally:
            if acquired:
                self._active_slots.release()
                self._observe("finished")
            else:
                self._observe("left_queue", time.monotonic() - start)
            self._admitted_slots.release()


API_REQUEST_GATE = AdmissionGate(
    active=4, pending=128, name="api", environment_prefix="DBT_MCP_API"
)


class _BoundedStream(httpx.AsyncByteStream):
    def __init__(
        self, stream: httpx.AsyncByteStream, encoding: str, limits: ResponseLimits
    ):
        self._stream = stream
        self._encoding = encoding
        self._limits = limits

    async def __aiter__(self) -> AsyncIterator[bytes]:
        wire_size = decoded_size = 0
        decoder = None
        if self._encoding in ("gzip", "deflate"):
            decoder = zlib.decompressobj(31 if self._encoding == "gzip" else 15)
        async for chunk in self._stream:
            wire_size += len(chunk)
            if wire_size > self._limits.wire_bytes:
                raise ResponseLimitError("Response transfer limit exceeded.")
            # Bound each decompressor allocation, including highly compressed input.
            for start in range(0, len(chunk), 64 * 1024):
                data = chunk[start : start + 64 * 1024]
                while data:
                    remaining = self._limits.decoded_bytes - decoded_size
                    try:
                        decoded = (
                            decoder.decompress(data, min(64 * 1024, remaining + 1))
                            if decoder
                            else data
                        )
                    except zlib.error as error:
                        raise UpstreamResponseError(
                            "Invalid compressed response."
                        ) from error
                    decoded_size += len(decoded)
                    if decoded_size > self._limits.decoded_bytes:
                        raise ResponseLimitError(
                            "Response decoded size limit exceeded."
                        )
                    yield decoded
                    data = decoder.unconsumed_tail if decoder else b""
                    if decoder and decoder.unused_data:
                        raise UpstreamResponseError(
                            "Concatenated or trailing compressed response data is unsupported."
                        )
        if decoder and not decoder.eof:
            raise UpstreamResponseError("Incomplete compressed response.")

    async def aclose(self) -> None:
        await self._stream.aclose()


def response_limit_hook(
    limits: ResponseLimits = API_RESPONSE_LIMITS,
) -> Callable[..., Any]:
    async def limit_response(response: httpx.Response) -> None:
        encoding = response.headers.get("content-encoding", "identity").strip().lower()
        if encoding not in ("identity", "gzip", "deflate"):
            raise UpstreamResponseError(
                f"Unsupported upstream Content-Encoding: {encoding!r}. Expected identity, gzip or deflate."
            )
        length = response.headers.get("content-length")
        if length and length.isdecimal() and int(length) > limits.wire_bytes:
            raise ResponseLimitError("Response transfer limit exceeded.")
        assert isinstance(response.stream, httpx.AsyncByteStream)
        response.stream = _BoundedStream(response.stream, encoding, limits)
        # Our stream owns decompression; prevent HTTPX from decoding it again.
        response.headers.pop("content-encoding", None)
        response.headers.pop("content-length", None)

    return limit_response


BOUNDED_RESPONSE_HOOKS = {"response": [response_limit_hook()]}
