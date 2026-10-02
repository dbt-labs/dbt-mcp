"""Bound response bodies before HTTPX decodes or buffers them."""

import asyncio
import threading
import zlib
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import httpx

from dbt_mcp.errors import InvalidParameterError


@dataclass(frozen=True)
class ResponseLimits:
    wire_bytes: int
    decoded_bytes: int


API_RESPONSE_LIMITS = ResponseLimits(4 * 1024 * 1024, 2 * 1024 * 1024)


class AdmissionGate:
    """Process-wide capacity, including a bounded number of waiting callers."""

    def __init__(self, *, active: int, pending: int):
        self._capacity = active
        self._pending_capacity = pending
        self._active = 0
        self._pending = 0
        self._lock = threading.Lock()

    @asynccontextmanager
    async def enter(self) -> AsyncIterator[None]:
        acquired = False
        with self._lock:
            if self._active < self._capacity:
                self._active += 1
                acquired = True
            elif self._pending < self._pending_capacity:
                self._pending += 1
            else:
                raise InvalidParameterError(
                    "Tool capacity limit reached; wait for another call to finish before retrying."
                )
        try:
            while not acquired:
                await asyncio.sleep(0.02)
                with self._lock:
                    if self._active < self._capacity:
                        self._pending -= 1
                        self._active += 1
                        acquired = True
            yield
        finally:
            with self._lock:
                if acquired:
                    self._active -= 1
                else:
                    self._pending -= 1


API_REQUEST_GATE = AdmissionGate(active=4, pending=8)


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
                raise InvalidParameterError(
                    "Response transfer limit exceeded; narrow the request or select a smaller resource."
                )
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
                        raise InvalidParameterError(
                            "Invalid compressed response; retry the request."
                        ) from error
                    decoded_size += len(decoded)
                    if decoded_size > self._limits.decoded_bytes:
                        raise InvalidParameterError(
                            "Response decoded size limit exceeded; narrow the request or select a smaller resource."
                        )
                    yield decoded
                    data = decoder.unconsumed_tail if decoder else b""
                    if decoder and decoder.unused_data:
                        raise InvalidParameterError(
                            "Concatenated or trailing compressed response data is unsupported."
                        )
        if decoder and not decoder.eof:
            raise InvalidParameterError(
                "Incomplete compressed response; retry the request."
            )

    async def aclose(self) -> None:
        await self._stream.aclose()


def response_limit_hook(
    limits: ResponseLimits = API_RESPONSE_LIMITS,
) -> Callable[..., Any]:
    async def limit_response(response: httpx.Response) -> None:
        encoding = response.headers.get("content-encoding", "identity").strip().lower()
        if encoding not in ("identity", "gzip", "deflate"):
            raise InvalidParameterError(
                "Unsupported response compression; use identity, gzip, or deflate."
            )
        length = response.headers.get("content-length")
        if length and length.isdecimal() and int(length) > limits.wire_bytes:
            raise InvalidParameterError(
                "Response transfer limit exceeded; narrow the request or select a smaller resource."
            )
        assert isinstance(response.stream, httpx.AsyncByteStream)
        response.stream = _BoundedStream(response.stream, encoding, limits)
        # Our stream owns decompression; prevent HTTPX from decoding it again.
        response.headers.pop("content-encoding", None)
        response.headers.pop("content-length", None)

    return limit_response


BOUNDED_RESPONSE_HOOKS = {"response": [response_limit_hook()]}
