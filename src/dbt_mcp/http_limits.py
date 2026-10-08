"""Bound HTTP response bodies before HTTPX decodes or buffers them."""

import logging
import zlib
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx

from dbt_mcp.errors import ResponseLimitError, UpstreamResponseError
from dbt_mcp.resource_limits import ResponseLimits, ResponseObserver, ResponseSize

logger = logging.getLogger(__name__)


def _observe(observer: ResponseObserver | None, size: ResponseSize) -> None:
    if observer is not None:
        try:
            observer(size)
        except Exception:
            logger.warning("Response size observer failed", exc_info=True)


class _BoundedStream(httpx.AsyncByteStream):
    def __init__(
        self,
        stream: httpx.AsyncByteStream,
        encoding: str,
        limits: ResponseLimits,
        response_type: str,
        observer: ResponseObserver | None,
    ):
        self._stream = stream
        self._encoding = encoding
        self._limits = limits
        self._response_type = response_type
        self._observer = observer
        self._wire_size = self._decoded_size = 0
        self._complete = self._observed = False

    def _observe(self) -> None:
        if not self._observed:
            self._observed = True
            _observe(
                self._observer,
                ResponseSize(
                    self._response_type,
                    self._wire_size,
                    self._decoded_size,
                    self._complete,
                ),
            )

    async def __aiter__(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self._read():
                yield chunk
        finally:
            self._observe()

    async def _read(self) -> AsyncIterator[bytes]:
        decoder = None
        if self._encoding in ("gzip", "deflate"):
            decoder = zlib.decompressobj(31 if self._encoding == "gzip" else 15)
        async for chunk in self._stream:
            self._wire_size += len(chunk)
            if (
                self._limits.wire_bytes is not None
                and self._wire_size > self._limits.wire_bytes
            ):
                raise ResponseLimitError("Response transfer limit exceeded.")
            # Bound each decompressor allocation, including highly compressed input.
            for start in range(0, len(chunk), 64 * 1024):
                data = chunk[start : start + 64 * 1024]
                while data:
                    allocation = 64 * 1024
                    if self._limits.decoded_bytes is not None:
                        allocation = min(
                            allocation,
                            self._limits.decoded_bytes - self._decoded_size + 1,
                        )
                    try:
                        decoded = (
                            decoder.decompress(data, allocation) if decoder else data
                        )
                    except zlib.error as error:
                        raise UpstreamResponseError(
                            "Invalid compressed response."
                        ) from error
                    self._decoded_size += len(decoded)
                    if (
                        self._limits.decoded_bytes is not None
                        and self._decoded_size > self._limits.decoded_bytes
                    ):
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
        self._complete = True

    async def aclose(self) -> None:
        try:
            await self._stream.aclose()
        finally:
            self._observe()


def response_limit_hook(
    limits: ResponseLimits,
    *,
    response_type: str = "http",
    observer: ResponseObserver | None = None,
) -> Callable[..., Any]:
    async def limit_response(response: httpx.Response) -> None:
        encoding = response.headers.get("content-encoding", "identity").strip().lower()
        if encoding not in ("identity", "gzip", "deflate"):
            _observe(observer, ResponseSize(response_type, 0, 0, False))
            raise UpstreamResponseError(
                f"Unsupported upstream Content-Encoding: {encoding!r}. Expected identity, gzip or deflate."
            )
        length = response.headers.get("content-length")
        if (
            limits.wire_bytes is not None
            and length
            and length.isdecimal()
            and int(length) > limits.wire_bytes
        ):
            _observe(observer, ResponseSize(response_type, 0, 0, False))
            raise ResponseLimitError("Response transfer limit exceeded.")
        assert isinstance(response.stream, httpx.AsyncByteStream)
        response.stream = _BoundedStream(
            response.stream, encoding, limits, response_type, observer
        )
        # Our stream owns decompression; prevent HTTPX from decoding it again.
        response.headers.pop("content-encoding", None)
        response.headers.pop("content-length", None)

    return limit_response
