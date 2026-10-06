"""Bound HTTP response bodies before HTTPX decodes or buffers them."""

import zlib
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx

from dbt_mcp.errors import ResponseLimitError, UpstreamResponseError
from dbt_mcp.resource_limits import ResponseLimits


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
    limits: ResponseLimits,
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
