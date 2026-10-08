import asyncio
import gzip
import zlib
from dataclasses import replace
from unittest.mock import patch

import httpx
import pytest

from dbt_mcp.discovery.client import execute_query
from dbt_mcp.errors import ResponseLimitError
from dbt_mcp.http_limits import response_limit_hook
from dbt_mcp.product_docs.client import ProductDocsClient
from dbt_mcp.resource_limits import (
    HttpConfig,
    ProductDocsConfig,
    ResponseLimits,
    ResponseSize,
)
from tests.unit.dbt_admin.test_artifact_limits import Chunks


@pytest.mark.parametrize("encoding", ["identity", "gzip", "deflate"])
async def test_discovery_observes_wire_and_decoded_size(
    unit_discovery_config, encoding
):
    observations = []
    decoded = b'{"data":{"name":"model"}}'
    wire = {
        "identity": decoded,
        "gzip": gzip.compress(decoded),
        "deflate": zlib.compress(decoded),
    }[encoding]
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"content-encoding": encoding},
            stream=Chunks([wire[:5], wire[5:]]),
        )
    )
    client_class = httpx.AsyncClient
    config = replace(
        unit_discovery_config, http_config=HttpConfig(observer=observations.append)
    )
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kwargs: client_class(transport=transport, **kwargs),
    ):
        assert await execute_query("query", {}, config=config) == {
            "data": {"name": "model"}
        }
    assert observations == [ResponseSize("discovery", len(wire), len(decoded), True)]


@pytest.mark.parametrize(
    "limits", [ResponseLimits(wire_bytes=5), ResponseLimits(decoded_bytes=5)]
)
async def test_rejected_stream_is_observed_once(limits):
    observations = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=Chunks([b"123456", b"unread"]))
        ),
        event_hooks={
            "response": [
                response_limit_hook(
                    limits, response_type="test", observer=observations.append
                )
            ]
        },
    ) as client:
        with pytest.raises(ResponseLimitError):
            await client.get("https://example.com")
    assert observations == [
        ResponseSize("test", 6, 0 if limits.wire_bytes else 6, False)
    ]


async def test_cancelled_and_unread_responses_are_observed_once():
    observations = []
    started = asyncio.Event()

    class WaitingStream(Chunks):
        async def __aiter__(self):
            yield b"123"
            started.set()
            await asyncio.Event().wait()

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=WaitingStream([]))
        ),
        event_hooks={
            "response": [
                response_limit_hook(ResponseLimits(), observer=observations.append)
            ]
        },
    ) as client:
        task = asyncio.create_task(client.get("https://example.com"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with client.stream("GET", "https://example.com"):
            pass
    assert observations == [
        ResponseSize("http", 3, 3, False),
        ResponseSize("http", 0, 0, False),
    ]


async def test_observer_failure_does_not_fail_response():
    def fail(size):
        raise RuntimeError("telemetry unavailable")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=Chunks([b"ok"]))
        ),
        event_hooks={
            "response": [response_limit_hook(ResponseLimits(), observer=fail)]
        },
    ) as client:
        assert (await client.get("https://example.com")).text == "ok"


async def test_docs_page_larger_than_cache_is_returned_without_caching():
    observations = []
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, stream=Chunks([b"large page"]))
    )
    client_class = httpx.AsyncClient
    docs = ProductDocsClient(
        http_config=HttpConfig(observer=observations.append),
        limits=ProductDocsConfig(cache_bytes=4),
    )
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kwargs: client_class(transport=transport, **kwargs),
    ):
        assert await docs.get_page("https://docs.getdbt.com/docs") == "large page"
        assert await docs.get_page("https://docs.getdbt.com/docs") == "large page"
    assert observations == [ResponseSize("product_docs.page", 10, 10, True)] * 2
