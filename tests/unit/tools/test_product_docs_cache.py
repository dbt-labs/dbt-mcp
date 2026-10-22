from unittest.mock import patch

import httpx
from dbt_mcp.product_docs.tools import ProductDocsToolContext, get_product_doc_pages
from dbt_mcp.product_docs.client import ProductDocsClient
from dbt_mcp.resource_limits import ProductDocsConfig


async def test_long_document_url_is_fetched_and_cached():
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, text="# Page content")

    client_class = httpx.AsyncClient
    transport = httpx.MockTransport(respond)
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kwargs: client_class(transport=transport, **kwargs),
    ):
        context = ProductDocsToolContext()
        path = "/docs/" + "x" * 5000
        first = await get_product_doc_pages.fn(context, [path])
        cached = await get_product_doc_pages.fn(context, [path])

    assert first.pages[0].content == "# Page content"
    assert first.pages[0].error is None
    assert cached == first
    assert len(requests) == 1


async def test_document_urls_count_toward_cache_eviction():
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, text="# Page content")

    client_class = httpx.AsyncClient
    transport = httpx.MockTransport(respond)
    # A small budget exercises the real eviction wiring through the public tool.
    with (
        patch(
            "httpx.AsyncClient",
            side_effect=lambda **kwargs: client_class(transport=transport, **kwargs),
        ),
    ):
        context = ProductDocsToolContext(
            client=ProductDocsClient(limits=ProductDocsConfig(cache_bytes=1024))
        )
        paths = ["/docs/" + character * 700 for character in ("a", "b")]
        for path in (*paths, paths[0]):
            result = await get_product_doc_pages.fn(context, [path])
            assert result.pages[0].content == "# Page content"
            assert result.pages[0].error is None

    assert len(requests) == 3
    assert requests[0].url == requests[2].url
