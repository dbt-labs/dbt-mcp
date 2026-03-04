import pytest

from dbt_mcp.product_docs.client import ProductDocsClient
from dbt_mcp.product_docs.tools import (
    ProductDocsToolContext,
    get_product_doc_pages,
    search_product_docs,
)


@pytest.fixture
def context() -> ProductDocsToolContext:
    return ProductDocsToolContext()


@pytest.fixture
def client() -> ProductDocsClient:
    return ProductDocsClient()


class TestProductDocsClient:
    @pytest.mark.asyncio
    async def test_get_index(self, client: ProductDocsClient):
        index = await client.get_index()

        assert isinstance(index, list)
        assert len(index) > 0
        for entry in index[:3]:
            assert "title" in entry
            assert "url" in entry

    @pytest.mark.asyncio
    async def test_search_index(self, client: ProductDocsClient):
        results = await client.search_index("incremental models")

        assert isinstance(results, list)
        assert len(results) > 0
        assert any("incremental" in r["title"].lower() for r in results)

    @pytest.mark.asyncio
    async def test_get_page(self, client: ProductDocsClient):
        content = await client.get_page(
            "https://docs.getdbt.com/docs/build/incremental-models.md"
        )

        assert isinstance(content, str)
        assert len(content) > 0

    @pytest.mark.asyncio
    async def test_search_full_text(self, client: ProductDocsClient):
        results = await client.search_full_text(["incremental"], max_results=5)

        assert isinstance(results, list)
        assert len(results) > 0
        for entry in results:
            assert "url" in entry
            assert "title" in entry


class TestProductDocsTools:
    @pytest.mark.asyncio
    async def test_search_product_docs(self, context: ProductDocsToolContext):
        result = await search_product_docs.fn(context, "incremental models")

        assert result.error is None
        assert result.total_matches > 0
        assert len(result.results) > 0

    @pytest.mark.asyncio
    async def test_search_product_docs_empty_query(
        self, context: ProductDocsToolContext
    ):
        result = await search_product_docs.fn(context, "  ")

        assert result.error is not None
        assert result.total_matches == 0

    @pytest.mark.asyncio
    async def test_get_single_page(self, context: ProductDocsToolContext):
        result = await get_product_doc_pages.fn(
            context, ["/docs/build/incremental-models"]
        )

        assert len(result.pages) == 1
        page = result.pages[0]
        assert page.error is None
        assert len(page.content) > 0
        assert page.url.startswith("https://docs.getdbt.com/")

    @pytest.mark.asyncio
    async def test_get_page_not_found(self, context: ProductDocsToolContext):
        result = await get_product_doc_pages.fn(
            context, ["/docs/this-page-does-not-exist-abc123"]
        )

        assert len(result.pages) == 1
        assert result.pages[0].error is not None

    @pytest.mark.asyncio
    async def test_get_multiple_pages(self, context: ProductDocsToolContext):
        result = await get_product_doc_pages.fn(
            context,
            ["/docs/build/incremental-models", "/docs/build/models"],
        )

        assert len(result.pages) == 2
        for page in result.pages:
            assert page.url
            assert page.error is None
            assert len(page.content) > 0
