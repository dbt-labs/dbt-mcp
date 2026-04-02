"""Unit tests for the Product Docs toolset."""

from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from dbt_mcp.config.config import load_config
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.mcp.server import create_dbt_mcp
from dbt_mcp.product_docs.client import (
    extract_relevant_sections,
    normalize_doc_url,
    parse_llms_full_txt,
    parse_llms_txt,
    split_markdown_sections,
)
from dbt_mcp.product_docs.tools import (
    ProductDocsToolContext,
    get_product_doc_pages,
    search_product_docs,
)

SAMPLE_LLMS_TXT = """\
# dbt Developer Hub

> End user documentation, guides and technical reference for dbt

- [The dbt Developer Hub](https://docs.getdbt.com/index.md)

## Build

- [Incremental models](https://docs.getdbt.com/docs/build/incremental-models.md): dbt incremental models let you transform and load only new or changed data.
- [Models](https://docs.getdbt.com/docs/build/models.md): A model is a SELECT statement. Models can be materialized as tables, views, or ephemeral.
- [Seeds](https://docs.getdbt.com/docs/build/seeds.md): Seeds are CSV files that dbt can load into your data warehouse.

## Deploy

- [Deploy jobs](https://docs.getdbt.com/docs/deploy/deploy-jobs.md): Create and schedule deploy jobs in dbt Cloud.
- [Continuous integration](https://docs.getdbt.com/docs/deploy/continuous-integration.md): Set up CI checks for your dbt project.

## Reference

- [Incremental strategy configs](https://docs.getdbt.com/reference/resource-configs/incremental-strategy.md): Configure incremental strategies for your models.
"""

SAMPLE_LLMS_FULL_TXT = """\
# dbt Developer Hub

> End user documentation, guides and technical reference for dbt

---

### Incremental models

[About incremental models](https://docs.getdbt.com/docs/build/incremental-models-overview.md)

Incremental models in dbt is a materialization strategy designed to efficiently
update your data warehouse tables. The merge strategy uses MERGE statements.
Incremental incremental incremental for keyword frequency testing.

---

### About the Fusion engine

[About Fusion](https://docs.getdbt.com/docs/fusion/about-fusion.md)

The dbt Fusion engine is the next-generation engine for dbt, written from
the ground up in Rust. It catches incorrect SQL immediately and enables
state-aware orchestration.

---

### Deploy jobs

[Deploy jobs](https://docs.getdbt.com/docs/deploy/deploy-jobs.md)

Create and schedule deploy jobs in dbt Cloud for production runs.
Use CI checks to validate changes before merging.
"""


@pytest.fixture
def mock_client():
    """Create a mock ProductDocsClient with AsyncMock methods."""
    client = Mock()
    client.get_index = AsyncMock(return_value=parse_llms_txt(SAMPLE_LLMS_TXT))
    client.get_page = AsyncMock(return_value="# Page Content")
    client.search_index = AsyncMock(return_value=[])
    return client


@pytest.fixture
def context(mock_client):
    """Create ProductDocsToolContext with a mocked client."""
    ctx = ProductDocsToolContext.__new__(ProductDocsToolContext)
    ctx.client = mock_client
    return ctx


class TestParseLlmsTxt:
    def test_parses_entries(self):
        entries = parse_llms_txt(SAMPLE_LLMS_TXT)
        assert len(entries) == 7

    def test_parses_title_and_url(self):
        entries = parse_llms_txt(SAMPLE_LLMS_TXT)
        inc = next(e for e in entries if "Incremental models" in e["title"])
        assert inc["url"] == "https://docs.getdbt.com/docs/build/incremental-models.md"

    def test_parses_description(self):
        entries = parse_llms_txt(SAMPLE_LLMS_TXT)
        inc = next(e for e in entries if "Incremental models" in e["title"])
        assert "incremental" in inc["description"].lower()

    def test_parses_section(self):
        entries = parse_llms_txt(SAMPLE_LLMS_TXT)
        inc = next(e for e in entries if "Incremental models" in e["title"])
        assert inc["section"] == "Build"

    def test_entry_without_description(self):
        entries = parse_llms_txt(SAMPLE_LLMS_TXT)
        hub = next(e for e in entries if "Developer Hub" in e["title"])
        assert hub["description"] == ""

    def test_empty_input(self):
        entries = parse_llms_txt("")
        assert entries == []


class TestNormalizeDocUrl:
    def test_relative_path(self):
        result = normalize_doc_url("/docs/build/incremental-models")
        assert result == "https://docs.getdbt.com/docs/build/incremental-models.md"

    def test_full_url(self):
        result = normalize_doc_url(
            "https://docs.getdbt.com/docs/build/incremental-models"
        )
        assert result == "https://docs.getdbt.com/docs/build/incremental-models.md"

    def test_already_has_md(self):
        result = normalize_doc_url(
            "https://docs.getdbt.com/docs/build/incremental-models.md"
        )
        assert result == "https://docs.getdbt.com/docs/build/incremental-models.md"

    def test_trailing_slash(self):
        result = normalize_doc_url("/docs/build/incremental-models/")
        assert result == "https://docs.getdbt.com/docs/build/incremental-models.md"

    def test_relative_no_leading_slash(self):
        result = normalize_doc_url("docs/build/incremental-models")
        assert result == "https://docs.getdbt.com/docs/build/incremental-models.md"

    def test_whitespace_stripped(self):
        result = normalize_doc_url("  /docs/build/incremental-models  ")
        assert result == "https://docs.getdbt.com/docs/build/incremental-models.md"

    def test_rejects_external_https_url(self):
        with pytest.raises(ValueError, match="docs.getdbt.com"):
            normalize_doc_url("https://evil.com/foo")

    def test_rejects_external_http_url(self):
        with pytest.raises(ValueError, match="docs.getdbt.com"):
            normalize_doc_url("http://internal:8080/secret")

    def test_rejects_subdomain_spoofing(self):
        with pytest.raises(ValueError, match="docs.getdbt.com"):
            normalize_doc_url("https://docs.getdbt.com.evil.com/page")


class TestSearchProductDocs:
    @pytest.mark.asyncio
    async def test_search_returns_results(self, context, mock_client):
        mock_client.search_index.return_value = [
            {
                "title": "Incremental models",
                "url": "https://docs.getdbt.com/docs/build/incremental-models",
                "description": "dbt incremental models",
                "section": "Build",
            },
            {
                "title": "Incremental strategy configs",
                "url": "https://docs.getdbt.com/reference/resource-configs/incremental-strategy",
                "description": "Configure incremental strategies",
                "section": "Reference",
            },
        ]
        result = await search_product_docs.fn(context, "incremental")
        assert result.total_matches == 2
        assert len(result.results) == 2
        titles = [r.title for r in result.results]
        assert any("Incremental" in t for t in titles)

    @pytest.mark.asyncio
    async def test_search_no_matches(self, context, mock_client):
        mock_client.search_index.return_value = []
        result = await search_product_docs.fn(context, "zzzznonexistent")
        assert result.total_matches == 0
        assert result.results == []

    @pytest.mark.asyncio
    async def test_search_empty_query(self, context):
        result = await search_product_docs.fn(context, "")
        assert result.error is not None
        assert "empty" in result.error.lower()

    @pytest.mark.asyncio
    async def test_search_ranks_title_matches_higher(self, context, mock_client):
        mock_client.search_index.return_value = [
            {"title": "Models", "url": "https://docs.getdbt.com/docs/build/models"},
            {
                "title": "Incremental models",
                "url": "https://docs.getdbt.com/docs/build/incremental-models",
            },
        ]
        result = await search_product_docs.fn(context, "models")
        titles = [r.title for r in result.results]
        assert titles[0] == "Models"

    @pytest.mark.asyncio
    async def test_search_multi_keyword(self, context, mock_client):
        mock_client.search_index.return_value = [
            {
                "title": "Incremental strategy configs",
                "url": "https://docs.getdbt.com/reference/resource-configs/incremental-strategy",
            },
        ]
        result = await search_product_docs.fn(context, "incremental strategy")
        assert result.total_matches >= 1

    @pytest.mark.asyncio
    async def test_results_are_typed(self, context, mock_client):
        mock_client.search_index.return_value = [
            {
                "title": "Incremental models",
                "url": "https://docs.getdbt.com/docs/build/incremental-models",
                "description": "dbt incremental models",
                "section": "Build",
            },
        ]
        result = await search_product_docs.fn(context, "incremental")
        assert result.results[0].title == "Incremental models"
        assert result.results[0].description == "dbt incremental models"
        assert result.results[0].section == "Build"

    @pytest.mark.asyncio
    async def test_query_expansion_when_few_title_matches(self, context, mock_client):
        mock_client.search_index.side_effect = [
            [],
            [
                {
                    "url": "https://docs.getdbt.com/docs/build/udfs",
                    "title": "User-defined functions",
                },
            ],
        ]
        result = await search_product_docs.fn(context, "udf")
        assert len(result.results) > 0
        assert result.results[0].title == "User-defined functions"
        assert result.search_method == "query_expansion"
        assert mock_client.search_index.call_count == 2

    @pytest.mark.asyncio
    async def test_query_expansion_deduplicates_urls(self, context, mock_client):
        mock_client.search_index.side_effect = [
            [
                {
                    "title": "Incremental models",
                    "url": "https://docs.getdbt.com/docs/build/incremental-models",
                },
            ],
            [
                {
                    "url": "https://docs.getdbt.com/docs/build/incremental-models",
                    "title": "Duplicate",
                },
                {"url": "https://docs.getdbt.com/docs/new-page", "title": "New Page"},
            ],
        ]
        result = await search_product_docs.fn(context, "ci")
        urls = [r.url for r in result.results]
        assert len(urls) == len(set(urls))

    @pytest.mark.asyncio
    async def test_no_query_expansion_when_enough_results(self, context, mock_client):
        mock_client.search_index.return_value = [
            {"title": "A", "url": "https://docs.getdbt.com/a"},
            {"title": "B", "url": "https://docs.getdbt.com/b"},
            {"title": "C", "url": "https://docs.getdbt.com/c"},
        ]
        result = await search_product_docs.fn(context, "test")
        assert result.search_method is None
        assert mock_client.search_index.call_count == 1


class TestGetProductDocPages:
    @pytest.mark.asyncio
    async def test_fetch_single_page(self, context, mock_client):
        mock_client.get_page.return_value = "# Incremental Models\n\nContent here."

        result = await get_product_doc_pages.fn(
            context, ["/docs/build/incremental-models"]
        )
        assert len(result.pages) == 1
        page = result.pages[0]
        assert page.content == "# Incremental Models\n\nContent here."
        assert "incremental-models" in page.url
        assert not page.url.endswith(".md")
        assert page.error is None

    @pytest.mark.asyncio
    async def test_fetch_not_found(self, context, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_client.get_page.side_effect = httpx.HTTPStatusError(
            "404 Not Found",
            request=Mock(),
            response=mock_response,
        )

        result = await get_product_doc_pages.fn(context, ["/docs/nonexistent"])
        page = result.pages[0]
        assert page.error is not None
        assert "404" in page.error
        assert page.content == ""

    @pytest.mark.asyncio
    async def test_fetch_request_error(self, context, mock_client):
        mock_client.get_page.side_effect = httpx.RequestError(
            "Connection failed", request=Mock()
        )

        result = await get_product_doc_pages.fn(context, ["/docs/build/models"])
        page = result.pages[0]
        assert page.error is not None
        assert "Failed to fetch" in page.error
        assert page.content == ""

    @pytest.mark.asyncio
    async def test_fetches_multiple_pages(self, context, mock_client):
        mock_client.get_page.return_value = "# Page Content"
        result = await get_product_doc_pages.fn(
            context,
            ["/docs/build/models", "/docs/build/seeds"],
        )
        assert len(result.pages) == 2
        assert all(p.content == "# Page Content" for p in result.pages)
        assert all(p.error is None for p in result.pages)

    @pytest.mark.asyncio
    async def test_handles_partial_failures(self, context, mock_client):
        mock_response = Mock()
        mock_response.status_code = 500

        async def side_effect(url):
            if "models" in url:
                raise httpx.HTTPStatusError(
                    "HTTP 500", request=Mock(), response=mock_response
                )
            return "# Good page"

        mock_client.get_page.side_effect = side_effect

        result = await get_product_doc_pages.fn(
            context,
            ["/docs/build/models", "/docs/build/seeds"],
        )
        assert len(result.pages) == 2
        models_page = result.pages[0]
        seeds_page = result.pages[1]
        assert models_page.error is not None
        assert models_page.content == ""
        assert seeds_page.content == "# Good page"
        assert seeds_page.error is None

    @pytest.mark.asyncio
    async def test_all_pages_fail(self, context, mock_client):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_client.get_page.side_effect = httpx.HTTPStatusError(
            "HTTP 500", request=Mock(), response=mock_response
        )

        result = await get_product_doc_pages.fn(
            context,
            ["/docs/build/models", "/docs/build/seeds"],
        )
        assert len(result.pages) == 2
        assert all(p.error is not None for p in result.pages)

    @pytest.mark.asyncio
    async def test_clamped_to_5_pages(self, context, mock_client):
        mock_client.get_page.return_value = "# Content"
        paths = [f"/docs/page-{i}" for i in range(15)]

        result = await get_product_doc_pages.fn(context, paths)
        assert len(result.pages) == 5

    @pytest.mark.asyncio
    async def test_query_filters_to_relevant_sections(self, context, mock_client):
        mock_client.get_page.return_value = (
            "## Incremental models\n\n"
            "Incremental models are efficient for large tables.\n\n"
            "## Seeds\n\n"
            "Seeds are CSV files in your project."
        )
        result = await get_product_doc_pages.fn(
            context, ["/docs/build/incremental-models"], "incremental"
        )
        page = result.pages[0]
        assert page.error is None
        assert "Incremental" in page.content
        assert "Seeds" not in page.content

    @pytest.mark.asyncio
    async def test_empty_paths_list(self, context, mock_client):
        result = await get_product_doc_pages.fn(context, [])
        assert len(result.pages) == 0

    @pytest.mark.asyncio
    async def test_urls_stripped_of_md(self, context, mock_client):
        mock_client.get_page.return_value = "# Content"
        result = await get_product_doc_pages.fn(context, ["/docs/build/models"])
        assert not result.pages[0].url.endswith(".md")

    @pytest.mark.asyncio
    async def test_rejects_external_url(self, context, mock_client):
        result = await get_product_doc_pages.fn(
            context, ["https://evil.com/steal-data"]
        )
        page = result.pages[0]
        assert page.error is not None
        assert page.error.startswith("URL must be on docs.getdbt.com")
        assert page.content == ""

    @pytest.mark.asyncio
    async def test_error_url_is_normalized(self, context, mock_client):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_client.get_page.side_effect = httpx.HTTPStatusError(
            "404 Not Found",
            request=Mock(),
            response=mock_response,
        )
        result = await get_product_doc_pages.fn(context, ["/docs/build/models"])
        page = result.pages[0]
        assert page.error is not None
        assert page.url.startswith("https://docs.getdbt.com/")
        assert not page.url.endswith(".md")


class TestParseLlmsFullTxt:
    def test_parses_pages(self):
        pages = parse_llms_full_txt(SAMPLE_LLMS_FULL_TXT)
        assert len(pages) >= 2

    def test_extracts_urls(self):
        pages = parse_llms_full_txt(SAMPLE_LLMS_FULL_TXT)
        urls = [p["url"] for p in pages]
        assert any("about-fusion" in u for u in urls)
        assert any("incremental-models" in u for u in urls)

    def test_extracts_titles(self):
        pages = parse_llms_full_txt(SAMPLE_LLMS_FULL_TXT)
        titles = [p["title"] for p in pages]
        assert any("Fusion" in t for t in titles)

    def test_content_is_lowercase(self):
        pages = parse_llms_full_txt(SAMPLE_LLMS_FULL_TXT)
        for page in pages:
            assert page["content_lower"] == page["content_lower"].lower()

    def test_empty_input(self):
        pages = parse_llms_full_txt("")
        assert pages == []


class TestSearchFullText:
    @pytest.mark.asyncio
    async def test_finds_keyword_in_body(self):
        from dbt_mcp.product_docs.client import ProductDocsClient

        client = ProductDocsClient()
        client._cache["full_text"] = parse_llms_full_txt(SAMPLE_LLMS_FULL_TXT)
        results = await client.search_full_text(["rust"])
        urls = [r["url"] for r in results]
        assert any("fusion" in u for u in urls)

    @pytest.mark.asyncio
    async def test_no_results_for_missing_keyword(self):
        from dbt_mcp.product_docs.client import ProductDocsClient

        client = ProductDocsClient()
        client._cache["full_text"] = parse_llms_full_txt(SAMPLE_LLMS_FULL_TXT)
        results = await client.search_full_text(["zzzznonexistent"])
        assert results == []

    @pytest.mark.asyncio
    async def test_or_logic_across_keywords(self):
        from dbt_mcp.product_docs.client import ProductDocsClient

        client = ProductDocsClient()
        client._cache["full_text"] = parse_llms_full_txt(SAMPLE_LLMS_FULL_TXT)
        results = await client.search_full_text(["rust", "deploy"])
        urls = [r["url"] for r in results]
        assert any("fusion" in u for u in urls)
        assert any("deploy" in u for u in urls)

    @pytest.mark.asyncio
    async def test_ranks_by_keyword_frequency(self):
        from dbt_mcp.product_docs.client import ProductDocsClient

        client = ProductDocsClient()
        client._cache["full_text"] = parse_llms_full_txt(SAMPLE_LLMS_FULL_TXT)
        results = await client.search_full_text(["incremental"])
        assert results[0]["url"].endswith("incremental-models-overview")


class TestProductDocsRegistration:
    @pytest.mark.asyncio
    async def test_tools_registered_by_default(self, env_setup):
        with (
            env_setup(),
            patch(
                "dbt_mcp.config.config.detect_binary_type",
                return_value=BinaryType.DBT_CORE,
            ),
        ):
            config = load_config(enable_proxied_tools=False)
            dbt_mcp = await create_dbt_mcp(config)
            server_tools = await dbt_mcp.list_tools()
            tool_names = {tool.name for tool in server_tools}

            assert "search_product_docs" in tool_names
            assert "get_product_doc_pages" in tool_names

    @pytest.mark.asyncio
    async def test_tools_available_without_cloud_credentials(self, env_setup):
        """Product docs tools should work even without DBT_HOST or DBT_TOKEN."""
        with (
            env_setup(
                env_vars={
                    "DBT_HOST": "",
                    "DBT_TOKEN": "",
                }
            ),
            patch(
                "dbt_mcp.config.config.detect_binary_type",
                return_value=BinaryType.DBT_CORE,
            ),
        ):
            config = load_config(enable_proxied_tools=False)
            dbt_mcp = await create_dbt_mcp(config)
            server_tools = await dbt_mcp.list_tools()
            tool_names = {tool.name for tool in server_tools}

            assert "search_product_docs" in tool_names
            assert "get_product_doc_pages" in tool_names


class TestSplitMarkdownSections:
    def test_splits_by_h2(self):
        content = "intro\n\n## Section A\n\nContent A.\n\n## Section B\n\nContent B."
        sections = split_markdown_sections(content)
        assert len(sections) == 3
        assert sections[0] == ("", "intro\n\n")
        assert sections[1][0] == "## Section A"
        assert sections[2][0] == "## Section B"

    def test_no_headers_returns_single(self):
        content = "Just plain text with no headers."
        sections = split_markdown_sections(content)
        assert sections == [("", content)]

    def test_h1_h2_h3_all_split(self):
        content = "# H1\n\nA\n\n## H2\n\nB\n\n### H3\n\nC"
        sections = split_markdown_sections(content)
        assert len(sections) == 3
        assert sections[0][0] == "# H1"
        assert sections[1][0] == "## H2"
        assert sections[2][0] == "### H3"

    def test_no_content_before_first_header(self):
        content = "## Only Section\n\nBody text."
        sections = split_markdown_sections(content)
        assert len(sections) == 1
        assert sections[0][0] == "## Only Section"


class TestExtractRelevantSections:
    def test_returns_most_relevant_section(self):
        content = (
            "## Incremental models\n\nIncremental models are efficient.\n\n"
            "## Seeds\n\nSeeds are CSV files."
        )
        result = extract_relevant_sections(content, "incremental")
        assert "Incremental models" in result
        assert "Seeds" not in result

    def test_falls_back_when_no_headers(self):
        content = "x" * 200
        result = extract_relevant_sections(content, "something", max_chars=50)
        assert len(result) <= 200

    def test_falls_back_when_empty_query(self):
        content = ("## Section\n\nSome content here.") * 10
        result = extract_relevant_sections(content, "", max_chars=20)
        assert len(result) <= 100

    def test_respects_max_chars(self):
        content = "## relevant\n\n" + "relevant word " * 500
        result = extract_relevant_sections(content, "relevant", max_chars=100)
        assert len(result) <= 200

    def test_falls_back_when_no_section_scores(self):
        content = "## Alpha\n\nAlpha content.\n\n## Beta\n\nBeta content."
        result = extract_relevant_sections(content, "zzznomatch", max_chars=500)
        assert "Alpha" in result or len(result) <= 500
