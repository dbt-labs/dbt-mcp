"""Product Docs MCP tool definitions.

Registers two tools that let AI agents query the public dbt product
documentation at docs.getdbt.com in real time:

- ``search_product_docs`` — keyword search against the llms.txt index
  (with query-expansion retry for abbreviations/synonyms)
- ``get_product_doc_pages`` — fetch one or more pages as clean Markdown
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Annotated

import httpx
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.product_docs.client import (
    MAX_CONTENT_CHARS_PER_PAGE,
    ProductDocsClient,
    display_url,
    expand_keywords,
    extract_relevant_sections,
    normalize_doc_url,
    truncate_content,
)
from dbt_mcp.product_docs.types import (
    DocSearchResult,
    GetProductDocPagesResponse,
    ProductDocPageResponse,
    SearchProductDocsResponse,
)
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)

QUERY_EXPANSION_THRESHOLD = 3


@dataclass
class ProductDocsToolContext:
    client: ProductDocsClient = field(default_factory=ProductDocsClient)


def _dict_to_doc_search_result(entry: dict[str, str]) -> DocSearchResult:
    return DocSearchResult(
        title=entry.get("title", ""),
        url=entry.get("url", ""),
        description=entry.get("description", ""),
        section=entry.get("section", ""),
    )


async def _fetch_page(
    client: ProductDocsClient, url: str, query: str | None = None
) -> ProductDocPageResponse:
    """Fetch a single page, returning a typed response with error handling."""
    try:
        normalized = normalize_doc_url(url)
    except ValueError as e:
        return ProductDocPageResponse(url=url, content="", error=str(e))
    try:
        content = await client.get_page(normalized)
    except httpx.HTTPStatusError as e:
        return ProductDocPageResponse(
            url=display_url(normalized),
            content="",
            error=f"Page not found or unavailable: {normalized} (HTTP {e.response.status_code})",
        )
    except httpx.RequestError as e:
        return ProductDocPageResponse(
            url=display_url(normalized),
            content="",
            error=f"Failed to fetch page: {normalized} ({e})",
        )

    if query:
        content = extract_relevant_sections(content, query, MAX_CONTENT_CHARS_PER_PAGE)
    else:
        content = truncate_content(
            content, MAX_CONTENT_CHARS_PER_PAGE, display_url(normalized)
        )
    return ProductDocPageResponse(url=display_url(normalized), content=content)


@dbt_mcp_tool(
    description=get_prompt("product_docs/search_product_docs"),
    title="Search Product Docs",
    # read_only: the in-memory cache is internal process state,
    # not an externally observable side-effect.
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
    open_world_hint=True,
)
async def search_product_docs(
    context: ProductDocsToolContext, query: str
) -> SearchProductDocsResponse:
    """Search docs.getdbt.com for pages matching a keyword query.

    Args:
        query: Search terms to match against page titles and descriptions.
    """
    if not query.strip():
        return SearchProductDocsResponse(
            query=query,
            total_matches=0,
            showing=0,
            results=[],
            error="Query must not be empty.",
        )

    client = context.client
    results = await client.search_index(query)
    used_query_expansion = False

    if len(results) < QUERY_EXPANSION_THRESHOLD:
        expanded_keywords = expand_keywords(query)
        expanded_query = " ".join(expanded_keywords)
        if expanded_query and expanded_query.lower() != query.strip().lower():
            try:
                expansion_results = await client.search_index(expanded_query)
                seen_urls = {r["url"] for r in results}
                for entry in expansion_results:
                    if entry["url"] not in seen_urls:
                        results.append(entry)
                        seen_urls.add(entry["url"])
                if expansion_results:
                    used_query_expansion = True
            except Exception as e:
                logger.warning("Query expansion fallback failed: %s", e)

    doc_results = [_dict_to_doc_search_result(r) for r in results]

    return SearchProductDocsResponse(
        query=query,
        total_matches=len(doc_results),
        showing=len(doc_results),
        results=doc_results,
        search_method="query_expansion" if used_query_expansion else None,
    )


@dbt_mcp_tool(
    description=get_prompt("product_docs/get_product_doc_pages"),
    title="Get Product Doc Pages",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
    open_world_hint=True,
)
async def get_product_doc_pages(
    context: ProductDocsToolContext,
    paths: Annotated[
        list[str],
        Field(
            description="List of docs.getdbt.com URLs or relative paths to fetch "
            "(e.g. ['/docs/build/incremental-models', '/docs/build/models']). "
            "Max 5 pages per call.",
        ),
    ],
    query: Annotated[
        str | None,
        Field(
            description="Optional search query. When provided, only the most relevant "
            "sections of each page are returned instead of the full content, "
            "significantly reducing response size.",
        ),
    ] = None,
) -> GetProductDocPagesResponse:
    """Fetch the full Markdown content of one or more docs.getdbt.com pages in parallel.

    Args:
        paths: List of docs.getdbt.com URLs or relative paths to fetch.
        query: Optional query to extract only the most relevant sections from each page.
    """
    paths = paths[:5]
    results = await asyncio.gather(
        *[_fetch_page(context.client, path, query) for path in paths],
        return_exceptions=True,
    )
    pages: list[ProductDocPageResponse] = []
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            try:
                err_url = display_url(normalize_doc_url(paths[i]))
            except Exception:
                err_url = paths[i]
            logger.warning("Failed to fetch %s: %s", err_url, result)
            pages.append(
                ProductDocPageResponse(
                    url=err_url,
                    content="",
                    error=f"Failed to fetch page: {err_url} ({result})",
                )
            )
        else:
            pages.append(result)

    return GetProductDocPagesResponse(pages=pages)


PRODUCT_DOCS_TOOLS = [
    search_product_docs,
    get_product_doc_pages,
]


def register_product_docs_tools(
    dbt_mcp: FastMCP,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    """Register Product Docs tools."""
    shared_client = ProductDocsClient()

    def bind_context() -> ProductDocsToolContext:
        return ProductDocsToolContext(client=shared_client)

    register_tools(
        dbt_mcp,
        tool_definitions=[
            tool.adapt_context(bind_context) for tool in PRODUCT_DOCS_TOOLS
        ],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
