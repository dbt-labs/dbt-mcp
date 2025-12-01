import logging
from dataclasses import dataclass

from dbtsl.api.shared.query_params import GroupByParam
from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import ConfigProvider, SemanticLayerConfig
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.semantic_layer.client import (
    SemanticLayerClientProvider,
    SemanticLayerFetcher,
)
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    EntityToolResponse,
    GetMetricsCompiledSqlSuccess,
    MetricToolResponse,
    OrderByParam,
    QueryMetricsSuccess,
    SavedQueryToolResponse,
)
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)


@dataclass
class SemanticLayerToolContext:
    semantic_layer_fetcher: SemanticLayerFetcher

    def __init__(
        self,
        config_provider: ConfigProvider[SemanticLayerConfig],
        client_provider: SemanticLayerClientProvider,
    ):
        self.semantic_layer_fetcher = SemanticLayerFetcher(
            config_provider=config_provider, client_provider=client_provider
        )


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/list_metrics"),
    title="List Metrics",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_metrics(
    context: SemanticLayerToolContext, search: str | None = None
) -> list[MetricToolResponse]:
    return await context.semantic_layer_fetcher.list_metrics(search=search)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/list_saved_queries"),
    title="List Saved Queries",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_saved_queries(
    context: SemanticLayerToolContext,
    search: str | None = None,
) -> list[SavedQueryToolResponse]:
    return await context.semantic_layer_fetcher.list_saved_queries(search=search)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_dimensions"),
    title="Get Dimensions",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_dimensions(
    context: SemanticLayerToolContext, metrics: list[str], search: str | None = None
) -> list[DimensionToolResponse]:
    return await context.semantic_layer_fetcher.get_dimensions(
        metrics=metrics, search=search
    )


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_entities"),
    title="Get Entities",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_entities(
    context: SemanticLayerToolContext, metrics: list[str], search: str | None = None
) -> list[EntityToolResponse]:
    return await context.semantic_layer_fetcher.get_entities(
        metrics=metrics, search=search
    )


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/query_metrics"),
    title="Query Metrics",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def query_metrics(
    context: SemanticLayerToolContext,
    metrics: list[str],
    group_by: list[GroupByParam] | None = None,
    order_by: list[OrderByParam] | None = None,
    where: str | None = None,
    limit: int | None = None,
) -> str:
    result = await context.semantic_layer_fetcher.query_metrics(
        metrics=metrics,
        group_by=group_by,
        order_by=order_by,
        where=where,
        limit=limit,
    )
    if isinstance(result, QueryMetricsSuccess):
        return result.result
    else:
        return result.error


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_metrics_compiled_sql"),
    title="Compile SQL",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_metrics_compiled_sql(
    context: SemanticLayerToolContext,
    metrics: list[str],
    group_by: list[GroupByParam] | None = None,
    order_by: list[OrderByParam] | None = None,
    where: str | None = None,
    limit: int | None = None,
) -> str:
    result = await context.semantic_layer_fetcher.get_metrics_compiled_sql(
        metrics=metrics,
        group_by=group_by,
        order_by=order_by,
        where=where,
        limit=limit,
    )
    if isinstance(result, GetMetricsCompiledSqlSuccess):
        return result.sql
    else:
        return result.error


SEMANTIC_LAYER_TOOLS = [
    list_metrics,
    list_saved_queries,
    get_dimensions,
    get_entities,
    query_metrics,
    get_metrics_compiled_sql,
]


def register_sl_tools(
    dbt_mcp: FastMCP,
    config_provider: ConfigProvider[SemanticLayerConfig],
    client_provider: SemanticLayerClientProvider,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName],
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    def bind_context() -> SemanticLayerToolContext:
        return SemanticLayerToolContext(
            config_provider=config_provider, client_provider=client_provider
        )

    register_tools(
        dbt_mcp,
        [tool.adapt_context(bind_context) for tool in SEMANTIC_LAYER_TOOLS],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
