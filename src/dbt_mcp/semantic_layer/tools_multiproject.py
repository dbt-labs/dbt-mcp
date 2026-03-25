import logging
from dataclasses import dataclass

from dbtsl.api.shared.query_params import GroupByParam
from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import (
    DefaultSemanticLayerConfigProvider,
)
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
from dbt_mcp.tools.definitions import GenericToolDefinition, dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)


@dataclass
class MultiProjectSemanticLayerToolContext:
    semantic_layer_config_provider: DefaultSemanticLayerConfigProvider
    _client_provider: SemanticLayerClientProvider

    def __init__(
        self,
        config_provider: DefaultSemanticLayerConfigProvider,
        client_provider: SemanticLayerClientProvider,
    ):
        self.semantic_layer_config_provider = config_provider
        self._client_provider = client_provider

    async def get_fetcher(self, project_id: int) -> SemanticLayerFetcher:
        config = await self.semantic_layer_config_provider.get_config_for_project(
            project_id
        )
        return SemanticLayerFetcher(
            config_provider=self.semantic_layer_config_provider,
            client_provider=self._client_provider,
            config=config,
        )


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/list_metrics"),
    name="list_metrics",
    title="List Metrics",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_metrics(
    context: MultiProjectSemanticLayerToolContext,
    project_id: int,
    search: str | None = None,
) -> list[MetricToolResponse]:
    fetcher = await context.get_fetcher(project_id)
    return await fetcher.list_metrics(search=search)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/list_saved_queries"),
    name="list_saved_queries",
    title="List Saved Queries",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_saved_queries(
    context: MultiProjectSemanticLayerToolContext,
    project_id: int,
    search: str | None = None,
) -> list[SavedQueryToolResponse]:
    fetcher = await context.get_fetcher(project_id)
    return await fetcher.list_saved_queries(search=search)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_dimensions"),
    name="get_dimensions",
    title="Get Dimensions",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_dimensions(
    context: MultiProjectSemanticLayerToolContext,
    project_id: int,
    metrics: list[str],
    search: str | None = None,
) -> list[DimensionToolResponse]:
    fetcher = await context.get_fetcher(project_id)
    return await fetcher.get_dimensions(metrics=metrics, search=search)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_entities"),
    name="get_entities",
    title="Get Entities",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_entities(
    context: MultiProjectSemanticLayerToolContext,
    project_id: int,
    metrics: list[str],
    search: str | None = None,
) -> list[EntityToolResponse]:
    fetcher = await context.get_fetcher(project_id)
    return await fetcher.get_entities(metrics=metrics, search=search)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/query_metrics"),
    name="query_metrics",
    title="Query Metrics",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def query_metrics(
    context: MultiProjectSemanticLayerToolContext,
    project_id: int,
    metrics: list[str],
    group_by: list[GroupByParam] | None = None,
    order_by: list[OrderByParam] | None = None,
    where: str | None = None,
    limit: int | None = None,
) -> str:
    fetcher = await context.get_fetcher(project_id)
    result = await fetcher.query_metrics(
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
    name="get_metrics_compiled_sql",
    title="Compile SQL",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_metrics_compiled_sql(
    context: MultiProjectSemanticLayerToolContext,
    project_id: int,
    metrics: list[str],
    group_by: list[GroupByParam] | None = None,
    order_by: list[OrderByParam] | None = None,
    where: str | None = None,
    limit: int | None = None,
) -> str:
    fetcher = await context.get_fetcher(project_id)
    result = await fetcher.get_metrics_compiled_sql(
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


MULTIPROJECT_SEMANTIC_LAYER_TOOLS: list[GenericToolDefinition[ToolName]] = [
    list_metrics,
    list_saved_queries,
    get_dimensions,
    get_entities,
    query_metrics,
    get_metrics_compiled_sql,
]


def register_multiproject_sl_tools(
    dbt_mcp: FastMCP,
    config_provider: DefaultSemanticLayerConfigProvider,
    client_provider: SemanticLayerClientProvider,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    def bind_context() -> MultiProjectSemanticLayerToolContext:
        return MultiProjectSemanticLayerToolContext(
            config_provider=config_provider, client_provider=client_provider
        )

    register_tools(
        dbt_mcp,
        [
            tool.adapt_context(bind_context)
            for tool in MULTIPROJECT_SEMANTIC_LAYER_TOOLS
        ],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
