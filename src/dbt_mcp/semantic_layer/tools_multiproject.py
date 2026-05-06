from dataclasses import dataclass
from typing import Annotated

from dbtsl.api.shared.query_params import GroupByParam
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import (
    MultiProjectConfigProvider,
    SemanticLayerConfig,
)
from dbt_mcp.config.config_providers.semantic_layer import (
    MultiProjectSemanticLayerConfigProvider,
)
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.semantic_layer.client import (
    SemanticLayerClientProvider,
    SemanticLayerFetcher,
)
from dbt_mcp.semantic_layer.param_descriptions import (
    QUERY_RESULT_LIMIT,
    SEMANTIC_GROUP_BY,
    SEMANTIC_LAYER_PROJECT_ID,
    SEMANTIC_METRICS,
    SEMANTIC_ORDER_BY,
    SEMANTIC_SEARCH_DIMENSIONS,
    SEMANTIC_SEARCH_ENTITIES,
    SEMANTIC_SEARCH_METRICS,
    SEMANTIC_SEARCH_SAVED_QUERIES,
    SEMANTIC_WHERE,
)
from dbt_mcp.semantic_layer.tools import metrics_to_csv
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    EntityToolResponse,
    GetMetricsCompiledSqlSuccess,
    OrderByParam,
    QueryMetricsSuccess,
    SavedQueryToolResponse,
)
from dbt_mcp.tools.definitions import GenericToolDefinition, dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset


@dataclass
class MultiProjectSemanticLayerToolContext:
    semantic_layer_config_provider: MultiProjectConfigProvider[SemanticLayerConfig]
    client_provider: SemanticLayerClientProvider

    def __init__(
        self,
        config_provider: MultiProjectConfigProvider[SemanticLayerConfig],
        client_provider: SemanticLayerClientProvider,
    ):
        self.semantic_layer_config_provider = config_provider
        self.client_provider = client_provider


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/list_metrics"),
    title="List Metrics",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_metrics(
    context: MultiProjectSemanticLayerToolContext,
    project_id: Annotated[int, Field(description=SEMANTIC_LAYER_PROJECT_ID)],
    search: Annotated[str | None, Field(description=SEMANTIC_SEARCH_METRICS)] = None,
) -> str:
    config = await context.semantic_layer_config_provider.get_config(project_id)
    response = await SemanticLayerFetcher(
        client_provider=context.client_provider,
    ).list_metrics(config=config, search=search)
    return metrics_to_csv(response, max_response_chars=config.max_response_chars)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/list_saved_queries"),
    title="List Saved Queries",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_saved_queries(
    context: MultiProjectSemanticLayerToolContext,
    project_id: Annotated[int, Field(description=SEMANTIC_LAYER_PROJECT_ID)],
    search: Annotated[
        str | None, Field(description=SEMANTIC_SEARCH_SAVED_QUERIES)
    ] = None,
) -> list[SavedQueryToolResponse]:
    config = await context.semantic_layer_config_provider.get_config(project_id)
    return await SemanticLayerFetcher(
        client_provider=context.client_provider
    ).list_saved_queries(config=config, search=search)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_dimensions"),
    title="Get Dimensions",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_dimensions(
    context: MultiProjectSemanticLayerToolContext,
    project_id: Annotated[int, Field(description=SEMANTIC_LAYER_PROJECT_ID)],
    metrics: Annotated[list[str], Field(description=SEMANTIC_METRICS)],
    search: Annotated[str | None, Field(description=SEMANTIC_SEARCH_DIMENSIONS)] = None,
) -> list[DimensionToolResponse]:
    config = await context.semantic_layer_config_provider.get_config(project_id)
    return await SemanticLayerFetcher(
        client_provider=context.client_provider
    ).get_dimensions(config=config, metrics=metrics, search=search)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_entities"),
    title="Get Entities",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_entities(
    context: MultiProjectSemanticLayerToolContext,
    project_id: Annotated[int, Field(description=SEMANTIC_LAYER_PROJECT_ID)],
    metrics: Annotated[list[str], Field(description=SEMANTIC_METRICS)],
    search: Annotated[str | None, Field(description=SEMANTIC_SEARCH_ENTITIES)] = None,
) -> list[EntityToolResponse]:
    config = await context.semantic_layer_config_provider.get_config(project_id)
    return await SemanticLayerFetcher(
        client_provider=context.client_provider
    ).get_entities(config=config, metrics=metrics, search=search)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/query_metrics"),
    title="Query Metrics",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def query_metrics(
    context: MultiProjectSemanticLayerToolContext,
    project_id: Annotated[int, Field(description=SEMANTIC_LAYER_PROJECT_ID)],
    metrics: Annotated[list[str], Field(description=SEMANTIC_METRICS)],
    group_by: Annotated[
        list[GroupByParam] | None, Field(description=SEMANTIC_GROUP_BY)
    ] = None,
    order_by: Annotated[
        list[OrderByParam] | None, Field(description=SEMANTIC_ORDER_BY)
    ] = None,
    where: Annotated[str | None, Field(description=SEMANTIC_WHERE)] = None,
    limit: Annotated[int | None, Field(description=QUERY_RESULT_LIMIT)] = None,
) -> str:
    config = await context.semantic_layer_config_provider.get_config(project_id)
    result = await SemanticLayerFetcher(
        client_provider=context.client_provider
    ).query_metrics(
        config=config,
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
    context: MultiProjectSemanticLayerToolContext,
    project_id: Annotated[int, Field(description=SEMANTIC_LAYER_PROJECT_ID)],
    metrics: Annotated[list[str], Field(description=SEMANTIC_METRICS)],
    group_by: Annotated[
        list[GroupByParam] | None, Field(description=SEMANTIC_GROUP_BY)
    ] = None,
    order_by: Annotated[
        list[OrderByParam] | None, Field(description=SEMANTIC_ORDER_BY)
    ] = None,
    where: Annotated[str | None, Field(description=SEMANTIC_WHERE)] = None,
    limit: Annotated[int | None, Field(description=QUERY_RESULT_LIMIT)] = None,
) -> str:
    config = await context.semantic_layer_config_provider.get_config(project_id)
    result = await SemanticLayerFetcher(
        client_provider=context.client_provider
    ).get_metrics_compiled_sql(
        config=config,
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
    config_provider: MultiProjectSemanticLayerConfigProvider,
    client_provider: SemanticLayerClientProvider,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    def bind_context() -> MultiProjectSemanticLayerToolContext:
        return MultiProjectSemanticLayerToolContext(
            config_provider=config_provider,
            client_provider=client_provider,
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
