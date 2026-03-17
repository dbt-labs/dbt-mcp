import logging
from dataclasses import dataclass

from dbtsl.api.shared.query_params import GroupByParam
from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import (
    DefaultSemanticLayerConfigProvider,
    SemanticLayerConfig,
)
from dbt_mcp.config.headers import SemanticLayerHeadersProvider
from dbt_mcp.project.environment_resolver import get_environments_for_project
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
from dbt_mcp.tools.definitions import GenericToolDefinition, generic_dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)


async def _resolve_sl_config_for_project(
    config_provider: DefaultSemanticLayerConfigProvider,
    project_id: int,
) -> SemanticLayerConfig:
    (
        settings,
        token_provider,
    ) = await config_provider.credentials_provider.get_credentials()
    assert settings.actual_host and settings.dbt_account_id
    dbt_platform_url = (
        f"https://{settings.actual_host_prefix}.{settings.actual_host}"
        if settings.actual_host_prefix
        else f"https://{settings.actual_host}"
    )
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token_provider.get_token()}",
    }
    prod_env, _ = get_environments_for_project(
        dbt_platform_url=dbt_platform_url,
        account_id=settings.dbt_account_id,
        project_id=project_id,
        headers=headers,
    )
    if not prod_env:
        raise ValueError(f"No production environment found for project {project_id}")
    host = settings.actual_host
    host_prefix = settings.actual_host_prefix
    is_local = host.startswith("localhost")
    if is_local:
        sl_host = host
    elif host_prefix:
        sl_host = f"{host_prefix}.semantic-layer.{host}"
    else:
        sl_host = f"semantic-layer.{host}"
    return SemanticLayerConfig(
        url=f"http://{sl_host}" if is_local else f"https://{sl_host}/api/graphql",
        host=sl_host,
        prod_environment_id=prod_env.id,
        token_provider=token_provider,
        headers_provider=SemanticLayerHeadersProvider(token_provider=token_provider),
    )


@dataclass
class MultiProjectSemanticLayerToolContext:
    semantic_layer_config_provider: DefaultSemanticLayerConfigProvider
    semantic_layer_fetcher: SemanticLayerFetcher

    def __init__(
        self,
        config_provider: DefaultSemanticLayerConfigProvider,
        client_provider: SemanticLayerClientProvider,
    ):
        self.semantic_layer_config_provider = config_provider
        self.semantic_layer_fetcher = SemanticLayerFetcher(
            config_provider=config_provider, client_provider=client_provider
        )


@generic_dbt_mcp_tool(
    description=get_prompt("semantic_layer/list_metrics"),
    name_enum=ToolName,
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
    config = await _resolve_sl_config_for_project(
        context.semantic_layer_config_provider, project_id
    )
    return await context.semantic_layer_fetcher.list_metrics(
        search=search, config_override=config
    )


@generic_dbt_mcp_tool(
    description=get_prompt("semantic_layer/list_saved_queries"),
    name_enum=ToolName,
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
    config = await _resolve_sl_config_for_project(
        context.semantic_layer_config_provider, project_id
    )
    return await context.semantic_layer_fetcher.list_saved_queries(
        search=search, config_override=config
    )


@generic_dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_dimensions"),
    name_enum=ToolName,
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
    config = await _resolve_sl_config_for_project(
        context.semantic_layer_config_provider, project_id
    )
    return await context.semantic_layer_fetcher.get_dimensions(
        metrics=metrics, search=search, config_override=config
    )


@generic_dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_entities"),
    name_enum=ToolName,
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
    config = await _resolve_sl_config_for_project(
        context.semantic_layer_config_provider, project_id
    )
    return await context.semantic_layer_fetcher.get_entities(
        metrics=metrics, search=search, config_override=config
    )


@generic_dbt_mcp_tool(
    description=get_prompt("semantic_layer/query_metrics"),
    name_enum=ToolName,
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
    config = await _resolve_sl_config_for_project(
        context.semantic_layer_config_provider, project_id
    )
    result = await context.semantic_layer_fetcher.query_metrics(
        metrics=metrics,
        group_by=group_by,
        order_by=order_by,
        where=where,
        limit=limit,
        config_override=config,
    )
    if isinstance(result, QueryMetricsSuccess):
        return result.result
    else:
        return result.error


@generic_dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_metrics_compiled_sql"),
    name_enum=ToolName,
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
    config = await _resolve_sl_config_for_project(
        context.semantic_layer_config_provider, project_id
    )
    result = await context.semantic_layer_fetcher.get_metrics_compiled_sql(
        metrics=metrics,
        group_by=group_by,
        order_by=order_by,
        where=where,
        limit=limit,
        config_override=config,
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
