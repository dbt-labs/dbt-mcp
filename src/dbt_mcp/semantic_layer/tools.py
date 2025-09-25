import logging
from collections.abc import Sequence

from dbtsl.api.shared.query_params import GroupByParam
from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import (
    ConfigProvider,
    SemanticLayerConfig,
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
)
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)


def create_sl_tool_definitions(
    config_provider: ConfigProvider[SemanticLayerConfig],
    client_provider: SemanticLayerClientProvider,
) -> list[ToolDefinition]:
    semantic_layer_fetcher = SemanticLayerFetcher(
        config_provider=config_provider,
        client_provider=client_provider,
    )

    async def list_metrics(search: str | None = None) -> list[MetricToolResponse]:
        return await semantic_layer_fetcher.list_metrics(search=search)

    async def get_dimensions(
        metrics: list[str], search: str | None = None
    ) -> list[DimensionToolResponse]:
        return await semantic_layer_fetcher.get_dimensions(
            metrics=metrics, search=search
        )

    async def get_entities(
        metrics: list[str], search: str | None = None
    ) -> list[EntityToolResponse]:
        return await semantic_layer_fetcher.get_entities(metrics=metrics, search=search)

    async def query_metrics(
        metrics: list[str],
        group_by: list[GroupByParam] | None = None,
        order_by: list[OrderByParam] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> str:
        result = await semantic_layer_fetcher.query_metrics(
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

    async def get_metrics_compiled_sql(
        metrics: list[str],
        group_by: list[GroupByParam] | None = None,
        order_by: list[OrderByParam] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> str:
        result = await semantic_layer_fetcher.get_metrics_compiled_sql(
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

    return [
        ToolDefinition(
            description=get_prompt("semantic_layer/list_metrics"),
            fn=list_metrics,
            annotations=create_tool_annotations(
                title="List Metrics",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("semantic_layer/get_dimensions"),
            fn=get_dimensions,
            annotations=create_tool_annotations(
                title="Get Dimensions",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("semantic_layer/get_entities"),
            fn=get_entities,
            annotations=create_tool_annotations(
                title="Get Entities",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("semantic_layer/query_metrics"),
            fn=query_metrics,
            annotations=create_tool_annotations(
                title="Query Metrics",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("semantic_layer/get_metrics_compiled_sql"),
            fn=get_metrics_compiled_sql,
            annotations=create_tool_annotations(
                title="Compile SQL",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
    ]


def register_sl_tools(
    dbt_mcp: FastMCP,
    config_provider: ConfigProvider[SemanticLayerConfig],
    client_provider: SemanticLayerClientProvider,
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    register_tools(
        dbt_mcp,
        create_sl_tool_definitions(config_provider, client_provider),
        exclude_tools,
    )
