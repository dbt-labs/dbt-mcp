import logging
from collections.abc import Sequence

from dbtsl.api.shared.query_params import GroupByParam
from dbtsl.client.sync import SyncSemanticLayerClient
from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import SemanticLayerConfig
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.semantic_layer.client import (
    SemanticLayerClientProtocol,
    SemanticLayerFetcher,
)
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    EntityToolResponse,
    MetricToolResponse,
    OrderByParam,
    QueryMetricsSuccess,
)
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)


def create_sl_tool_definitions(
    config: SemanticLayerConfig, sl_client: SemanticLayerClientProtocol
) -> list[ToolDefinition]:
    semantic_layer_fetcher = SemanticLayerFetcher(
        sl_client=sl_client,
        config=config,
    )

    def list_metrics() -> list[MetricToolResponse] | str:
        try:
            return semantic_layer_fetcher.list_metrics()
        except Exception as e:
            return str(e)

    def get_dimensions(metrics: list[str]) -> list[DimensionToolResponse] | str:
        try:
            return semantic_layer_fetcher.get_dimensions(metrics=metrics)
        except Exception as e:
            return str(e)

    def get_entities(metrics: list[str]) -> list[EntityToolResponse] | str:
        try:
            return semantic_layer_fetcher.get_entities(metrics=metrics)
        except Exception as e:
            return str(e)

    def query_metrics(
        metrics: list[str],
        group_by: list[GroupByParam] | None = None,
        order_by: list[OrderByParam] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> str:
        try:
            result = semantic_layer_fetcher.query_metrics(
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
        except Exception as e:
            return str(e)

    return [
        ToolDefinition(
            description=get_prompt("semantic_layer/list_metrics"),
            fn=list_metrics,
        ),
        ToolDefinition(
            description=get_prompt("semantic_layer/get_dimensions"),
            fn=get_dimensions,
        ),
        ToolDefinition(
            description=get_prompt("semantic_layer/get_entities"),
            fn=get_entities,
        ),
        ToolDefinition(
            description=get_prompt("semantic_layer/query_metrics"),
            fn=query_metrics,
        ),
    ]


def register_sl_tools(
    dbt_mcp: FastMCP,
    config: SemanticLayerConfig,
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    register_tools(
        dbt_mcp,
        create_sl_tool_definitions(
            config,
            SyncSemanticLayerClient(
                environment_id=config.prod_environment_id,
                auth_token=config.service_token,
                host=config.host,
            ),
        ),
        exclude_tools,
    )
