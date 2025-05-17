import logging
from time import time

from dbtsl.api.shared.query_params import GroupByParam
from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import SemanticLayerConfig
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.semantic_layer.client import get_semantic_layer_fetcher
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    EntityToolResponse,
    MetricToolResponse,
    OrderByParam,
    QueryMetricsSuccess,
)

logger = logging.getLogger(__name__)


def register_sl_tools(dbt_mcp: FastMCP, config: SemanticLayerConfig) -> None:
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)

    @dbt_mcp.tool(description=get_prompt("semantic_layer/list_metrics"))
    def list_metrics() -> list[MetricToolResponse] | str:
        try:
            start_time = time()
            result = semantic_layer_fetcher.list_metrics()
            end_time = time()
            logger.info(f"list_metrics took {end_time - start_time} seconds")
            return result
        except Exception as e:
            logger.error(f"Error listing metrics: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("semantic_layer/get_dimensions"))
    def get_dimensions(metrics: list[str]) -> list[DimensionToolResponse] | str:
        try:
            start_time = time()
            result = semantic_layer_fetcher.get_dimensions(metrics=metrics)
            end_time = time()
            logger.info(f"get_dimensions took {end_time - start_time} seconds")
            return result
        except Exception as e:
            logger.error(f"Error getting dimensions: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("semantic_layer/get_entities"))
    def get_entities(metrics: list[str]) -> list[EntityToolResponse] | str:
        try:
            start_time = time()
            result = semantic_layer_fetcher.get_entities(metrics=metrics)
            end_time = time()
            logger.info(f"get_entities took {end_time - start_time} seconds")
            return result
        except Exception as e:
            logger.error(f"Error getting entities: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("semantic_layer/query_metrics"))
    def query_metrics(
        metrics: list[str],
        group_by: list[GroupByParam] | None = None,
        order_by: list[OrderByParam] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> str:
        try:
            start_time = time()
            result = semantic_layer_fetcher.query_metrics(
                metrics=metrics,
                group_by=group_by,
                order_by=order_by,
                where=where,
                limit=limit,
            )
            end_time = time()
            logger.info(f"query_metrics took {end_time - start_time} seconds")
            if isinstance(result, QueryMetricsSuccess):
                return result.result
            else:
                return result.error
        except Exception as e:
            logger.error(f"Error querying metrics: {e}")
            return str(e)
