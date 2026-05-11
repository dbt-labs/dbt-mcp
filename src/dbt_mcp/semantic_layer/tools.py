import csv
import io
import json
import logging
from dataclasses import dataclass
from typing import Annotated

from dbtsl.api.shared.query_params import GroupByParam
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import ConfigProvider, SemanticLayerConfig
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.semantic_layer.client import (
    SemanticLayerClientProvider,
    SemanticLayerFetcher,
)
from dbt_mcp.semantic_layer.param_descriptions import (
    QUERY_RESULT_LIMIT,
    SEMANTIC_GROUP_BY,
    SEMANTIC_METRICS,
    SEMANTIC_ORDER_BY,
    SEMANTIC_SEARCH_DIMENSIONS,
    SEMANTIC_SEARCH_ENTITIES,
    SEMANTIC_SEARCH_METRICS,
    SEMANTIC_SEARCH_SAVED_QUERIES,
    SEMANTIC_WHERE,
)
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    EntityToolResponse,
    GetMetricsCompiledSqlSuccess,
    ListMetricsResponse,
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


def _build_csv(metrics: list[MetricToolResponse], columns: list[str]) -> str:
    def _cell(m: MetricToolResponse, col: str) -> str:
        val = getattr(m, col)
        if val is None:
            return ""
        if isinstance(val, list):
            return ",".join(str(v) for v in val)
        if isinstance(val, dict):
            return json.dumps(val, separators=(",", ":"), sort_keys=True)
        return str(val)

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(columns)
    for m in metrics:
        writer.writerow([_cell(m, col) for col in columns])
    return output.getvalue().rstrip("\n")


def metrics_to_csv(response: ListMetricsResponse, max_response_chars: int = 0) -> str:
    """Serialize metrics to CSV, optionally trimming verbose fields.

    When trimming fires, a `# Note:` comment line is prepended to the CSV so
    the LLM (the primary consumer) sees the explanation up front. Programmatic
    consumers should strip leading `#`-prefixed lines before parsing — same
    convention as pandas `comment='#'`.
    """
    metrics = response.metrics
    if not metrics:
        return ""

    def _has_any(field: str) -> bool:
        # Skip columns where every value is None/empty — empty lists/dicts/strings
        # count as "no data" so the column is omitted entirely.
        return any(getattr(m, field) for m in metrics)

    columns: list[str] = ["name", "type"]
    for col in ("label", "description", "metadata", "dimensions", "entities"):
        if _has_any(col):
            columns.append(col)

    result = _build_csv(metrics, columns)
    if max_response_chars > 0 and len(result) > max_response_chars:
        # Strip optional fields and rebuild, then prepend a notice so the LLM
        # knows fields were dropped and can re-query with `search` for details.
        trimmed_columns = [c for c in columns if c not in ("description", "metadata")]
        dropped = [c for c in ("description", "metadata") if c in columns]
        result = _build_csv(metrics, trimmed_columns)
        if dropped:
            notice = (
                f"# Note: {', '.join(repr(c) for c in dropped)} omitted because "
                f"the response exceeded {max_response_chars} chars. "
                "Call list_metrics again with the `search` parameter "
                "(a name substring or list of substrings) to retrieve "
                "these fields for a specific subset of metrics.\n"
            )
            result = notice + result
    return result


@dataclass
class SemanticLayerToolContext:
    config_provider: ConfigProvider[SemanticLayerConfig]
    semantic_layer_fetcher: SemanticLayerFetcher

    def __init__(
        self,
        config_provider: ConfigProvider[SemanticLayerConfig],
        client_provider: SemanticLayerClientProvider,
    ):
        self.config_provider = config_provider
        self.semantic_layer_fetcher = SemanticLayerFetcher(
            client_provider=client_provider,
        )


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/list_metrics"),
    title="List Metrics",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_metrics(
    context: SemanticLayerToolContext,
    search: Annotated[
        str | list[str] | None, Field(description=SEMANTIC_SEARCH_METRICS)
    ] = None,
) -> str:
    config = await context.config_provider.get_config()
    response = await context.semantic_layer_fetcher.list_metrics(
        config=config, search=search
    )
    # Only trim broad listings. Below the related-metrics threshold the
    # response already includes per-metric dimensions/entities — meaning the
    # caller asked about a small, specific set, so return full data even if
    # verbose. Trimming there would drop the very fields they're after.
    is_broad_listing = len(response.metrics) > config.metrics_related_max
    max_chars = config.max_response_chars if is_broad_listing else 0
    return metrics_to_csv(response, max_response_chars=max_chars)


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/list_saved_queries"),
    title="List Saved Queries",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_saved_queries(
    context: SemanticLayerToolContext,
    search: Annotated[
        str | None, Field(description=SEMANTIC_SEARCH_SAVED_QUERIES)
    ] = None,
) -> list[SavedQueryToolResponse]:
    config = await context.config_provider.get_config()
    return await context.semantic_layer_fetcher.list_saved_queries(
        config=config, search=search
    )


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_dimensions"),
    title="Get Dimensions",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_dimensions(
    context: SemanticLayerToolContext,
    metrics: Annotated[list[str], Field(description=SEMANTIC_METRICS)],
    search: Annotated[str | None, Field(description=SEMANTIC_SEARCH_DIMENSIONS)] = None,
) -> list[DimensionToolResponse]:
    config = await context.config_provider.get_config()
    return await context.semantic_layer_fetcher.get_dimensions(
        config=config, metrics=metrics, search=search
    )


@dbt_mcp_tool(
    description=get_prompt("semantic_layer/get_entities"),
    title="Get Entities",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_entities(
    context: SemanticLayerToolContext,
    metrics: Annotated[list[str], Field(description=SEMANTIC_METRICS)],
    search: Annotated[str | None, Field(description=SEMANTIC_SEARCH_ENTITIES)] = None,
) -> list[EntityToolResponse]:
    config = await context.config_provider.get_config()
    return await context.semantic_layer_fetcher.get_entities(
        config=config, metrics=metrics, search=search
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
    config = await context.config_provider.get_config()
    result = await context.semantic_layer_fetcher.query_metrics(
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
    context: SemanticLayerToolContext,
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
    config = await context.config_provider.get_config()
    result = await context.semantic_layer_fetcher.get_metrics_compiled_sql(
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
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    def bind_context() -> SemanticLayerToolContext:
        return SemanticLayerToolContext(
            config_provider=config_provider,
            client_provider=client_provider,
        )

    register_tools(
        dbt_mcp,
        [tool.adapt_context(bind_context) for tool in SEMANTIC_LAYER_TOOLS],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
