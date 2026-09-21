import asyncio
import csv
import io
import json
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, replace
from typing import Annotated, Any

from dbtsl.api.shared.query_params import GroupByParam
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import ConfigProvider, SemanticLayerConfig
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.semantic_layer.client import (
    SemanticLayerClientProvider,
    SemanticLayerFetcher,
)
from dbt_mcp.semantic_layer.jev import (
    JevCandidate,
    JevConfig,
    JevRanker,
    JevUnavailableError,
)
from dbt_mcp.semantic_layer.param_descriptions import (
    QUERY_RESULT_LIMIT,
    SEMANTIC_QUESTION,
    SEMANTIC_DIMENSION,
    SEMANTIC_DIMENSION_VALUES_LIMIT,
    SEMANTIC_GROUP_BY,
    SEMANTIC_METRICS,
    SEMANTIC_ORDER_BY,
    SEMANTIC_SEARCH_DIMENSIONS,
    SEMANTIC_META_FILTER,
    SEMANTIC_SEARCH_ENTITIES,
    SEMANTIC_SEARCH_METRICS,
    SEMANTIC_SEARCH_SAVED_QUERIES,
    SEMANTIC_WHERE,
)
from dbt_mcp.semantic_layer.types import (
    DimensionValuesError,
    DimensionToolResponse,
    DimensionValuesResponse,
    EntityToolResponse,
    GetMetricsCompiledSqlSuccess,
    ListMetricsResponse,
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
    for col in (
        "label",
        "description",
        "metadata",
        "dimensions",
        "entities",
        "relevance",
    ):
        if _has_any(col):
            columns.append(col)

    result = _build_csv(metrics, columns)
    if max_response_chars > 0 and len(result) > max_response_chars:
        # Progressive trimming: drop description first, then metadata only if needed.
        # Metadata contains semantic flags (e.g. access controls) that agents rely on
        # for routing — preserve it as long as dropping description alone suffices.
        dropped: list[str] = []
        current_columns = columns

        for col in ("description", "metadata"):
            if len(result) <= max_response_chars:
                break
            if col in current_columns:
                current_columns = [c for c in current_columns if c != col]
                result = _build_csv(metrics, current_columns)
                dropped.append(col)

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


def filter_metrics_by_meta(
    response: ListMetricsResponse, meta_filter: dict[str, Any]
) -> ListMetricsResponse:
    """Return a new response containing only metrics whose metadata matches all filter pairs.

    String values "true"/"false" in meta_filter are normalized to Python booleans so
    that LLM agents producing JSON strings get the same result as those producing JSON
    booleans — the most common type mismatch against YAML-sourced metadata.
    """

    def _normalize(v: Any) -> Any:
        if isinstance(v, str):
            lower = v.lower()
            if lower == "true":
                return True
            if lower == "false":
                return False
        return v

    normalized = {k: _normalize(v) for k, v in meta_filter.items()}
    return ListMetricsResponse(
        metrics=[
            m
            for m in response.metrics
            if m.metadata
            and all(m.metadata.get(k) == normalized[k] for k in normalized)
        ]
    )


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
    meta_filter: Annotated[
        dict[str, Any] | None, Field(description=SEMANTIC_META_FILTER)
    ] = None,
) -> str:
    config = await context.config_provider.get_config()
    response = await context.semantic_layer_fetcher.list_metrics(
        config=config, search=search
    )
    if meta_filter:
        response = filter_metrics_by_meta(response, meta_filter)
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
    description=get_prompt("semantic_layer/get_dimension_values"),
    title="Get Dimension Values",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_dimension_values(
    context: SemanticLayerToolContext,
    dimension: Annotated[str, Field(description=SEMANTIC_DIMENSION)],
    metrics: Annotated[list[str] | None, Field(description=SEMANTIC_METRICS)] = None,
    limit: Annotated[
        int, Field(ge=1, description=SEMANTIC_DIMENSION_VALUES_LIMIT)
    ] = 100,
) -> DimensionValuesResponse | DimensionValuesError:
    config = await context.config_provider.get_config()
    return await context.semantic_layer_fetcher.get_dimension_values(
        config=config,
        dimension=dimension,
        metrics=metrics,
        limit=limit,
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


def _unranked_csv(response: ListMetricsResponse, config: SemanticLayerConfig) -> str:
    """Today's behavior: trim broad listings, return narrow ones in full."""
    is_broad_listing = len(response.metrics) > config.metrics_related_max
    max_chars = config.max_response_chars if is_broad_listing else 0
    return metrics_to_csv(response, max_response_chars=max_chars)


def _dimensions_csv(
    metric_name: str,
    dimensions: list[DimensionToolResponse],
    ranked: list[Any],
    total: int,
) -> str:
    """Render the dimensions Jev judged relevant to the question, with descriptions.

    Descriptions are what let the caller tell near-duplicate dimensions apart
    (the same concept reachable by different join paths), so they are always
    included here even though the inline `dimensions` column carries names only.
    """
    by_name = {d.name: d for d in dimensions}
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["name", "type", "description", "granularities", "relevance"])
    rows = 0
    for item in ranked:
        dimension = by_name.get(item.name)
        if dimension is None:
            continue
        writer.writerow(
            [
                dimension.name,
                str(dimension.type),
                dimension.description or "",
                ",".join(dimension.granularities or []),
                f"{item.score:.2f}",
            ]
        )
        rows += 1
    header = (
        f"# Dimensions for `{metric_name}` - top {rows} of {total} by relevance "
        f"to the question. Call get_dimensions for the full list.\n"
    )
    return header + output.getvalue().rstrip("\n")


def build_jev_list_metrics(
    ranker: JevRanker, jev_config: JevConfig
) -> Callable[..., Coroutine[Any, Any, str]]:
    """Build a `list_metrics` that can rank the catalog against a question.

    Returned as a closure rather than a module-level tool so that the `question`
    parameter is absent from the tool schema entirely when Jev is not configured.
    """

    async def list_metrics(
        context: SemanticLayerToolContext,
        question: Annotated[str | None, Field(description=SEMANTIC_QUESTION)] = None,
        search: Annotated[
            str | list[str] | None, Field(description=SEMANTIC_SEARCH_METRICS)
        ] = None,
        meta_filter: Annotated[
            dict[str, Any] | None, Field(description=SEMANTIC_META_FILTER)
        ] = None,
    ) -> str:
        config = await context.config_provider.get_config()
        response = await context.semantic_layer_fetcher.list_metrics(
            config=config, search=search
        )

        # A substring `search` that shares no vocabulary with the catalog returns
        # nothing, which is exactly the case semantic ranking is good at. Widen to
        # the full catalog rather than returning an empty result - but say so.
        widened = False
        if question and search and not response.metrics:
            response = await context.semantic_layer_fetcher.list_metrics(
                config=config, search=None
            )
            widened = True

        if meta_filter:
            response = filter_metrics_by_meta(response, meta_filter)

        if not question or not response.metrics:
            return _unranked_csv(response, config)

        try:
            return await _ranked_csv(
                context=context,
                config=config,
                response=response,
                question=question,
                ranker=ranker,
                jev_config=jev_config,
                widened=widened,
                search=search,
            )
        except JevUnavailableError as e:
            # Relevance ranking is an optimization; never fail the tool call for it.
            logger.warning("Falling back to unranked listing: %s", e)
            return _unranked_csv(response, config)

    return list_metrics


async def _ranked_csv(
    *,
    context: SemanticLayerToolContext,
    config: SemanticLayerConfig,
    response: ListMetricsResponse,
    question: str,
    ranker: JevRanker,
    jev_config: JevConfig,
    widened: bool,
    search: str | list[str] | None,
) -> str:
    total_metrics = len(response.metrics)
    ranked = await ranker.rank_groups(
        question=question,
        groups={
            "metrics": [
                JevCandidate(m.name, m.description, m.label) for m in response.metrics
            ]
        },
        kind="metric",
        top_k=jev_config.top_k_metrics,
    )
    scored = ranked.get("metrics", [])
    if not scored:
        logger.info("No metric cleared the relevance floor; returning full listing")
        return _unranked_csv(response, config)

    by_name = {m.name: m for m in response.metrics}
    ranked_metrics = [
        replace(by_name[item.name], relevance=round(item.score, 2))
        for item in scored
        if item.name in by_name
    ]

    # `get_dimensions` intersects dimensions across metrics, so a multi-metric
    # call returns near-nothing. Fetch each metric separately instead, and cap
    # the fan-out. `search` is left None so the question never enters the
    # fetcher's cache key.
    top_score = scored[0].score
    dimension_metrics = [
        item.name
        for item in scored[: jev_config.dimension_metrics]
        if item.score >= top_score * jev_config.dimension_metric_score_ratio
    ]
    fetched = await asyncio.gather(
        *(
            context.semantic_layer_fetcher.get_dimensions(
                config=config, metrics=[name], search=None
            )
            for name in dimension_metrics
        )
    )
    dimensions_by_metric = dict(zip(dimension_metrics, fetched, strict=True))
    ranked_dimensions = await ranker.rank_groups(
        question=question,
        groups={
            name: [JevCandidate(d.name, d.description, d.label) for d in dims]
            for name, dims in dimensions_by_metric.items()
        },
        kind="dimension",
        top_k=jev_config.top_k_dimensions,
    )

    sections: list[str] = []
    if widened:
        sections.append(
            f"# Note: `search`={search!r} matched no metrics, so these are "
            f"semantic matches ranked across all {total_metrics} metrics in the "
            f"catalog."
        )
    sections.append(
        f"# Ranked by relevance to the question: top {len(ranked_metrics)} of "
        f"{total_metrics} metrics. `relevance` is 0-1; low scores across the "
        f"board mean no metric matches the question well."
    )
    sections.append(metrics_to_csv(ListMetricsResponse(metrics=ranked_metrics)))
    for name in dimension_metrics:
        dims = dimensions_by_metric[name]
        sections.append(
            _dimensions_csv(name, dims, ranked_dimensions.get(name, []), len(dims))
        )
    return "\n\n".join(sections)


def build_jev_list_metrics_tool(
    ranker: JevRanker, jev_config: JevConfig
) -> GenericToolDefinition[ToolName]:
    return dbt_mcp_tool(
        description=get_prompt("semantic_layer/list_metrics_jev"),
        title="List Metrics",
        name="list_metrics",
        read_only_hint=True,
        destructive_hint=False,
        idempotent_hint=True,
    )(build_jev_list_metrics(ranker, jev_config))


SEMANTIC_LAYER_TOOLS = [
    list_metrics,
    list_saved_queries,
    get_dimensions,
    get_entities,
    get_dimension_values,
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
    jev_ranker: JevRanker | None = None,
    jev_config: JevConfig | None = None,
) -> None:
    def bind_context() -> SemanticLayerToolContext:
        return SemanticLayerToolContext(
            config_provider=config_provider,
            client_provider=client_provider,
        )

    tools = list(SEMANTIC_LAYER_TOOLS)
    if jev_ranker is not None and jev_config is not None:
        # Swap in the variant whose signature carries `question`. Same tool name,
        # so no new tool appears; when Jev is unconfigured the parameter is absent
        # from the schema entirely and behavior is unchanged.
        logger.info("Jev relevance filtering enabled for list_metrics")
        tools[tools.index(list_metrics)] = build_jev_list_metrics_tool(
            jev_ranker, jev_config
        )

    register_tools(
        dbt_mcp,
        [tool.adapt_context(bind_context) for tool in tools],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
