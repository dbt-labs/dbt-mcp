"""Tests for `question`-aware list_metrics: ranking, fallbacks, and schema gating."""

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest
from dbtsl.models.dimension import DimensionType
from dbtsl.models.metric import MetricType

from dbt_mcp.config.config_providers.base import SemanticLayerConfig
from dbt_mcp.semantic_layer.jev import (
    JevConfig,
    JevUnavailableError,
    RankedItem,
)
from dbt_mcp.semantic_layer.tools import (
    SemanticLayerToolContext,
    build_jev_list_metrics,
    metrics_to_csv,
)
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    ListMetricsResponse,
    MetricToolResponse,
)

# Near-duplicate names that differ only by description — the case that motivates
# ranking on descriptions rather than trimming them away.
CATALOG = [
    MetricToolResponse(
        name="revenue_churn_self_serve",
        type=MetricType.SIMPLE,
        label="Revenue Churn (Self-Serve)",
        description="Revenue lost to churn, self-serve customers only.",
    ),
    MetricToolResponse(
        name="revenue_churn_enterprise",
        type=MetricType.SIMPLE,
        label="Revenue Churn (Enterprise)",
        description="Revenue lost to churn, enterprise customers only.",
    ),
    MetricToolResponse(
        name="new_signups",
        type=MetricType.SIMPLE,
        label="Account Signups",
        description="Count of new account signups.",
    ),
]

DIMENSIONS = [
    DimensionToolResponse(
        name="metric_time",
        type=DimensionType.TIME,
        description="Standard time dimension.",
        granularities=["day", "month"],
    ),
    DimensionToolResponse(
        name="account__subscription_tier",
        type=DimensionType.CATEGORICAL,
        description="The product tier the account is on.",
    ),
]


def _config() -> SemanticLayerConfig:
    return SemanticLayerConfig(
        url="https://example.com",
        host="example.com",
        prod_environment_id=1,
        token_provider=MagicMock(),
        headers_provider=MagicMock(),
    )


def _context(metrics, dimensions=None) -> SemanticLayerToolContext:
    context = MagicMock(spec=SemanticLayerToolContext)
    context.config_provider = MagicMock()
    context.config_provider.get_config = AsyncMock(return_value=_config())
    fetcher = MagicMock()
    fetcher.list_metrics = AsyncMock(return_value=ListMetricsResponse(metrics=metrics))
    fetcher.get_dimensions = AsyncMock(return_value=dimensions or DIMENSIONS)
    context.semantic_layer_fetcher = fetcher
    return context


class FakeRanker:
    """Stands in for TypeSafe so unit tests never touch the network."""

    def __init__(self, results=None, error: Exception | None = None):
        self.results = results or {}
        self.error = error
        self.calls: list[dict] = []

    async def rank_groups(self, *, question, groups, kind, top_k):
        self.calls.append(
            {
                "question": question,
                "groups": {k: list(v) for k, v in groups.items()},
                "kind": kind,
                "top_k": top_k,
            }
        )
        if self.error:
            raise self.error
        return self.results.get(kind, {g: [] for g in groups})


JEV_CONFIG = JevConfig(api_key="test-key", top_k_metrics=2, top_k_dimensions=2)


def _ranker_picking_churn_self_serve():
    return FakeRanker(
        results={
            "metric": {
                "metrics": [
                    RankedItem("revenue_churn_self_serve", 0.96),
                    RankedItem("revenue_churn_enterprise", 0.62),
                ]
            },
            "dimension": {"dimensions": [RankedItem("metric_time", 0.88)]},
        }
    )


@pytest.mark.asyncio
async def test_ranks_metrics_and_keeps_descriptions():
    ranker = _ranker_picking_churn_self_serve()
    tool = build_jev_list_metrics(ranker, JEV_CONFIG)
    result = await tool(
        _context(CATALOG), question="How much ARR did we lose to self-serve churn?"
    )

    assert "revenue_churn_self_serve" in result
    assert "new_signups" not in result, "unranked metric should be filtered out"
    assert "Revenue lost to churn, self-serve customers only." in result, (
        "descriptions must survive — they are the disambiguating signal"
    )
    assert "relevance" in result


@pytest.mark.asyncio
async def test_includes_dimension_block_with_descriptions():
    tool = build_jev_list_metrics(_ranker_picking_churn_self_serve(), JEV_CONFIG)
    result = await tool(_context(CATALOG), question="self-serve churn over time")

    assert "metric_time" in result
    assert "Standard time dimension." in result


@pytest.mark.asyncio
async def test_question_omitted_behaves_exactly_like_today():
    tool = build_jev_list_metrics(_ranker_picking_churn_self_serve(), JEV_CONFIG)
    context = _context(CATALOG)
    result = await tool(context)

    expected = metrics_to_csv(ListMetricsResponse(metrics=CATALOG))
    assert result == expected
    assert context.semantic_layer_fetcher.get_dimensions.await_count == 0


@pytest.mark.asyncio
async def test_jev_failure_falls_back_to_todays_output():
    ranker = FakeRanker(error=JevUnavailableError("timed out"))
    tool = build_jev_list_metrics(ranker, JEV_CONFIG)
    result = await tool(_context(CATALOG), question="anything at all")

    assert result == metrics_to_csv(ListMetricsResponse(metrics=CATALOG))


@pytest.mark.asyncio
async def test_zero_lexical_results_widens_to_full_catalog_with_a_note():
    context = _context(CATALOG)
    # First call (with `search`) matches nothing; the widened retry returns all.
    context.semantic_layer_fetcher.list_metrics = AsyncMock(
        side_effect=[
            ListMetricsResponse(metrics=[]),
            ListMetricsResponse(metrics=CATALOG),
        ]
    )
    tool = build_jev_list_metrics(_ranker_picking_churn_self_serve(), JEV_CONFIG)
    result = await tool(context, question="self-serve churn", search="tier")

    assert "# Note:" in result
    assert "tier" in result
    assert "revenue_churn_self_serve" in result
    assert context.semantic_layer_fetcher.list_metrics.await_count == 2
    assert (
        context.semantic_layer_fetcher.list_metrics.await_args_list[1].kwargs["search"]
        is None
    )


@pytest.mark.asyncio
async def test_zero_results_without_question_does_not_widen():
    context = _context([])
    context.semantic_layer_fetcher.list_metrics = AsyncMock(
        return_value=ListMetricsResponse(metrics=[])
    )
    tool = build_jev_list_metrics(_ranker_picking_churn_self_serve(), JEV_CONFIG)
    await tool(context, search="tier")

    assert context.semantic_layer_fetcher.list_metrics.await_count == 1


@pytest.mark.asyncio
async def test_question_never_reaches_the_dimension_cache_key():
    context = _context(CATALOG)
    tool = build_jev_list_metrics(_ranker_picking_churn_self_serve(), JEV_CONFIG)
    await tool(context, question="self-serve churn over time")

    for call in context.semantic_layer_fetcher.get_dimensions.await_args_list:
        assert "question" not in call.kwargs
        assert call.kwargs.get("search") is None


@pytest.mark.asyncio
async def test_dimensions_fetched_per_metric_not_intersected():
    """get_dimensions intersects across metrics, so each ranked metric is fetched
    separately rather than in one multi-metric call."""
    context = _context(CATALOG)
    tool = build_jev_list_metrics(_ranker_picking_churn_self_serve(), JEV_CONFIG)
    await tool(context, question="self-serve churn over time")

    for call in context.semantic_layer_fetcher.get_dimensions.await_args_list:
        assert len(call.kwargs["metrics"]) == 1


@pytest.mark.asyncio
async def test_dimensions_fetched_for_every_ranked_metric():
    """Unlike the old score-ratio gate, no ranked metric is excluded from the
    dimension fetch - the fan-out is bounded by `top_k_metrics` alone."""
    context = _context(CATALOG)
    tool = build_jev_list_metrics(_ranker_picking_churn_self_serve(), JEV_CONFIG)
    await tool(context, question="self-serve churn")

    assert context.semantic_layer_fetcher.get_dimensions.await_count == 2


def _register(jev_config):
    from dbt_mcp.semantic_layer.tools import register_sl_tools
    from tests.conftest import MockFastMCP

    fastmcp = MockFastMCP()
    register_sl_tools(
        fastmcp,
        config_provider=MagicMock(),
        client_provider=MagicMock(),
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
        jev_ranker=FakeRanker() if jev_config else None,
        jev_config=jev_config,
    )
    return fastmcp


def _list_metrics_params(fastmcp) -> set[str]:
    for fn, kwargs in zip(fastmcp.tools.values(), fastmcp.tool_kwargs.values()):
        if kwargs["name"] == "list_metrics":
            return set(inspect.signature(fn).parameters)
    raise AssertionError("list_metrics was not registered")


def test_question_param_absent_from_schema_when_jev_disabled():
    params = _list_metrics_params(_register(None))
    assert "question" not in params, (
        "with Jev off the parameter must not exist at all, so clients cannot send it"
    )
    assert {"search", "meta_filter"} <= params


def test_question_param_present_when_jev_enabled():
    assert "question" in _list_metrics_params(_register(JEV_CONFIG))


def test_jev_does_not_add_a_new_tool():
    without = {k["name"] for k in _register(None).tool_kwargs.values()}
    with_jev = {k["name"] for k in _register(JEV_CONFIG).tool_kwargs.values()}
    assert without == with_jev
    assert "list_metrics" in with_jev


@pytest.mark.asyncio
async def test_runner_up_metric_still_gets_dimensions_fetched():
    """No score-ratio gate anymore: a weak runner-up's dimensions are still
    fetched and folded into the shared, deduped table."""
    ranker = FakeRanker(
        results={
            "metric": {
                "metrics": [
                    RankedItem("new_signups", 0.95),
                    RankedItem("revenue_churn_enterprise", 0.64),
                ]
            },
            "dimension": {"dimensions": [RankedItem("metric_time", 0.9)]},
        }
    )
    context = _context(CATALOG)
    tool = build_jev_list_metrics(ranker, JevConfig(api_key="k", top_k_metrics=2))
    result = await tool(context, question="how many signups?")

    assert context.semantic_layer_fetcher.get_dimensions.await_count == 2
    assert "# Dimensions for `new_signups`, `revenue_churn_enterprise`" in result
    assert "revenue_churn_enterprise" in result


@pytest.mark.asyncio
async def test_dimensions_deduped_across_ranked_metrics_with_metrics_column():
    """Metrics ranked together usually share most dimensions, so the union is
    ranked and rendered once. The `metrics` column marks which ranked metric(s)
    actually carry each dimension - `all` when shared, a name otherwise."""
    dims_a = [
        DimensionToolResponse(
            name="metric_time",
            type=DimensionType.TIME,
            description="Standard time dimension.",
            granularities=["day"],
        ),
        DimensionToolResponse(
            name="account__subscription_tier",
            type=DimensionType.CATEGORICAL,
            description="The product tier the account is on.",
        ),
    ]
    dims_b = dims_a + [
        DimensionToolResponse(
            name="account__region",
            type=DimensionType.CATEGORICAL,
            description="Sales region.",
        )
    ]
    ranker = FakeRanker(
        results={
            "metric": {
                "metrics": [
                    RankedItem("revenue_churn_self_serve", 0.9),
                    RankedItem("revenue_churn_enterprise", 0.85),
                ]
            },
            "dimension": {
                "dimensions": [
                    RankedItem("metric_time", 0.9),
                    RankedItem("account__subscription_tier", 0.8),
                    RankedItem("account__region", 0.7),
                ]
            },
        }
    )
    context = _context(CATALOG)
    context.semantic_layer_fetcher.get_dimensions = AsyncMock(
        side_effect=[dims_a, dims_b]
    )
    tool = build_jev_list_metrics(ranker, JevConfig(api_key="k", top_k_metrics=2))
    result = await tool(context, question="churn by region?")

    lines = result.splitlines()
    time_line = next(line for line in lines if line.startswith("metric_time,"))
    tier_line = next(
        line for line in lines if line.startswith("account__subscription_tier,")
    )
    region_line = next(line for line in lines if line.startswith("account__region,"))

    assert time_line.endswith(",all"), "shared by both ranked metrics"
    assert tier_line.endswith(",all"), "shared by both ranked metrics"
    assert region_line.endswith(",revenue_churn_enterprise"), (
        "only the second metric has this dimension"
    )
    assert (
        "# Dimensions for `revenue_churn_self_serve`, `revenue_churn_enterprise`"
        in result
    )
