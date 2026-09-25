from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dbtsl.models.metric import MetricType

from dbt_mcp.semantic_layer.client import SemanticLayerFetcher
from dbt_mcp.semantic_layer.tools import (
    SemanticLayerToolContext,
    list_metrics,
    metrics_to_csv,
)
from dbt_mcp.semantic_layer.types import ListMetricsResponse, MetricToolResponse


def _metric_item(
    name: str,
    metric_type: str = "simple",
    measures: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "name": name,
        "type": metric_type,
        "label": None,
        "description": None,
        "config": None,
        "dimensions": [{"name": "order_date"}],
        "entities": [{"name": "customer"}],
    }
    if measures is not None:
        item["measures"] = measures
    return item


def _make_dispatcher(related_items: list[dict[str, Any]]):
    """Dispatch by query: the lightweight metrics query gets bare items, the
    metrics_with_related query gets `related_items` (which carry measures)."""

    def dispatch(_, payload, **kwargs):
        query = payload.get("query", "")
        if "measures {" in query:
            return {"data": {"metricsPaginated": {"items": related_items}}}
        bare = [
            {
                k: v
                for k, v in i.items()
                if k not in ("dimensions", "entities", "measures")
            }
            for i in related_items
        ]
        return {"data": {"metricsPaginated": {"items": bare}}}

    return dispatch


@pytest.fixture
def config():
    return MagicMock(
        prod_environment_id=123,
        url="https://test-host/api/graphql",
        metrics_related_max=10,
    )


@pytest.fixture
def fetcher():
    return SemanticLayerFetcher(client_provider=AsyncMock())


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_simple_metric_resolves_to_its_measure_time_dimension(
    mock_submit_request, fetcher, config
):
    mock_submit_request.side_effect = _make_dispatcher(
        [
            _metric_item(
                "revenue",
                measures=[{"name": "revenue_amount", "aggTimeDimension": "order_date"}],
            )
        ]
    )
    result = await fetcher.list_metrics(config=config)
    assert result.metrics[0].default_time_dimension == ["order_date"]


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_ratio_metric_lists_each_differing_time_dimension_in_order(
    mock_submit_request, fetcher, config
):
    mock_submit_request.side_effect = _make_dispatcher(
        [
            _metric_item(
                "revenue_per_shipment",
                metric_type="ratio",
                measures=[
                    {"name": "revenue_amount", "aggTimeDimension": "order_date"},
                    {"name": "shipments", "aggTimeDimension": "ship_date"},
                ],
            )
        ]
    )
    result = await fetcher.list_metrics(config=config)
    assert result.metrics[0].default_time_dimension == ["order_date", "ship_date"]


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_measures_sharing_a_time_dimension_are_deduplicated(
    mock_submit_request, fetcher, config
):
    mock_submit_request.side_effect = _make_dispatcher(
        [
            _metric_item(
                "margin",
                metric_type="derived",
                measures=[
                    {"name": "revenue_amount", "aggTimeDimension": "order_date"},
                    {"name": "cost_amount", "aggTimeDimension": "order_date"},
                ],
            )
        ]
    )
    result = await fetcher.list_metrics(config=config)
    assert result.metrics[0].default_time_dimension == ["order_date"]


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
@pytest.mark.parametrize(
    "measures",
    [
        [],
        [{"name": "revenue_amount", "aggTimeDimension": None}],
    ],
    ids=["no-measures", "measure-without-agg-time-dimension"],
)
async def test_unresolvable_metric_yields_empty_list_not_none(
    mock_submit_request, fetcher, config, measures
):
    mock_submit_request.side_effect = _make_dispatcher(
        [_metric_item("revenue", measures=measures)]
    )
    result = await fetcher.list_metrics(config=config)
    assert result.metrics[0].default_time_dimension == []


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_missing_measures_key_is_unresolved(mock_submit_request, fetcher, config):
    mock_submit_request.side_effect = _make_dispatcher([_metric_item("revenue")])
    result = await fetcher.list_metrics(config=config)
    assert result.metrics[0].default_time_dimension == []


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_failing_related_query_falls_back_to_dimensionless_response(
    mock_submit_request, fetcher, config
):
    """If the API rejected the added selection, callers keep the bare listing."""
    dispatch = _make_dispatcher(
        [_metric_item("revenue", measures=[{"aggTimeDimension": "order_date"}])]
    )

    def failing_dispatch(_, payload, **kwargs):
        if "measures {" in payload.get("query", ""):
            raise RuntimeError("Cannot query field 'measures' on type 'Metric'")
        return dispatch(_, payload, **kwargs)

    mock_submit_request.side_effect = failing_dispatch
    result = await fetcher.list_metrics(config=config)
    metric = result.metrics[0]
    assert metric.name == "revenue"
    assert metric.dimensions is None
    assert metric.entities is None
    assert metric.default_time_dimension is None


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_above_threshold_leaves_default_time_dimension_unset(
    mock_submit_request, fetcher, config
):
    config.metrics_related_max = 1
    mock_submit_request.side_effect = _make_dispatcher(
        [
            _metric_item("a", measures=[{"name": "m", "aggTimeDimension": "d"}]),
            _metric_item("b", measures=[{"name": "n", "aggTimeDimension": "d"}]),
        ]
    )
    result = await fetcher.list_metrics(config=config)
    assert [m.default_time_dimension for m in result.metrics] == [None, None]
    assert mock_submit_request.call_count == 1


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_related_query_requests_measure_agg_time_dimension(
    mock_submit_request, fetcher, config
):
    mock_submit_request.side_effect = _make_dispatcher(
        [_metric_item("revenue", measures=[])]
    )
    await fetcher.list_metrics(config=config)
    related_query = mock_submit_request.call_args_list[-1].args[1]["query"]
    assert "measures {" in related_query
    assert "aggTimeDimension" in related_query


def _response(*metrics: MetricToolResponse) -> ListMetricsResponse:
    return ListMetricsResponse(metrics=list(metrics))


@pytest.mark.asyncio
async def test_list_metrics_tool_emits_default_time_dimension_column():
    response = _response(
        MetricToolResponse(
            name="revenue",
            type=MetricType.SIMPLE,
            dimensions=["order_date"],
            entities=["customer"],
            default_time_dimension=["order_date"],
        )
    )
    context = MagicMock(spec=SemanticLayerToolContext)
    context.config_provider = MagicMock()
    context.config_provider.get_config = AsyncMock(
        return_value=MagicMock(metrics_related_max=10, max_response_chars=16000)
    )
    context.semantic_layer_fetcher = MagicMock()
    context.semantic_layer_fetcher.list_metrics = AsyncMock(return_value=response)

    result = await list_metrics.fn(context=context)

    lines = result.splitlines()
    assert lines[0] == "name,type,dimensions,entities,default_time_dimension"
    assert lines[1].startswith("revenue,")
    assert lines[1].endswith(",order_date,customer,order_date")


def test_csv_includes_default_time_dimension_column_when_resolved():
    result = metrics_to_csv(
        _response(
            MetricToolResponse(
                name="revenue",
                type=MetricType.SIMPLE,
                default_time_dimension=["order_date"],
            ),
            MetricToolResponse(
                name="revenue_per_shipment",
                type=MetricType.RATIO,
                default_time_dimension=["order_date", "ship_date"],
            ),
        )
    )
    lines = result.splitlines()
    assert lines[0].split(",")[-1] == "default_time_dimension"
    assert lines[1].endswith(",order_date")
    assert lines[2].endswith(',"order_date,ship_date"')


def test_csv_omits_column_and_note_when_not_fetched():
    result = metrics_to_csv(
        _response(MetricToolResponse(name="revenue", type=MetricType.SIMPLE))
    )
    assert "default_time_dimension" not in result
    assert "# Note:" not in result


def test_csv_notes_unresolved_metrics_when_column_is_empty():
    result = metrics_to_csv(
        _response(
            MetricToolResponse(
                name="revenue", type=MetricType.SIMPLE, default_time_dimension=[]
            ),
            MetricToolResponse(
                name="orders", type=MetricType.SIMPLE, default_time_dimension=[]
            ),
        )
    )
    lines = result.splitlines()
    assert lines[0].startswith("# Note:")
    assert "default_time_dimension" in lines[0]
    assert "any of these metrics" in lines[0]
    assert "'revenue'" not in lines[0] and "'orders'" not in lines[0]
    assert "default_time_dimension" not in lines[1]


def test_csv_note_names_only_the_unresolved_metrics():
    result = metrics_to_csv(
        _response(
            MetricToolResponse(
                name="revenue",
                type=MetricType.SIMPLE,
                default_time_dimension=["order_date"],
            ),
            MetricToolResponse(
                name="orders", type=MetricType.SIMPLE, default_time_dimension=[]
            ),
        )
    )
    lines = result.splitlines()
    assert lines[0].startswith("# Note:")
    assert "'orders'" in lines[0]
    assert "'revenue'" not in lines[0]
    assert "default_time_dimension" in lines[1]
