from dbtsl.api.shared.query_params import GroupByParam, GroupByType

from dbt_mcp.config.config import load_config
from dbt_mcp.semantic_layer.client import get_semantic_layer_fetcher
from dbt_mcp.semantic_layer.types import OrderByParam

config = load_config()


def test_semantic_layer_list_metrics():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    metrics = semantic_layer_fetcher.list_metrics()
    assert len(metrics) > 0


def test_semantic_layer_list_dimensions():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    metrics = semantic_layer_fetcher.list_metrics()
    dimensions = semantic_layer_fetcher.get_dimensions(metrics=[metrics[0].name])
    assert len(dimensions) > 0


def test_semantic_layer_query_metrics():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    result = semantic_layer_fetcher.query_metrics(
        metrics=["revenue"],
        group_by=[
            GroupByParam(
                name="metric_time",
                type=GroupByType.DIMENSION,
                grain=None,
            )
        ],
    )
    assert result is not None


def test_semantic_layer_query_metrics_with_group_by_grain():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    result = semantic_layer_fetcher.query_metrics(
        metrics=["revenue"],
        group_by=[
            GroupByParam(
                name="metric_time",
                type=GroupByType.DIMENSION,
                grain="day",
            )
        ],
    )
    assert result is not None


def test_semantic_layer_query_metrics_with_order_by():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    result = semantic_layer_fetcher.query_metrics(
        metrics=["revenue"],
        group_by=[
            GroupByParam(
                name="metric_time",
                type=GroupByType.DIMENSION,
                grain=None,
            )
        ],
        order_by=[OrderByParam(name="metric_time", descending=True)],
    )
    assert result is not None


def test_semantic_layer_query_metrics_with_misspellings():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    result = semantic_layer_fetcher.query_metrics(["revehue"])
    assert result is not None
    assert "revenue" in result


def test_semantic_layer_get_entities():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    entities = semantic_layer_fetcher.get_entities(
        metrics=["count_dbt_copilot_requests"]
    )
    assert len(entities) > 0
