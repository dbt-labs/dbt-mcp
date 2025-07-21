import pytest
from dbtsl.api.shared.query_params import GroupByParam, GroupByType
from dbtsl.client.sync import SyncSemanticLayerClient

from dbt_mcp.config.config import load_config
from dbt_mcp.semantic_layer.client import SemanticLayerFetcher
from dbt_mcp.semantic_layer.types import OrderByParam

config = load_config()


@pytest.fixture
def semantic_layer_fetcher() -> SemanticLayerFetcher:
    sl_config = config.semantic_layer_config
    assert sl_config is not None
    return SemanticLayerFetcher(
        sl_client=SyncSemanticLayerClient(
            environment_id=sl_config.prod_environment_id,
            auth_token=sl_config.service_token,
            host=sl_config.host,
        ),
        config=sl_config,
    )


def test_semantic_layer_list_metrics(semantic_layer_fetcher: SemanticLayerFetcher):
    metrics = semantic_layer_fetcher.list_metrics()
    assert len(metrics) > 0


def test_semantic_layer_list_dimensions(semantic_layer_fetcher: SemanticLayerFetcher):
    metrics = semantic_layer_fetcher.list_metrics()
    dimensions = semantic_layer_fetcher.get_dimensions(metrics=[metrics[0].name])
    assert len(dimensions) > 0


def test_semantic_layer_query_metrics(semantic_layer_fetcher: SemanticLayerFetcher):
    result = semantic_layer_fetcher.query_metrics(
        metrics=["revenue"],
        group_by=[
            GroupByParam(
                name="metric_time",
                type=GroupByType.TIME_DIMENSION,
                grain=None,
            )
        ],
    )
    assert result is not None


def test_semantic_layer_query_metrics_invalid_query(
    semantic_layer_fetcher: SemanticLayerFetcher,
):
    result = semantic_layer_fetcher.query_metrics(
        metrics=["food_revenue"],
        group_by=[
            GroupByParam(
                name="order_id__location__location_name",
                type=GroupByType.DIMENSION,
                grain=None,
            ),
            GroupByParam(
                name="metric_time",
                type=GroupByType.TIME_DIMENSION,
                grain="MONTH",
            ),
        ],
        order_by=[
            OrderByParam(
                name="metric_time",
                descending=True,
            ),
            OrderByParam(
                name="food_revenue",
                descending=True,
            ),
        ],
        limit=5,
    )
    assert result is not None


def test_semantic_layer_query_metrics_with_group_by_grain(
    semantic_layer_fetcher: SemanticLayerFetcher,
):
    result = semantic_layer_fetcher.query_metrics(
        metrics=["revenue"],
        group_by=[
            GroupByParam(
                name="metric_time",
                type=GroupByType.TIME_DIMENSION,
                grain="day",
            )
        ],
    )
    assert result is not None


def test_semantic_layer_query_metrics_with_order_by(
    semantic_layer_fetcher: SemanticLayerFetcher,
):
    result = semantic_layer_fetcher.query_metrics(
        metrics=["revenue"],
        group_by=[
            GroupByParam(
                name="metric_time",
                type=GroupByType.TIME_DIMENSION,
                grain=None,
            )
        ],
        order_by=[OrderByParam(name="metric_time", descending=True)],
    )
    assert result is not None


def test_semantic_layer_query_metrics_with_misspellings(
    semantic_layer_fetcher: SemanticLayerFetcher,
):
    result = semantic_layer_fetcher.query_metrics(["revehue"])
    assert result.result is not None
    assert "revenue" in result.result


def test_semantic_layer_get_entities(semantic_layer_fetcher: SemanticLayerFetcher):
    entities = semantic_layer_fetcher.get_entities(
        metrics=["count_dbt_copilot_requests"]
    )
    assert len(entities) > 0
