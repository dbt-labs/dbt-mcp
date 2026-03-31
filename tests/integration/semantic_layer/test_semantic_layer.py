import io
from dataclasses import replace

import pyarrow as pa
import pyarrow.csv
import pytest
from dbtsl.api.shared.query_params import GroupByParam, GroupByType

from dbt_mcp.config.config import load_config
from dbt_mcp.errors import GraphQLError
from dbt_mcp.semantic_layer.client import (
    DefaultSemanticLayerClientProvider,
    SemanticLayerFetcher,
)
from dbt_mcp.semantic_layer.types import OrderByParam, QueryMetricsError

config = load_config()


@pytest.fixture
def semantic_layer_fetcher() -> SemanticLayerFetcher:
    assert config.semantic_layer_config_provider is not None
    return SemanticLayerFetcher(
        client_provider=DefaultSemanticLayerClientProvider(),
    )


@pytest.fixture
async def semantic_layer_config():
    assert config.semantic_layer_config_provider is not None
    return await config.semantic_layer_config_provider.get_config()


async def test_semantic_layer_list_metrics(
    semantic_layer_fetcher: SemanticLayerFetcher,
    semantic_layer_config,
):
    metrics = await semantic_layer_fetcher.list_metrics(
        config=semantic_layer_config,
    )
    assert len(metrics) > 0


async def test_semantic_layer_sdk_respects_fetcher_config_environment_id():
    """SDK query must use the fetcher's semantic layer config, not default get_config().

    Regression guard for multi-project mode: GraphQL already passes environmentId from
    fetcher._config; dbtsl calls must use the same resolved config.
    """
    assert config.semantic_layer_config_provider is not None
    provider = config.semantic_layer_config_provider
    default_cfg = await provider.get_config()
    invalid_cfg = replace(default_cfg, prod_environment_id=9_999_999_999)

    fetcher = SemanticLayerFetcher(
        client_provider=DefaultSemanticLayerClientProvider(),
    )

    with pytest.raises(GraphQLError):
        await fetcher.list_metrics(config=invalid_cfg)

    result = await fetcher.query_metrics(config=invalid_cfg, metrics=["revenue"])
    assert isinstance(result, QueryMetricsError), (
        "query_metrics must not silently use the default environment when the fetcher "
        "is scoped to a different (invalid) semantic layer config"
    )


async def test_semantic_layer_list_dimensions(
    semantic_layer_fetcher: SemanticLayerFetcher,
    semantic_layer_config,
):
    metrics = await semantic_layer_fetcher.list_metrics(config=semantic_layer_config)
    dimensions = await semantic_layer_fetcher.get_dimensions(
        config=semantic_layer_config,
        metrics=[metrics[0].name],
    )
    assert len(dimensions) > 0
    # Verify metadata field exists and has correct type
    for dimension in dimensions:
        assert hasattr(dimension, "metadata")
        # Metadata must be either None or a dict, nothing else
        assert dimension.metadata is None or isinstance(dimension.metadata, dict), (
            f"metadata should be dict or None, got {type(dimension.metadata)}"
        )


async def test_semantic_layer_query_metrics(
    semantic_layer_fetcher: SemanticLayerFetcher,
    semantic_layer_config,
):
    result = await semantic_layer_fetcher.query_metrics(
        config=semantic_layer_config,
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


async def test_semantic_layer_query_metrics_invalid_query(
    semantic_layer_fetcher: SemanticLayerFetcher,
    semantic_layer_config,
):
    result = await semantic_layer_fetcher.query_metrics(
        config=semantic_layer_config,
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


async def test_semantic_layer_query_metrics_with_group_by_grain(
    semantic_layer_fetcher: SemanticLayerFetcher,
    semantic_layer_config,
):
    result = await semantic_layer_fetcher.query_metrics(
        config=semantic_layer_config,
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


async def test_semantic_layer_query_metrics_with_order_by(
    semantic_layer_fetcher: SemanticLayerFetcher,
    semantic_layer_config,
):
    result = await semantic_layer_fetcher.query_metrics(
        config=semantic_layer_config,
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


async def test_semantic_layer_query_metrics_with_misspellings(
    semantic_layer_fetcher: SemanticLayerFetcher,
    semantic_layer_config,
):
    result = await semantic_layer_fetcher.query_metrics(
        config=semantic_layer_config,
        metrics=["revehue"],
    )
    assert result.error is not None
    assert "revenue" in result.error


async def test_semantic_layer_get_entities(
    semantic_layer_fetcher: SemanticLayerFetcher,
    semantic_layer_config,
):
    metrics = await semantic_layer_fetcher.list_metrics(config=semantic_layer_config)
    assert len(metrics) > 0
    metric = metrics[0]
    entities = await semantic_layer_fetcher.get_entities(
        config=semantic_layer_config,
        metrics=[metric.name],
    )
    assert len(entities) > 0


async def test_semantic_layer_query_metrics_with_csv_formatter(
    semantic_layer_fetcher: SemanticLayerFetcher,
    semantic_layer_config,
):
    def csv_formatter(table: pa.Table) -> str:
        # Use PyArrow's native CSV writer instead of pandas
        buffer = io.BytesIO()
        pa.csv.write_csv(table, buffer)
        return buffer.getvalue().decode("utf-8")

    result = await semantic_layer_fetcher.query_metrics(
        config=semantic_layer_config,
        metrics=["revenue"],
        group_by=[
            GroupByParam(
                name="metric_time",
                type=GroupByType.TIME_DIMENSION,
                grain="day",
            )
        ],
        result_formatter=csv_formatter,
    )
    assert result.result is not None
    assert "revenue" in result.result.casefold()
    # CSV format should have comma separators
    assert "," in result.result
