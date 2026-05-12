from unittest.mock import AsyncMock, MagicMock

import pytest
from dbtsl.models.metric import MetricType

from dbt_mcp.config.config_providers.base import SemanticLayerConfig
from dbt_mcp.semantic_layer.tools import (
    SemanticLayerToolContext,
    list_metrics,
    metrics_to_csv,
)
from dbt_mcp.semantic_layer.types import ListMetricsResponse, MetricToolResponse


def test_semantic_layer_config_max_response_chars_default():
    config = SemanticLayerConfig(
        url="https://example.com",
        host="example.com",
        prod_environment_id=1,
        token_provider=MagicMock(),
        headers_provider=MagicMock(),
    )
    assert config.max_response_chars == 16000


def test_semantic_layer_config_max_response_chars_custom():
    config = SemanticLayerConfig(
        url="https://example.com",
        host="example.com",
        prod_environment_id=1,
        token_provider=MagicMock(),
        headers_provider=MagicMock(),
        max_response_chars=8000,
    )
    assert config.max_response_chars == 8000


def _make_response(count: int, description: str | None = None) -> ListMetricsResponse:
    return ListMetricsResponse(
        metrics=[
            MetricToolResponse(
                name=f"metric_{i}",
                type=MetricType.SIMPLE,
                label=f"Metric {i}",
                description=description,
                metadata={"key": "value"} if description else None,
            )
            for i in range(count)
        ]
    )


def test_no_trimming_when_response_fits():
    """When CSV fits within max_response_chars, description and metadata are kept."""
    response = _make_response(3, description="short")
    result = metrics_to_csv(response, max_response_chars=16000)
    assert "short" in result
    assert "description" in result.splitlines()[0]


def test_trims_when_csv_exceeds_max_chars():
    """When CSV exceeds max_response_chars, description and metadata are stripped."""
    response = _make_response(2, description="A " * 500)  # ~1000 chars each
    result = metrics_to_csv(response, max_response_chars=100)
    lines = result.splitlines()
    # First line should be the trim notice
    assert lines[0].startswith("# Note:")
    assert "description" in lines[0] and "metadata" in lines[0]
    assert "search" in lines[0]
    # Header is the second line; trimmed columns are absent
    header = lines[1]
    assert "description" not in header
    assert "metadata" not in header
    assert "name" in header
    assert "metric_0" in result


def test_no_trim_notice_when_response_fits():
    """The trim notice must only appear when trimming actually happens."""
    response = _make_response(3, description="short")
    result = metrics_to_csv(response, max_response_chars=16000)
    assert not result.startswith("# Note:")


def test_trimming_disabled_when_max_is_zero():
    """max_response_chars=0 disables trimming."""
    response = _make_response(2, description="A " * 500)
    result = metrics_to_csv(response, max_response_chars=0)
    assert "description" in result.splitlines()[0]


def test_empty_response_returns_empty_string():
    result = metrics_to_csv(ListMetricsResponse(metrics=[]))
    assert result == ""


def test_columns_without_data_are_omitted():
    """Columns with all-None values are not included."""
    response = _make_response(2, description=None)
    result = metrics_to_csv(response)
    header = result.splitlines()[0]
    assert "description" not in header
    assert "metadata" not in header
    assert "name" in header


def _make_context(
    response: ListMetricsResponse, config: MagicMock
) -> SemanticLayerToolContext:
    context = MagicMock(spec=SemanticLayerToolContext)
    context.config_provider = MagicMock()
    context.config_provider.get_config = AsyncMock(return_value=config)
    context.semantic_layer_fetcher = MagicMock()
    context.semantic_layer_fetcher.list_metrics = AsyncMock(return_value=response)
    return context


@pytest.mark.asyncio
async def test_list_metrics_skips_trim_for_small_result_set():
    """A small result set (<= metrics_related_max) is never trimmed, even if verbose."""
    # 2 metrics each with a huge description, well over max_response_chars
    response = _make_response(2, description="X" * 20000)
    config = MagicMock(metrics_related_max=10, max_response_chars=100)
    context = _make_context(response, config)

    result = await list_metrics.fn(context=context)

    assert not result.startswith("# Note:")
    assert "description" in result.splitlines()[0]
    # The verbose description survives untrimmed
    assert "X" * 100 in result


@pytest.mark.asyncio
async def test_list_metrics_trims_broad_listing():
    """A result set above metrics_related_max is trimmed when it exceeds max_response_chars."""
    response = _make_response(15, description="A " * 500)
    config = MagicMock(metrics_related_max=10, max_response_chars=200)
    context = _make_context(response, config)

    result = await list_metrics.fn(context=context)

    assert result.startswith("# Note:")
    header = result.splitlines()[1]
    assert "description" not in header
    assert "metadata" not in header
