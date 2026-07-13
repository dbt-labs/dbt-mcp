from unittest.mock import AsyncMock, MagicMock

import pytest
from dbtsl.models.metric import MetricType

from dbt_mcp.config.config_providers.base import SemanticLayerConfig
from dbt_mcp.semantic_layer.tools import (
    SemanticLayerToolContext,
    filter_metrics_by_meta,
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
    """When CSV exceeds max_response_chars and pass 2 is needed, both columns are dropped."""
    response = _make_response(2, description="A " * 500)  # ~1000 chars each
    result = metrics_to_csv(response, max_response_chars=100)
    lines = result.splitlines()
    # First line should be the trim notice listing both dropped columns
    assert lines[0].startswith("# Note:")
    assert "description" in lines[0] and "metadata" in lines[0]
    assert "search" in lines[0]
    # Header is the second line; both trimmed columns are absent
    header = lines[1]
    assert "description" not in header
    assert "metadata" not in header
    assert "name" in header
    assert "metric_0" in result


def test_description_dropped_but_metadata_kept_when_pass1_sufficient():
    """When dropping description alone brings response under budget, metadata is preserved."""
    # description is long enough to push over budget, but metadata is small
    response = ListMetricsResponse(
        metrics=[
            MetricToolResponse(
                name=f"metric_{i}",
                type=MetricType.SIMPLE,
                label=f"Metric {i}",
                description="A " * 100,
                metadata={"agent_accessible": True},
            )
            for i in range(2)
        ]
    )
    # Full CSV >> 200 chars; without description ≈ 100 chars (under budget)
    result = metrics_to_csv(response, max_response_chars=200)
    lines = result.splitlines()
    assert lines[0].startswith("# Note:")
    assert "'description'" in lines[0]
    assert "'metadata'" not in lines[0]
    header = lines[1]
    assert "description" not in header
    assert "metadata" in header


def test_metadata_dropped_only_when_pass1_insufficient():
    """When dropping description still leaves response over budget, metadata is also dropped."""
    # Many metrics with metadata but no description — large even after pass 1
    response = ListMetricsResponse(
        metrics=[
            MetricToolResponse(
                name=f"metric_{i}",
                type=MetricType.SIMPLE,
                metadata={"key": "value"},
            )
            for i in range(100)
        ]
    )
    result = metrics_to_csv(response, max_response_chars=100)
    lines = result.splitlines()
    assert lines[0].startswith("# Note:")
    assert "'metadata'" in lines[0]
    header = lines[1]
    assert "metadata" not in header


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


@pytest.mark.asyncio
async def test_meta_filter_returns_only_matching_metrics():
    """meta_filter keeps only metrics whose metadata contains all specified key-value pairs."""
    metrics = [
        MetricToolResponse(
            name=f"metric_{i}",
            type=MetricType.SIMPLE,
            metadata={"agent_accessible": i < 2},
        )
        for i in range(5)
    ]
    response = ListMetricsResponse(metrics=metrics)
    config = MagicMock(metrics_related_max=10, max_response_chars=16000)
    context = _make_context(response, config)

    result = await list_metrics.fn(
        context=context, meta_filter={"agent_accessible": True}
    )

    data_lines = [line for line in result.splitlines() if not line.startswith("#")]
    assert len(data_lines) == 3  # header + 2 matching rows


@pytest.mark.asyncio
async def test_meta_filter_none_returns_all_metrics():
    """meta_filter=None returns all metrics unchanged."""
    response = _make_response(5, description="short")
    config = MagicMock(metrics_related_max=10, max_response_chars=16000)
    context = _make_context(response, config)

    result = await list_metrics.fn(context=context, meta_filter=None)

    data_lines = [line for line in result.splitlines() if not line.startswith("#")]
    assert len(data_lines) == 6  # header + 5 rows


@pytest.mark.asyncio
async def test_meta_filter_excludes_metrics_without_metadata():
    """Metrics with metadata=None are excluded when a meta_filter is provided."""
    metrics = [
        MetricToolResponse(
            name="metric_with_meta", type=MetricType.SIMPLE, metadata={"flag": True}
        ),
        MetricToolResponse(
            name="metric_no_meta", type=MetricType.SIMPLE, metadata=None
        ),
    ]
    response = ListMetricsResponse(metrics=metrics)
    config = MagicMock(metrics_related_max=10, max_response_chars=16000)
    context = _make_context(response, config)

    result = await list_metrics.fn(context=context, meta_filter={"flag": True})

    assert "metric_with_meta" in result
    assert "metric_no_meta" not in result


@pytest.mark.asyncio
async def test_meta_filter_multikey_requires_all_pairs():
    """meta_filter with multiple keys requires all pairs to match."""
    metrics = [
        MetricToolResponse(
            name="both_match", type=MetricType.SIMPLE, metadata={"a": 1, "b": 2}
        ),
        MetricToolResponse(
            name="b_mismatch", type=MetricType.SIMPLE, metadata={"a": 1, "b": 99}
        ),
        MetricToolResponse(name="b_missing", type=MetricType.SIMPLE, metadata={"a": 1}),
    ]
    response = ListMetricsResponse(metrics=metrics)
    config = MagicMock(metrics_related_max=10, max_response_chars=16000)
    context = _make_context(response, config)

    result = await list_metrics.fn(context=context, meta_filter={"a": 1, "b": 2})

    assert "both_match" in result
    assert "b_mismatch" not in result
    assert "b_missing" not in result


def test_filter_metrics_by_meta_normalizes_string_booleans():
    """String 'true'/'false' in meta_filter are normalized to Python booleans."""
    metrics = [
        MetricToolResponse(
            name="accessible",
            type=MetricType.SIMPLE,
            metadata={"agent_accessible": True},
        ),
        MetricToolResponse(
            name="not_accessible",
            type=MetricType.SIMPLE,
            metadata={"agent_accessible": False},
        ),
    ]
    response = ListMetricsResponse(metrics=metrics)

    # String "true" should match boolean True stored in metadata
    result = filter_metrics_by_meta(response, {"agent_accessible": "true"})
    assert len(result.metrics) == 1
    assert result.metrics[0].name == "accessible"

    # String "false" should match boolean False
    result = filter_metrics_by_meta(response, {"agent_accessible": "false"})
    assert len(result.metrics) == 1
    assert result.metrics[0].name == "not_accessible"

    # Non-boolean strings are not coerced
    result = filter_metrics_by_meta(response, {"agent_accessible": "yes"})
    assert len(result.metrics) == 0
