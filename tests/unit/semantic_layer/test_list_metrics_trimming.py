from unittest.mock import MagicMock

from dbtsl.models.metric import MetricType

from dbt_mcp.config.config_providers.base import SemanticLayerConfig
from dbt_mcp.semantic_layer.tools import metrics_to_csv
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
    header = result.splitlines()[0]
    assert "description" not in header
    assert "metadata" not in header
    assert "name" in header
    assert "metric_0" in result


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
