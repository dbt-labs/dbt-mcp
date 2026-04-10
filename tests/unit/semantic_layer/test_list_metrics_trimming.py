from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.config.config_providers.base import SemanticLayerConfig
from dbt_mcp.semantic_layer.client import SemanticLayerFetcher


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


def _make_config(
    max_response_chars: int = 16000, metrics_related_max: int = 2
) -> SemanticLayerConfig:
    return SemanticLayerConfig(
        url="https://example.com/api/graphql",
        host="example.com",
        prod_environment_id=1,
        token_provider=MagicMock(),
        headers_provider=MagicMock(),
        metrics_related_max=metrics_related_max,
        max_response_chars=max_response_chars,
    )


def _make_metrics_result(count: int, with_description: bool = True) -> dict:
    """Build a fake metricsPaginated GraphQL response."""
    items = [
        {
            "name": f"metric_{i}",
            "type": "SIMPLE",
            "label": f"Metric {i}",
            "description": "A " * 500 if with_description else "",  # ~1000 chars each
            "config": {"meta": {"key": "value"}},
        }
        for i in range(count)
    ]
    return {"data": {"metricsPaginated": {"items": items}}}


@pytest.mark.asyncio
async def test_list_metrics_no_trimming_when_small_enough():
    """When names-only response fits within max_response_chars, keep description and metadata."""
    config = _make_config(max_response_chars=16000)
    fetcher = SemanticLayerFetcher(client_provider=MagicMock())

    # 3 metrics > metrics_related_max=2, so names-only path
    # but small enough to fit in 16000 chars
    small_metrics = {
        "data": {
            "metricsPaginated": {
                "items": [
                    {
                        "name": "m1",
                        "type": "SIMPLE",
                        "label": "M1",
                        "description": "short",
                        "config": {"meta": {}},
                    },
                    {
                        "name": "m2",
                        "type": "SIMPLE",
                        "label": "M2",
                        "description": "short",
                        "config": {"meta": {}},
                    },
                    {
                        "name": "m3",
                        "type": "SIMPLE",
                        "label": "M3",
                        "description": "short",
                        "config": {"meta": {}},
                    },
                ]
            }
        }
    }

    with patch(
        "dbt_mcp.semantic_layer.client.submit_request", new_callable=AsyncMock
    ) as mock_req:
        mock_req.return_value = small_metrics
        result = await fetcher.list_metrics(config)

    assert result.metrics[0].description == "short"
    assert result.metrics[0].metadata == {}


@pytest.mark.asyncio
async def test_list_metrics_trims_when_response_exceeds_max_chars():
    """When names-only response exceeds max_response_chars, strip description and metadata."""
    # Set a very small limit to force trimming
    config = _make_config(max_response_chars=200, metrics_related_max=1)
    fetcher = SemanticLayerFetcher(client_provider=MagicMock())

    # 2 metrics > metrics_related_max=1 → names-only path
    # With long descriptions, response will exceed 200 chars
    big_metrics = _make_metrics_result(count=2, with_description=True)

    with patch(
        "dbt_mcp.semantic_layer.client.submit_request", new_callable=AsyncMock
    ) as mock_req:
        mock_req.return_value = big_metrics
        result = await fetcher.list_metrics(config)

    # description and metadata should be stripped
    assert result.metrics[0].description is None
    assert result.metrics[0].metadata is None
    # but name, type, label must be preserved
    assert result.metrics[0].name == "metric_0"
    assert result.metrics[0].type == "SIMPLE"
    assert result.metrics[0].label == "Metric 0"


@pytest.mark.asyncio
async def test_list_metrics_trimming_not_applied_in_full_config_path():
    """The full-config path (count <= metrics_related_max) is never trimmed."""
    config = _make_config(
        max_response_chars=10, metrics_related_max=10
    )  # limit=10 chars, tiny
    fetcher = SemanticLayerFetcher(client_provider=MagicMock())

    one_metric = {
        "data": {
            "metricsPaginated": {
                "items": [
                    {
                        "name": "m1",
                        "type": "SIMPLE",
                        "label": "M1",
                        "description": "keep me",
                        "config": {"meta": {"x": 1}},
                        "dimensions": [],
                        "entities": [],
                    },
                ]
            }
        }
    }

    with patch(
        "dbt_mcp.semantic_layer.client.submit_request", new_callable=AsyncMock
    ) as mock_req:
        mock_req.return_value = one_metric
        result = await fetcher.list_metrics(config)

    # Full config path — description must be kept even though max_response_chars=10
    assert result.metrics[0].description == "keep me"
    assert result.metrics[0].dimensions == []
