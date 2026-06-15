from unittest.mock import Mock

import pytest

from dbt_mcp.discovery.client import ModelsFetcher


@pytest.fixture
def models_fetcher():
    return ModelsFetcher(paginator=Mock())


async def test_fetch_model_health_wraps_single_node_in_list(
    models_fetcher, mock_api_client, unit_discovery_config
):
    """fetch_model_health must return a list[dict], not a bare dict."""
    node = {
        "uniqueId": "model.project.my_model",
        "executionInfo": {"lastSuccessJobDefinitionId": 1, "lastRunStatus": "success"},
        "tests": [{"name": "not_null", "status": "pass"}],
        "ancestors": [],
    }
    mock_api_client.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "models": {
                        "edges": [{"node": node}]
                    }
                }
            }
        }
    }

    result = await models_fetcher.fetch_model_health(
        unique_id="model.project.my_model", config=unit_discovery_config
    )

    assert isinstance(result, list), (
        "fetch_model_health must return a list, not a bare dict"
    )
    assert len(result) == 1
    assert result[0] == node


async def test_fetch_model_health_empty_edges_returns_empty_list(
    models_fetcher, mock_api_client, unit_discovery_config
):
    """fetch_model_health returns [] when no model is found."""
    mock_api_client.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "models": {
                        "edges": []
                    }
                }
            }
        }
    }

    result = await models_fetcher.fetch_model_health(
        unique_id="model.project.nonexistent", config=unit_discovery_config
    )

    assert result == []
