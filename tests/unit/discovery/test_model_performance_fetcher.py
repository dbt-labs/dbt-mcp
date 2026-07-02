from unittest.mock import AsyncMock

import pytest

from dbt_mcp.discovery.client import ModelPerformanceFetcher
from dbt_mcp.errors import InvalidParameterError, ToolCallError


@pytest.fixture
def models_fetcher():
    """Mock ModelsFetcher for name resolution."""
    return AsyncMock()


@pytest.fixture
def model_performance_fetcher(models_fetcher):
    """Create ModelPerformanceFetcher with mocked dependencies."""
    return ModelPerformanceFetcher(
        models_fetcher=models_fetcher,
    )


async def test_fetch_performance_with_unique_id(
    model_performance_fetcher, mock_api_client, unit_discovery_config
):
    """Test fetching latest run performance using unique_id."""
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "modelHistoricalRuns": [
                        {
                            "uniqueId": "model.analytics.stg_orders",
                            "runId": 12345678,
                            "status": "success",
                            "executeStartedAt": "2025-12-16T10:30:00Z",
                            "executionTime": 45.67,
                        }
                    ]
                }
            }
        }
    }

    mock_api_client.return_value = mock_response

    result = await model_performance_fetcher.fetch_performance(
        unit_discovery_config,
        unique_id="model.analytics.stg_orders",
        num_runs=1,
    )

    # Verify API was called correctly
    mock_api_client.assert_called_once()
    call_args = mock_api_client.call_args

    # Check query and variables
    query = call_args[0][0]
    assert "ModelHistoricalRuns" in query
    assert "modelHistoricalRuns" in query

    variables = call_args[0][1]
    assert variables["environmentId"] == 123
    assert variables["uniqueId"] == "model.analytics.stg_orders"
    assert variables["lastRunCount"] == 1

    # Verify result
    assert len(result) == 1
    assert result[0]["runId"] == 12345678
    assert result[0]["executionTime"] == 45.67
    assert result[0]["status"] == "success"


async def test_fetch_performance_with_name_resolution(
    model_performance_fetcher,
    mock_api_client,
    models_fetcher,
    unit_discovery_config,
):
    """Test fetching performance using model name (requires resolution)."""
    # Mock name resolution
    models_fetcher.resolve_unique_ids_by_name.return_value = [
        "model.analytics.stg_orders"
    ]

    # Mock performance query
    mock_api_client.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "modelHistoricalRuns": [
                        {
                            "uniqueId": "model.analytics.stg_orders",
                            "runId": 12345678,
                            "status": "success",
                            "executeStartedAt": "2025-12-16T10:30:00Z",
                            "executionTime": 45.67,
                        }
                    ]
                }
            }
        }
    }

    result = await model_performance_fetcher.fetch_performance(
        unit_discovery_config,
        name="stg_orders",
        num_runs=1,
    )

    # Verify name was resolved via models_fetcher
    models_fetcher.resolve_unique_ids_by_name.assert_called_once_with(
        "stg_orders",
        config=unit_discovery_config,
    )

    # Verify result contains correct data
    assert result[0]["uniqueId"] == "model.analytics.stg_orders"
    assert result[0]["runId"] == 12345678


async def test_fetch_performance_multiple_runs(
    model_performance_fetcher, mock_api_client, unit_discovery_config
):
    """Test fetching multiple historical runs."""
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "modelHistoricalRuns": [
                        {
                            "uniqueId": "model.analytics.stg_orders",
                            "runId": 12345,
                            "status": "success",
                            "executeStartedAt": "2025-12-16T10:30:00Z",
                            "executionTime": 45.67,
                        },
                        {
                            "uniqueId": "model.analytics.stg_orders",
                            "runId": 12344,
                            "status": "success",
                            "executeStartedAt": "2025-12-15T10:30:00Z",
                            "executionTime": 48.23,
                        },
                        {
                            "uniqueId": "model.analytics.stg_orders",
                            "runId": 12343,
                            "status": "success",
                            "executeStartedAt": "2025-12-14T10:30:00Z",
                            "executionTime": 44.15,
                        },
                    ]
                }
            }
        }
    }

    mock_api_client.return_value = mock_response

    result = await model_performance_fetcher.fetch_performance(
        unit_discovery_config,
        unique_id="model.analytics.stg_orders",
        num_runs=3,
    )

    # Verify variables
    call_args = mock_api_client.call_args
    variables = call_args[0][1]
    assert variables["lastRunCount"] == 3

    # Verify result contains all runs
    assert len(result) == 3
    assert result[0]["runId"] == 12345  # Most recent first
    assert result[1]["runId"] == 12344
    assert result[2]["runId"] == 12343


async def test_fetch_performance_no_parameters_raises_error(
    model_performance_fetcher, unit_discovery_config
):
    """Test that missing both name and unique_id raises InvalidParameterError."""
    with pytest.raises(
        InvalidParameterError, match="Either 'name' or 'unique_id' must be provided"
    ):
        await model_performance_fetcher.fetch_performance(
            unit_discovery_config, num_runs=1
        )


async def test_fetch_performance_model_not_found(
    model_performance_fetcher, models_fetcher, unit_discovery_config
):
    """Test error when model name doesn't resolve to any model."""
    models_fetcher.resolve_unique_ids_by_name.return_value = []

    with pytest.raises(ToolCallError, match="Model not found"):
        await model_performance_fetcher.fetch_performance(
            unit_discovery_config,
            name="nonexistent_model",
            num_runs=1,
        )


async def test_fetch_performance_multiple_name_matches(
    model_performance_fetcher, models_fetcher, unit_discovery_config
):
    """Ensure ambiguous name lookups raise ToolCallError."""
    models_fetcher.resolve_unique_ids_by_name.return_value = [
        "model.pkg_a.duplicate",
        "model.pkg_b.duplicate",
    ]

    with pytest.raises(
        ToolCallError, match="Multiple models found for name 'duplicate'"
    ):
        await model_performance_fetcher.fetch_performance(
            unit_discovery_config,
            name="duplicate",
        )


async def test_fetch_performance_no_runs_returns_empty(
    model_performance_fetcher, mock_api_client, unit_discovery_config
):
    """Test that models with no historical runs return empty list."""
    mock_response = {"data": {"environment": {"applied": {"modelHistoricalRuns": []}}}}

    mock_api_client.return_value = mock_response

    result = await model_performance_fetcher.fetch_performance(
        unit_discovery_config,
        unique_id="model.analytics.new_model",
        num_runs=1,
    )

    assert result == []


@pytest.mark.parametrize(
    "include_tests,expect_tests_in_result",
    [
        (True, True),
        (False, False),
    ],
)
async def test_fetch_performance_include_tests(
    model_performance_fetcher,
    mock_api_client,
    unit_discovery_config,
    include_tests,
    expect_tests_in_result,
):
    """Test that include_tests parameter controls whether tests are in the response."""
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "modelHistoricalRuns": [
                        {
                            "uniqueId": "model.analytics.stg_orders",
                            "runId": 12345,
                            "status": "success",
                            "executeStartedAt": "2025-12-16T10:30:00Z",
                            "executionTime": 45.67,
                            "tests": [
                                {
                                    "name": "unique_order_id",
                                    "status": "pass",
                                    "executionTime": 5.0,
                                },
                                {
                                    "name": "not_null_customer_id",
                                    "status": "fail",
                                    "executionTime": 3.2,
                                },
                            ],
                        }
                    ]
                }
            }
        }
    }

    mock_api_client.return_value = mock_response

    result = await model_performance_fetcher.fetch_performance(
        unit_discovery_config,
        unique_id="model.analytics.stg_orders",
        num_runs=1,
        include_tests=include_tests,
    )

    assert len(result) == 1
    if expect_tests_in_result:
        assert "tests" in result[0]
        assert len(result[0]["tests"]) == 2
        assert result[0]["tests"][0]["name"] == "unique_order_id"
        assert result[0]["tests"][0]["status"] == "pass"
        assert result[0]["tests"][0]["executionTime"] == 5.0
        assert result[0]["tests"][1]["name"] == "not_null_customer_id"
        assert result[0]["tests"][1]["status"] == "fail"
        assert result[0]["tests"][1]["executionTime"] == 3.2
    else:
        assert "tests" not in result[0]
