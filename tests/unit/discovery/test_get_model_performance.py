from unittest.mock import AsyncMock, Mock

import pytest

from dbt_mcp.discovery.tools import (
    DiscoveryToolContext,
    get_model_performance as get_model_performance_tool,
)

# Access the underlying function from the ToolDefinition
get_model_performance = get_model_performance_tool.fn


@pytest.fixture
def mock_discovery_tool_context():
    """Mock DiscoveryToolContext for testing."""
    context = Mock(spec=DiscoveryToolContext)
    context.model_performance_fetcher = AsyncMock()
    return context


async def test_get_model_performance_passes_correct_parameters(
    mock_discovery_tool_context,
):
    """Test that the tool passes correct parameters to the fetcher."""
    mock_discovery_tool_context.model_performance_fetcher.fetch_performance.return_value = [
        {"runId": 12345, "executionTime": 45.67}
    ]

    await get_model_performance(
        context=mock_discovery_tool_context,
        name="stg_orders",
        unique_id="model.analytics.stg_orders",
        num_runs=10,
        include_tests=False,
    )

    # Verify fetcher was called with correct parameters
    mock_discovery_tool_context.model_performance_fetcher.fetch_performance.assert_called_once_with(
        name="stg_orders",
        unique_id="model.analytics.stg_orders",
        num_runs=10,
        include_tests=False,
    )


async def test_get_model_performance_with_tests_included(
    mock_discovery_tool_context,
):
    """Test that include_tests parameter is properly passed to the fetcher."""
    mock_discovery_tool_context.model_performance_fetcher.fetch_performance.return_value = [
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
                }
            ],
        }
    ]

    result = await get_model_performance(
        context=mock_discovery_tool_context,
        name=None,
        unique_id="model.analytics.stg_orders",
        num_runs=1,
        include_tests=True,
    )

    # Verify fetcher was called with include_tests=True
    mock_discovery_tool_context.model_performance_fetcher.fetch_performance.assert_called_once_with(
        name=None,
        unique_id="model.analytics.stg_orders",
        num_runs=1,
        include_tests=True,
    )

    # Verify tests are included in result
    assert len(result) == 1
    assert "tests" in result[0]
    assert len(result[0]["tests"]) == 1
    assert result[0]["tests"][0]["name"] == "unique_order_id"
