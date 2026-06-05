from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dbt_mcp.semantic_layer.tools import get_dimension_values
from dbt_mcp.semantic_layer.types import DimensionValuesError, DimensionValuesResponse


@pytest.fixture
def tool_context():
    config = object()
    return SimpleNamespace(
        config_provider=SimpleNamespace(get_config=AsyncMock(return_value=config)),
        semantic_layer_fetcher=SimpleNamespace(get_dimension_values=AsyncMock()),
    )


@pytest.mark.asyncio
async def test_get_dimension_values_tool_returns_success_response(tool_context):
    response = DimensionValuesResponse(values=["US", "UK"], truncated=False)
    tool_context.semantic_layer_fetcher.get_dimension_values.return_value = response

    result = await get_dimension_values.fn(
        context=tool_context,
        dimension="customer__country",
        metrics=["revenue"],
        limit=100,
    )

    assert result is response


@pytest.mark.asyncio
async def test_get_dimension_values_tool_returns_error_response(tool_context):
    response = DimensionValuesError(error="Dimension 'foo' not found")
    tool_context.semantic_layer_fetcher.get_dimension_values.return_value = response

    result = await get_dimension_values.fn(
        context=tool_context,
        dimension="foo",
        metrics=["revenue"],
        limit=100,
    )

    assert result is response
