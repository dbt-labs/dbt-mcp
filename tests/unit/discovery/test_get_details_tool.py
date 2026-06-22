"""Tests for the consolidated get_details discovery tool."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from dbt_mcp.discovery.client import AppliedResourceType
from dbt_mcp.discovery.tools import DISCOVERY_TOOLS, get_details
from dbt_mcp.tools.tool_names import ToolName


@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.config_provider.get_config = AsyncMock(return_value=MagicMock())
    ctx.resource_details_fetcher.fetch_details = AsyncMock(
        return_value=[{"name": "orders", "uniqueId": "model.pkg.orders"}]
    )
    return ctx


@pytest.mark.parametrize(
    "resource_type",
    list(AppliedResourceType),
)
async def test_get_details_delegates_to_fetcher(
    mock_context, resource_type: AppliedResourceType
):
    """get_details should delegate to resource_details_fetcher for every resource type."""
    result = await get_details.fn(
        context=mock_context,
        resource_type=resource_type,
        name=None,
        unique_id="model.pkg.orders",
    )

    mock_context.resource_details_fetcher.fetch_details.assert_called_once_with(
        resource_type=resource_type,
        unique_id="model.pkg.orders",
        name=None,
        config=mock_context.config_provider.get_config.return_value,
    )
    assert result == [{"name": "orders", "uniqueId": "model.pkg.orders"}]


def test_get_details_in_discovery_tools():
    """get_details should be registered in DISCOVERY_TOOLS."""
    tool_names = {t.get_name() for t in DISCOVERY_TOOLS}
    assert ToolName.GET_DETAILS in tool_names


def test_old_detail_tools_removed_from_discovery_tools():
    """The 8 individual get_*_details tools should no longer be in DISCOVERY_TOOLS."""
    removed_values = {
        "get_model_details",
        "get_source_details",
        "get_exposure_details",
        "get_test_details",
        "get_seed_details",
        "get_snapshot_details",
        "get_macro_details",
        "get_semantic_model_details",
    }
    tool_name_values = {t.get_name().value for t in DISCOVERY_TOOLS}
    for value in removed_values:
        assert value not in tool_name_values, (
            f"{value!r} should have been removed from DISCOVERY_TOOLS"
        )
