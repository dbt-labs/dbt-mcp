from unittest.mock import AsyncMock, Mock

import pytest

from dbt_mcp.discovery.client import AppliedResourceType
from dbt_mcp.discovery.tools import (
    DISCOVERY_TOOLS,
    get_details,
)
from dbt_mcp.discovery.tools_multiproject import (
    MULTIPROJECT_DISCOVERY_TOOLS,
)
from dbt_mcp.discovery.tools_multiproject import (
    get_details as get_details_multiproject,
)
from dbt_mcp.tools.tool_names import ToolName

DEPRECATED_DETAIL_TOOLS = [
    ToolName.GET_MODEL_DETAILS,
    ToolName.GET_SOURCE_DETAILS,
    ToolName.GET_EXPOSURE_DETAILS,
    ToolName.GET_TEST_DETAILS,
    ToolName.GET_SEED_DETAILS,
    ToolName.GET_SNAPSHOT_DETAILS,
    ToolName.GET_MACRO_DETAILS,
    ToolName.GET_SEMANTIC_MODEL_DETAILS,
]


@pytest.mark.parametrize("resource_type", list(AppliedResourceType))
async def test_get_details_delegates_to_fetcher(resource_type: AppliedResourceType):
    config = object()
    context = Mock()
    context.config_provider.get_config = AsyncMock(return_value=config)
    context.resource_details_fetcher.fetch_details = AsyncMock(return_value=["row"])

    result = await get_details.fn(
        context=context,
        resource_type=resource_type,
        name=None,
        unique_id=f"{resource_type.value}.pkg.thing",
    )

    assert result == ["row"]
    context.resource_details_fetcher.fetch_details.assert_awaited_once_with(
        resource_type=resource_type,
        unique_id=f"{resource_type.value}.pkg.thing",
        name=None,
        config=config,
    )


@pytest.mark.parametrize("resource_type", list(AppliedResourceType))
async def test_get_details_multiproject_delegates_to_fetcher(
    resource_type: AppliedResourceType,
):
    config = object()
    context = Mock()
    context.config_provider.get_config = AsyncMock(return_value=config)
    context.resource_details_fetcher.fetch_details = AsyncMock(return_value=["row"])

    result = await get_details_multiproject.fn(
        context=context,
        project_id=42,
        resource_type=resource_type,
        name="thing",
        unique_id=None,
    )

    assert result == ["row"]
    context.config_provider.get_config.assert_awaited_once_with(project_id=42)
    context.resource_details_fetcher.fetch_details.assert_awaited_once_with(
        resource_type=resource_type,
        unique_id=None,
        name="thing",
        config=config,
    )


@pytest.mark.parametrize("tool_name", DEPRECATED_DETAIL_TOOLS)
def test_detail_tools_are_deprecated(tool_name: ToolName):
    tool = next(t for t in DISCOVERY_TOOLS if t.get_name() == tool_name)
    assert tool.meta is not None
    assert tool.meta["deprecated"] is True
    assert tool.meta["replacement"] == "get_details"
    assert tool.description.startswith("**DEPRECATED")
    # A short, blunt description (not the original prompt) speeds the soak.
    assert len(tool.description) < 200


@pytest.mark.parametrize("tool_name", DEPRECATED_DETAIL_TOOLS)
def test_detail_tools_are_deprecated_multiproject(tool_name: ToolName):
    tool = next(t for t in MULTIPROJECT_DISCOVERY_TOOLS if t.get_name() == tool_name)
    assert tool.meta is not None
    assert tool.meta["deprecated"] is True
    assert tool.meta["replacement"] == "get_details"
    assert tool.description.startswith("**DEPRECATED")
    assert len(tool.description) < 200


def test_get_details_not_deprecated():
    assert get_details.meta is None
    assert get_details_multiproject.meta is None
