import pytest

from dbt_mcp.config.config_providers import ConfigProvider, DiscoveryConfig
from dbt_mcp.discovery.client import (
    DEFAULT_PAGE_SIZE,
    ExposuresFetcher,
    ModelFilter,
    ModelsFetcher,
    SourcesFetcher,
)
from dbt_mcp.discovery.tools import DISCOVERY_TOOLS, DiscoveryToolContext
from dbt_mcp.tools.tool_names import ToolName


@pytest.mark.asyncio
async def test_fetch_models(models_fetcher: ModelsFetcher):
    results = await models_fetcher.fetch_models()

    # Basic validation of the response
    assert isinstance(results, list)
    assert len(results) > 0

    # Validate structure of returned models
    for model in results:
        assert "name" in model
        assert "uniqueId" in model
        assert "description" in model
        assert isinstance(model["name"], str)


@pytest.mark.asyncio
async def test_fetch_models_with_filter(models_fetcher: ModelsFetcher):
    # model_filter: ModelFilter = {"access": "protected"}
    model_filter: ModelFilter = {"modelingLayer": "marts"}

    # Fetch filtered results
    filtered_results = await models_fetcher.fetch_models(model_filter=model_filter)

    # Validate filtered results
    assert len(filtered_results) > 0


@pytest.mark.asyncio
async def test_fetch_model_parents(models_fetcher: ModelsFetcher):
    models = await models_fetcher.fetch_models()
    model_name = models[0]["name"]

    # Fetch filtered results
    filtered_results = await models_fetcher.fetch_model_parents(model_name)

    # Validate filtered results
    assert len(filtered_results) > 0


@pytest.mark.asyncio
async def test_fetch_model_parents_with_uniqueId(models_fetcher: ModelsFetcher):
    models = await models_fetcher.fetch_models()
    model = models[0]
    model_name = model["name"]
    unique_id = model["uniqueId"]

    # Fetch by name
    results_by_name = await models_fetcher.fetch_model_parents(model_name)

    # Fetch by uniqueId
    results_by_uniqueId = await models_fetcher.fetch_model_parents(
        model_name, unique_id
    )

    # Validate that both methods return the same result
    assert len(results_by_name) == len(results_by_uniqueId)
    if len(results_by_name) > 0:
        # Compare the first parent's name if there are any parents
        assert results_by_name[0]["name"] == results_by_uniqueId[0]["name"]


@pytest.mark.asyncio
async def test_fetch_model_children(models_fetcher: ModelsFetcher):
    models = await models_fetcher.fetch_models()
    model_name = models[0]["name"]

    # Fetch filtered results
    filtered_results = await models_fetcher.fetch_model_children(model_name)

    # Validate filtered results
    assert isinstance(filtered_results, list)


@pytest.mark.asyncio
async def test_fetch_model_children_with_uniqueId(models_fetcher: ModelsFetcher):
    models = await models_fetcher.fetch_models()
    model = models[0]
    model_name = model["name"]
    unique_id = model["uniqueId"]

    # Fetch by name
    results_by_name = await models_fetcher.fetch_model_children(model_name)

    # Fetch by uniqueId
    results_by_uniqueId = await models_fetcher.fetch_model_children(
        model_name, unique_id
    )

    # Validate that both methods return the same result
    assert len(results_by_name) == len(results_by_uniqueId)
    if len(results_by_name) > 0:
        # Compare the first child's name if there are any children
        assert results_by_name[0]["name"] == results_by_uniqueId[0]["name"]


@pytest.mark.asyncio
async def test_fetch_exposures(exposures_fetcher: ExposuresFetcher):
    results = await exposures_fetcher.fetch_exposures()

    # Basic validation of the response
    assert isinstance(results, list)

    # If there are exposures, validate their structure
    if len(results) > 0:
        for exposure in results:
            assert "name" in exposure
            assert "uniqueId" in exposure
            assert isinstance(exposure["name"], str)
            assert isinstance(exposure["uniqueId"], str)


@pytest.mark.asyncio
async def test_fetch_exposures_pagination(exposures_fetcher: ExposuresFetcher):
    # Test that pagination works correctly by fetching all exposures
    # This test ensures the pagination logic handles multiple pages properly
    results = await exposures_fetcher.fetch_exposures()

    # Validate that we get results (assuming the test environment has some exposures)
    assert isinstance(results, list)

    # If we have more than the page size, ensure no duplicates
    if len(results) > DEFAULT_PAGE_SIZE:
        unique_ids = set()
        for exposure in results:
            unique_id = exposure["uniqueId"]
            assert unique_id not in unique_ids, f"Duplicate exposure found: {unique_id}"
            unique_ids.add(unique_id)


@pytest.mark.asyncio
async def test_fetch_sources(sources_fetcher: SourcesFetcher):
    """Test basic sources fetching functionality."""
    results = await sources_fetcher.fetch_sources()

    # Basic validation of the response
    assert isinstance(results, list)

    # If sources exist, validate their structure
    if len(results) > 0:
        for source in results:
            assert "name" in source
            assert "uniqueId" in source
            assert "sourceName" in source
            assert "resourceType" in source
            assert source["resourceType"] == "source"

            # Validate types
            assert isinstance(source["name"], str)
            assert isinstance(source["uniqueId"], str)
            assert isinstance(source["sourceName"], str)

            # Check for description (may be None)
            assert "description" in source

            # Validate freshness data if present
            if source.get("freshness"):
                freshness = source["freshness"]
                assert isinstance(freshness, dict)


@pytest.mark.asyncio
async def test_fetch_sources_with_filter(sources_fetcher: SourcesFetcher):
    """Test sources fetching with filter."""
    # First get all sources to find a valid source name
    all_sources = await sources_fetcher.fetch_sources()

    if len(all_sources) > 0:
        # Pick the first source name for filtering
        source_name = all_sources[0]["sourceName"]

        # Test filtering by source name
        filtered_results = await sources_fetcher.fetch_sources(
            source_names=[source_name]
        )

        # Validate filtered results
        assert isinstance(filtered_results, list)

        # All results should have the specified source name
        for source in filtered_results:
            assert source["sourceName"] == source_name


@pytest.mark.asyncio
async def test_get_all_sources_tool(
    config_provider: ConfigProvider[DiscoveryConfig],
) -> None:
    """Test the get_all_sources tool function integration."""

    # Create tool definitions
    tool_definitions = DISCOVERY_TOOLS

    # Find the get_all_sources tool
    get_all_sources_tool = None
    for tool_def in tool_definitions:
        if tool_def.get_name() == ToolName.GET_ALL_SOURCES:
            get_all_sources_tool = tool_def
            break

    assert get_all_sources_tool is not None, (
        "get_all_sources tool not found in tool definitions"
    )

    # Execute the tool function
    result = await get_all_sources_tool.fn(
        context=DiscoveryToolContext(config_provider=config_provider)
    )

    # Validate the result
    assert isinstance(result, list)

    # If sources exist, validate structure
    if len(result) > 0:
        for source in result:
            assert "name" in source
            assert "uniqueId" in source
            assert "sourceName" in source
            assert "resourceType" in source
            assert source["resourceType"] == "source"
