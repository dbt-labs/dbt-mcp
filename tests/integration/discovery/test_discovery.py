import os

import pytest

from dbt_mcp.config.config_providers import DefaultDiscoveryConfigProvider
from dbt_mcp.config.settings import CredentialsProvider, DbtMcpSettings
from dbt_mcp.discovery.client import (
    ExposuresFetcher,
    MetadataAPIClient,
    ModelFilter,
    ModelsFetcher,
    SourcesFetcher,
)


@pytest.fixture
def api_client() -> MetadataAPIClient:
    # Set up environment variables needed by DbtMcpSettings
    host = os.getenv("DBT_HOST")
    token = os.getenv("DBT_TOKEN")
    prod_env_id = os.getenv("DBT_PROD_ENV_ID")

    if not host or not token or not prod_env_id:
        raise ValueError(
            "DBT_HOST, DBT_TOKEN, and DBT_PROD_ENV_ID environment variables are required"
        )

    # Create settings and credentials provider
    # DbtMcpSettings will automatically pick up from environment variables
    settings = DbtMcpSettings()  # type: ignore
    credentials_provider = CredentialsProvider(settings)
    config_provider = DefaultDiscoveryConfigProvider(credentials_provider)

    return MetadataAPIClient(config_provider)


@pytest.fixture
def models_fetcher(api_client: MetadataAPIClient) -> ModelsFetcher:
    return ModelsFetcher(api_client)


@pytest.fixture
def exposures_fetcher(api_client: MetadataAPIClient) -> ExposuresFetcher:
    return ExposuresFetcher(api_client)


@pytest.fixture
def sources_fetcher(api_client: MetadataAPIClient) -> SourcesFetcher:
    return SourcesFetcher(api_client)


@pytest.mark.asyncio
async def test_fetch_models(models_fetcher: ModelsFetcher):
    results = await models_fetcher.fetch_models()

    # Basic validation of the response
    assert isinstance(results, list)
    assert len(results) > 0

    # Validate structure of returned models
    for model in results:
        assert "name" in model
        assert "compiledCode" in model
        assert isinstance(model["name"], str)

        # If catalog exists, validate its structure
        if model.get("catalog"):
            assert isinstance(model["catalog"], dict)
            if "columns" in model["catalog"]:
                for column in model["catalog"]["columns"]:
                    assert "name" in column
                    assert "type" in column


@pytest.mark.asyncio
async def test_fetch_models_with_filter(models_fetcher: ModelsFetcher):
    # model_filter: ModelFilter = {"access": "protected"}
    model_filter: ModelFilter = {"modelingLayer": "marts"}

    # Fetch filtered results
    filtered_results = await models_fetcher.fetch_models(model_filter=model_filter)

    # Validate filtered results
    assert len(filtered_results) > 0


@pytest.mark.asyncio
async def test_fetch_model_details(models_fetcher: ModelsFetcher):
    models = await models_fetcher.fetch_models()
    model_name = models[0]["name"]

    # Fetch filtered results
    filtered_results = await models_fetcher.fetch_model_details(model_name)

    # Validate filtered results
    assert len(filtered_results) > 0


@pytest.mark.asyncio
async def test_fetch_model_details_with_uniqueId(models_fetcher: ModelsFetcher):
    models = await models_fetcher.fetch_models()
    model = models[0]
    model_name = model["name"]
    unique_id = model["uniqueId"]

    # Fetch by name
    results_by_name = await models_fetcher.fetch_model_details(model_name)

    # Fetch by uniqueId
    results_by_uniqueId = await models_fetcher.fetch_model_details(
        model_name, unique_id
    )

    # Validate that both methods return the same result
    assert results_by_name["uniqueId"] == results_by_uniqueId["uniqueId"]
    assert results_by_name["name"] == results_by_uniqueId["name"]


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
    if len(results) > 100:  # PAGE_SIZE is 100
        unique_ids = set()
        for exposure in results:
            unique_id = exposure["uniqueId"]
            assert unique_id not in unique_ids, f"Duplicate exposure found: {unique_id}"
            unique_ids.add(unique_id)


@pytest.mark.asyncio
async def test_fetch_exposure_details_by_unique_ids(
    exposures_fetcher: ExposuresFetcher,
):
    # First get all exposures to find one to test with
    exposures = await exposures_fetcher.fetch_exposures()

    # Skip test if no exposures are available
    if not exposures:
        pytest.skip("No exposures available in the test environment")

    # Pick the first exposure to test with
    test_exposure = exposures[0]
    unique_id = test_exposure["uniqueId"]

    # Fetch the same exposure by unique_ids
    result = await exposures_fetcher.fetch_exposure_details(unique_ids=[unique_id])

    # Validate that we got the correct exposure back
    assert isinstance(result, list)
    assert len(result) == 1
    exposure = result[0]
    assert exposure["uniqueId"] == unique_id
    assert exposure["name"] == test_exposure["name"]
    assert "exposureType" in exposure
    assert "maturity" in exposure

    # Validate structure
    if exposure.get("parents"):
        assert isinstance(exposure["parents"], list)
        for parent in exposure["parents"]:
            assert "uniqueId" in parent


@pytest.mark.asyncio
async def test_fetch_exposure_details_nonexistent(exposures_fetcher: ExposuresFetcher):
    # Test with a non-existent exposure
    result = await exposures_fetcher.fetch_exposure_details(
        unique_ids=["exposure.nonexistent.exposure"]
    )

    # Should return empty list when not found
    assert result == []


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
            if "freshness" in source and source["freshness"]:
                freshness = source["freshness"]
                assert isinstance(freshness, dict)
                # These fields may be present depending on configuration
                if "freshnessStatus" in freshness:
                    assert isinstance(freshness["freshnessStatus"], str)
                if "maxLoadedAt" in freshness:
                    assert freshness["maxLoadedAt"] is None or isinstance(
                        freshness["maxLoadedAt"], str
                    )
                if "maxLoadedAtTimeAgoInS" in freshness:
                    assert freshness["maxLoadedAtTimeAgoInS"] is None or isinstance(
                        freshness["maxLoadedAtTimeAgoInS"], int
                    )


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
async def test_get_all_sources_tool():
    """Test the get_all_sources tool function integration."""
    from dbt_mcp.config.config_providers import DefaultDiscoveryConfigProvider
    from dbt_mcp.config.settings import CredentialsProvider, DbtMcpSettings
    from dbt_mcp.discovery.tools import create_discovery_tool_definitions

    # Set up environment variables needed by DbtMcpSettings
    host = os.getenv("DBT_HOST")
    token = os.getenv("DBT_TOKEN")
    prod_env_id = os.getenv("DBT_PROD_ENV_ID")

    if not host or not token or not prod_env_id:
        pytest.skip(
            "DBT_HOST, DBT_TOKEN, and DBT_PROD_ENV_ID environment variables are required"
        )

    # Create settings and config provider
    settings = DbtMcpSettings()  # type: ignore
    credentials_provider = CredentialsProvider(settings)
    config_provider = DefaultDiscoveryConfigProvider(credentials_provider)

    # Create tool definitions
    tool_definitions = create_discovery_tool_definitions(config_provider)

    # Find the get_all_sources tool
    get_all_sources_tool = None
    for tool_def in tool_definitions:
        if tool_def.get_name() == "get_all_sources":
            get_all_sources_tool = tool_def
            break

    assert get_all_sources_tool is not None, (
        "get_all_sources tool not found in tool definitions"
    )

    # Execute the tool function
    result = await get_all_sources_tool.fn()

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
