import pytest

from dbt_mcp.config.config_providers import DiscoveryConfig
from dbt_mcp.discovery.client import (
    AppliedResourceType,
    ModelsFetcher,
    ResourceDetailsFetcher,
    SourcesFetcher,
)


@pytest.fixture
def resource_details_fetcher() -> ResourceDetailsFetcher:
    return ResourceDetailsFetcher()


async def test_resource_details_fetcher_accepts_unique_id_for_model(
    models_fetcher: ModelsFetcher,
    resource_details_fetcher: ResourceDetailsFetcher,
    discovery_config: DiscoveryConfig,
) -> None:
    models = await models_fetcher.fetch_models(config=discovery_config)
    assert len(models) > 0
    model = models[0]
    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.MODEL,
        discovery_config,
        unique_id=model["uniqueId"],
        name=None,
    )
    assert len(result) == 1
    assert result[0]["name"] == model["name"]
    assert result[0]["uniqueId"] == model["uniqueId"]


async def test_resource_details_fetcher_accepts_name_for_model(
    models_fetcher: ModelsFetcher,
    resource_details_fetcher: ResourceDetailsFetcher,
    discovery_config: DiscoveryConfig,
) -> None:
    models = await models_fetcher.fetch_models(config=discovery_config)
    assert len(models) > 0
    model = models[0]
    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.MODEL,
        discovery_config,
        unique_id=None,
        name=model["name"],
    )
    assert len(result) == 1
    assert result[0]["name"] == model["name"]
    assert result[0]["uniqueId"] == model["uniqueId"]


async def test_resource_details_fetcher_accepts_unique_id_for_source(
    sources_fetcher: SourcesFetcher,
    resource_details_fetcher: ResourceDetailsFetcher,
    discovery_config: DiscoveryConfig,
) -> None:
    sources = await sources_fetcher.fetch_sources(config=discovery_config)
    assert len(sources) > 0
    source = sources[0]
    unique_id = source["uniqueId"]
    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.SOURCE,
        discovery_config,
        unique_id=unique_id,
        name=None,
    )
    assert len(result) == 1
    assert result[0]["name"] == source["name"]
    assert result[0]["uniqueId"] == unique_id


@pytest.mark.skip(
    reason="unique_id construction for sources "
    "needs to follow this pattern: source.<package_name>.<source_name>.<table_name>"
)
async def test_resource_details_fetcher_accepts_name_for_source(
    sources_fetcher: SourcesFetcher,
    resource_details_fetcher: ResourceDetailsFetcher,
    discovery_config: DiscoveryConfig,
) -> None:
    sources = await sources_fetcher.fetch_sources(config=discovery_config)
    assert len(sources) > 0
    source = sources[0]
    name = source["name"]
    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.SOURCE,
        discovery_config,
        unique_id=None,
        name=name,
    )
    assert len(result) == 1
    assert result[0]["name"] == name
    assert result[0]["uniqueId"] == source["uniqueId"]


async def test_resource_details_fetcher_non_existent_unique_id(
    resource_details_fetcher: ResourceDetailsFetcher,
    discovery_config: DiscoveryConfig,
) -> None:
    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.MODEL,
        discovery_config,
        unique_id="model.nonexistent.resource",
        name=None,
    )
    assert result == []
