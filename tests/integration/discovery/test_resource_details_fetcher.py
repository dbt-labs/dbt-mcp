import pytest

from dbt_mcp.config.config_providers import DefaultDiscoveryConfigProvider
from dbt_mcp.config.settings import CredentialsProvider, DbtMcpSettings
from dbt_mcp.discovery.client import (
    AppliedResourceType,
    MetadataAPIClient,
    ResourceDetailsFetcher,
)

RESOURCE_CASES: list[tuple[AppliedResourceType, str, str, bool]] = [
    (
        AppliedResourceType.MODEL,
        "model.jaffle_semantic_layer_testing.orders",
        "orders",
        True,
    ),
    (
        AppliedResourceType.SOURCE,
        "source.jaffle_semantic_layer_testing.raw_customers",
        "raw_customers",
        False,
    ),
    (
        AppliedResourceType.EXPOSURE,
        "exposure.jaffle_semantic_layer_testing.customer_dashboard",
        "customer_dashboard",
        False,
    ),
    (
        AppliedResourceType.TEST,
        "test.jaffle_semantic_layer_testing.not_null_orders_order_id",
        "not_null_orders_order_id",
        False,
    ),
    (
        AppliedResourceType.SEED,
        "seed.jaffle_semantic_layer_testing.raw_customers",
        "raw_customers",
        True,
    ),
    (
        AppliedResourceType.SNAPSHOT,
        "snapshot.jaffle_semantic_layer_testing.snapshot_orders",
        "snapshot_orders",
        False,
    ),
    (
        AppliedResourceType.MACRO,
        "macro.jaffle_semantic_layer_testing.cents_to_dollars",
        "cents_to_dollars",
        True,
    ),
    (
        AppliedResourceType.SEMANTIC_MODEL,
        "semantic_model.jaffle_semantic_layer_testing.stg_customers",
        "stg_customers",
        True,
    ),
]


@pytest.fixture
def resource_details_fetcher() -> ResourceDetailsFetcher:
    settings = DbtMcpSettings()  # type: ignore
    credentials_provider = CredentialsProvider(settings)
    config_provider = DefaultDiscoveryConfigProvider(credentials_provider)
    return ResourceDetailsFetcher(
        api_client=MetadataAPIClient(config_provider=config_provider)
    )


async def test_get_resource_details_resource_type_test_cases():
    assert {c[0].value for c in RESOURCE_CASES} == set(AppliedResourceType)


@pytest.mark.parametrize("resource_type, unique_id, name, results", RESOURCE_CASES)
async def test_resource_details_fetcher_accepts_unique_id(
    resource_details_fetcher: ResourceDetailsFetcher,
    resource_type: AppliedResourceType,
    unique_id: str,
    name: str,
    results: bool,
) -> None:
    result = await resource_details_fetcher.fetch_details(
        resource_type=resource_type,
        unique_id=unique_id,
        name=None,
    )
    if results:
        assert len(result) == 1
        assert result[0]["uniqueId"] == unique_id
    else:
        assert result == []


@pytest.mark.parametrize("resource_type, unique_id, name, results", RESOURCE_CASES)
async def test_resource_details_fetcher_accepts_name(
    resource_details_fetcher: ResourceDetailsFetcher,
    resource_type: AppliedResourceType,
    unique_id: str,
    name: str,
    results: bool,
) -> None:
    result = await resource_details_fetcher.fetch_details(
        resource_type=resource_type,
        unique_id=None,
        name=name,
    )
    if results:
        assert len(result) == 1
        assert result[0]["name"] == name
    else:
        assert result == []


async def test_resource_details_fetcher_non_existent_unique_id(
    resource_details_fetcher: ResourceDetailsFetcher,
) -> None:
    result = await resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.MODEL,
        unique_id="model.nonexistent.resource",
        name=None,
    )
    assert result == []
