import copy
from typing import Any

import pytest

from dbt_mcp.config.config_providers import (
    ConfigProvider,
    DefaultDiscoveryConfigProvider,
    DiscoveryConfig,
)
from dbt_mcp.config.settings import CredentialsProvider, DbtMcpSettings
from dbt_mcp.discovery.client import (
    MetadataAPIClient,
    ModelsFetcher,
    PaginatedResourceFetcher,
)


class CountingMetadataAPIClient(MetadataAPIClient):
    def __init__(self, config_provider: ConfigProvider[DiscoveryConfig]) -> None:
        super().__init__(config_provider)
        self.request_calls = 0
        self.request_payloads: list[dict] = []

    async def execute_query(
        self, query: str, variables: dict[str, Any]
    ) -> dict[str, Any]:
        self.request_calls += 1
        self.request_payloads.append(
            copy.deepcopy({"query": query, "variables": variables})
        )
        return await super().execute_query(query, variables)


@pytest.fixture
def api_client() -> CountingMetadataAPIClient:
    settings = DbtMcpSettings()  # type: ignore
    credentials_provider = CredentialsProvider(settings)
    config_provider = DefaultDiscoveryConfigProvider(credentials_provider)
    return CountingMetadataAPIClient(config_provider)


async def test_models_fetcher_paginates_without_has_next_page(
    api_client: CountingMetadataAPIClient,
):
    paginator = PaginatedResourceFetcher(
        api_client,
        edges_path=("data", "environment", "applied", "models", "edges"),
        page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        page_size=1,
        max_node_query_limit=5,
    )
    models_fetcher = ModelsFetcher(api_client, paginator=paginator)

    results = await models_fetcher.fetch_models()

    assert isinstance(results, list)
    assert api_client.request_calls == len(api_client.request_payloads) > 0

    first_request = api_client.request_payloads[0]["variables"]
    assert first_request["first"] == 1
    assert "after" not in first_request

    if len(results) <= 1 or api_client.request_calls <= 1:
        raise Exception("Not enough models returned to exercise pagination")

    second_request = api_client.request_payloads[1]["variables"]
    assert second_request["first"] == 1
    assert isinstance(second_request.get("after"), str)


async def test_models_fetcher_paginates_until_has_next_false(
    api_client: CountingMetadataAPIClient,
):
    paginator = PaginatedResourceFetcher(
        api_client,
        edges_path=("data", "environment", "applied", "models", "edges"),
        page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        page_size=1,
        max_node_query_limit=5,
    )
    models_fetcher = ModelsFetcher(api_client, paginator=paginator)

    results = await models_fetcher.fetch_models()

    assert isinstance(results, list)
    assert api_client.request_calls == len(api_client.request_payloads) > 0

    first_request = api_client.request_payloads[0]["variables"]
    assert first_request["first"] == 1
    assert "after" not in first_request

    if len(results) <= 1 or api_client.request_calls <= 1:
        raise Exception("Not enough models returned to exercise pagination")

    second_request = api_client.request_payloads[1]["variables"]
    assert second_request["first"] == 1
    assert isinstance(second_request.get("after"), str)
