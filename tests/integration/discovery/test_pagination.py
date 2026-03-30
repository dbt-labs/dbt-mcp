import copy
from typing import Any
from unittest.mock import patch


from dbt_mcp.config.config_providers import (
    DiscoveryConfig,
)
from dbt_mcp.discovery import client as discovery_client
from dbt_mcp.discovery.client import (
    ModelsFetcher,
    PaginatedResourceFetcher,
    execute_query as real_execute_query,
)


class CountingExecuteQuery:
    """Wraps the module-level execute_query to count calls and capture payloads."""

    def __init__(self) -> None:
        self.request_calls = 0
        self.request_payloads: list[dict] = []

    async def __call__(
        self,
        query: str,
        variables: dict[str, Any],
        *,
        config: DiscoveryConfig,
    ) -> dict[str, Any]:
        self.request_calls += 1
        self.request_payloads.append(
            copy.deepcopy({"query": query, "variables": variables})
        )
        return await real_execute_query(query, variables, config=config)


async def test_models_fetcher_paginates_without_has_next_page(
    discovery_config: DiscoveryConfig,
):
    counter = CountingExecuteQuery()
    paginator = PaginatedResourceFetcher(
        edges_path=("data", "environment", "applied", "models", "edges"),
        page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        page_size=1,
        max_node_query_limit=5,
    )
    models_fetcher = ModelsFetcher(paginator=paginator)

    with patch.object(discovery_client, "execute_query", counter):
        results = await models_fetcher.fetch_models(config=discovery_config)

    assert isinstance(results, list)
    assert counter.request_calls == len(counter.request_payloads) > 0

    first_request = counter.request_payloads[0]["variables"]
    assert first_request["first"] == 1
    assert "after" not in first_request

    if len(results) <= 1 or counter.request_calls <= 1:
        raise Exception("Not enough models returned to exercise pagination")

    second_request = counter.request_payloads[1]["variables"]
    assert second_request["first"] == 1
    assert isinstance(second_request.get("after"), str)


async def test_models_fetcher_paginates_until_has_next_false(
    discovery_config: DiscoveryConfig,
):
    counter = CountingExecuteQuery()
    paginator = PaginatedResourceFetcher(
        edges_path=("data", "environment", "applied", "models", "edges"),
        page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        page_size=1,
        max_node_query_limit=5,
    )
    models_fetcher = ModelsFetcher(paginator=paginator)

    with patch.object(discovery_client, "execute_query", counter):
        results = await models_fetcher.fetch_models(config=discovery_config)

    assert isinstance(results, list)
    assert counter.request_calls == len(counter.request_payloads) > 0

    first_request = counter.request_payloads[0]["variables"]
    assert first_request["first"] == 1
    assert "after" not in first_request

    if len(results) <= 1 or counter.request_calls <= 1:
        raise Exception("Not enough models returned to exercise pagination")

    second_request = counter.request_payloads[1]["variables"]
    assert second_request["first"] == 1
    assert isinstance(second_request.get("after"), str)
