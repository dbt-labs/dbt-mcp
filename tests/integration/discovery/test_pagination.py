from dbt_mcp.config.config_providers import DiscoveryConfig
from dbt_mcp.discovery.client import ModelsFetcher, PaginatedResourceFetcher


async def test_models_fetcher_follows_native_cursor(discovery_config: DiscoveryConfig):
    fetcher = ModelsFetcher(
        paginator=PaginatedResourceFetcher(
            edges_path=("data", "environment", "applied", "models", "edges"),
            page_info_path=("data", "environment", "applied", "models", "pageInfo"),
            config=discovery_config,
        ),
        config=discovery_config,
    )
    first = await fetcher.fetch_models(limit=1)
    assert len(first.result) <= 1
    if first.pagination.has_more:
        assert first.pagination.next_cursor is not None
        second = await fetcher.fetch_models(limit=1, after=first.pagination.next_cursor)
        assert len(second.result) <= 1
        assert {m["uniqueId"] for m in first.result}.isdisjoint(
            m["uniqueId"] for m in second.result
        )
    else:
        assert first.pagination.next_cursor is None
