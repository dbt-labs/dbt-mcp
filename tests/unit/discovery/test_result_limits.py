import pytest

from dbt_mcp.discovery.client import ModelsFetcher, PaginatedResourceFetcher
from dbt_mcp.errors import InvalidParameterError


async def test_wide_page_requires_a_smaller_result(
    mock_api_client, unit_discovery_config
):
    mock_api_client.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "models": {
                        "edges": [{"node": {"description": "x" * 512001}}],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        }
    }
    fetcher = ModelsFetcher(
        paginator=PaginatedResourceFetcher(
            edges_path=("data", "environment", "applied", "models", "edges"),
            page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        )
    )
    with pytest.raises(InvalidParameterError, match="reduce the page size"):
        await fetcher.fetch_models(config=unit_discovery_config)
