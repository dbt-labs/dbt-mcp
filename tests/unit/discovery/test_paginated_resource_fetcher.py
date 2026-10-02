import pytest

from dbt_mcp.discovery.client import PaginatedResourceFetcher
from dbt_mcp.errors import DiscoveryToolCallError, InvalidParameterError


def paginator():
    return PaginatedResourceFetcher(
        edges_path=("data", "environment", "applied", "models", "edges"),
        page_info_path=("data", "environment", "applied", "models", "pageInfo"),
    )


def response(nodes, *, has_more=False, cursor=None):
    return {
        "data": {
            "environment": {
                "applied": {
                    "models": {
                        "edges": [{"node": node} for node in nodes],
                        "pageInfo": {"hasNextPage": has_more, "endCursor": cursor},
                    }
                }
            }
        }
    }


async def test_one_page_preserves_native_cursor_and_order(
    mock_api_client, unit_discovery_config
):
    mock_api_client.return_value = response(
        [{"id": 2}, {"id": 1}], has_more=True, cursor="next"
    )
    page = await paginator().fetch_paginated(
        "query", {}, config=unit_discovery_config, limit=2, after="previous"
    )
    assert page.result == [{"id": 2}, {"id": 1}]
    assert page.pagination.has_more
    assert page.pagination.next_cursor == "next"
    mock_api_client.assert_awaited_once_with(
        "query",
        {
            "environmentId": unit_discovery_config.environment_id,
            "first": 2,
            "after": "previous",
        },
        config=unit_discovery_config,
    )


async def test_empty_terminal_page(mock_api_client, unit_discovery_config):
    mock_api_client.return_value = response([])
    page = await paginator().fetch_paginated("query", {}, config=unit_discovery_config)
    assert page.result == []
    assert not page.pagination.has_more
    assert page.pagination.next_cursor is None


@pytest.mark.parametrize("cursor", [None, "previous"])
async def test_non_advancing_cursor_is_actionable(
    mock_api_client, unit_discovery_config, cursor
):
    mock_api_client.return_value = response([{"id": 1}], has_more=True, cursor=cursor)
    with pytest.raises(DiscoveryToolCallError, match="did not advance"):
        await paginator().fetch_paginated(
            "query", {}, config=unit_discovery_config, after="previous"
        )


async def test_upstream_excess_nodes_is_a_server_error(
    mock_api_client, unit_discovery_config
):
    mock_api_client.return_value = response([{"id": 1}, {"id": 2}])
    with pytest.raises(DiscoveryToolCallError, match="more nodes than requested"):
        await paginator().fetch_paginated(
            "query", {}, config=unit_discovery_config, limit=1
        )


@pytest.mark.parametrize("limit", [0, -1, 101, None])
async def test_invalid_page_size_does_not_request_data(
    mock_api_client, unit_discovery_config, limit
):
    with pytest.raises(InvalidParameterError, match="limit"):
        await paginator().fetch_paginated(
            "query", {}, config=unit_discovery_config, limit=limit
        )
    mock_api_client.assert_not_awaited()


async def test_wide_page_is_rejected_without_skipping_rows(
    mock_api_client, unit_discovery_config
):
    mock_api_client.return_value = response(
        [{"description": "x" * 512001}], has_more=True, cursor="next"
    )
    with pytest.raises(InvalidParameterError, match="reduce the page size"):
        await paginator().fetch_paginated("query", {}, config=unit_discovery_config)
