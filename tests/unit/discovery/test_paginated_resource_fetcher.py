from collections.abc import Iterable
from typing import Any

import pytest

from dbt_mcp.discovery.client import PaginatedResourceFetcher


def _make_page(
    nodes: Iterable[dict[str, Any]],
    *,
    has_next: bool | None,
    end_cursor: str | None,
) -> dict[str, Any]:
    return {
        "data": {
            "environment": {
                "applied": {
                    "models": {
                        "edges": [{"node": node} for node in nodes],
                        "pageInfo": {
                            "hasNextPage": has_next,
                            "endCursor": end_cursor,
                        },
                    }
                }
            }
        }
    }


def _build_paginator(
    mock_api_client, *, page_size: int, max_limit: int
) -> PaginatedResourceFetcher:
    return PaginatedResourceFetcher(
        mock_api_client,
        edges_path=("data", "environment", "applied", "models", "edges"),
        page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        page_size=page_size,
        max_node_query_limit=max_limit,
    )


@pytest.mark.asyncio
async def test_fetch_paginated_stops_at_max_limit(mock_api_client):
    paginator = _build_paginator(mock_api_client, page_size=1, max_limit=2)

    mock_api_client.execute_query.side_effect = [
        _make_page([{"id": 1}], has_next=True, end_cursor="cursor-1"),
        _make_page([{"id": 2}], has_next=True, end_cursor="cursor-2"),
        _make_page([{"id": 3}], has_next=False, end_cursor="cursor-3"),
    ]

    result = await paginator.fetch_paginated("GetModels", variables={})

    assert [node["id"] for node in result] == [1, 2]
    assert mock_api_client.execute_query.await_count == 2


@pytest.mark.asyncio
async def test_fetch_paginated_stops_when_cursor_repeats(mock_api_client):
    paginator = _build_paginator(mock_api_client, page_size=1, max_limit=5)

    mock_api_client.execute_query.side_effect = [
        _make_page([{"id": 1}], has_next=True, end_cursor="cursor-repeat"),
        _make_page([{"id": 2}], has_next=True, end_cursor="cursor-repeat"),
        _make_page([{"id": 3}], has_next=True, end_cursor="cursor-final"),
    ]

    result = await paginator.fetch_paginated("GetModels", variables={})

    assert [node["id"] for node in result] == [1, 2]
    assert mock_api_client.execute_query.await_count == 2


@pytest.mark.asyncio
async def test_fetch_paginated_handles_partial_final_page(mock_api_client):
    paginator = _build_paginator(mock_api_client, page_size=2, max_limit=10)

    mock_api_client.execute_query.side_effect = [
        _make_page([{"id": 1}, {"id": 2}], has_next=True, end_cursor="cursor-1"),
        _make_page([{"id": 3}], has_next=False, end_cursor="cursor-2"),
    ]

    result = await paginator.fetch_paginated("GetModels", variables={})

    assert [node["id"] for node in result] == [1, 2, 3]
    assert mock_api_client.execute_query.await_count == 2

    first_call_variables = mock_api_client.execute_query.await_args_list[0][0][1]
    second_call_variables = mock_api_client.execute_query.await_args_list[1][0][1]
    assert "after" not in first_call_variables
    assert second_call_variables["after"] == "cursor-1"


@pytest.mark.asyncio
async def test_fetch_paginated_empty_edges(mock_api_client):
    paginator = _build_paginator(mock_api_client, page_size=5, max_limit=10)

    mock_api_client.execute_query.return_value = _make_page(
        [], has_next=False, end_cursor=None
    )

    result = await paginator.fetch_paginated("GetModels", variables={})

    assert result == []
    mock_api_client.execute_query.assert_awaited_once()
