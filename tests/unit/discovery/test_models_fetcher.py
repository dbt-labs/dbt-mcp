from unittest.mock import Mock

import pytest

from dbt_mcp.discovery.client import ModelsFetcher, PaginatedResourceFetcher


@pytest.fixture
def models_fetcher():
    return ModelsFetcher(paginator=Mock())


def _models_page(nodes, *, has_next, end_cursor):
    return {
        "data": {
            "environment": {
                "applied": {
                    "models": {
                        "edges": [{"node": node} for node in nodes],
                        "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
                    }
                }
            }
        }
    }


@pytest.fixture
def paginated_models_fetcher():
    """ModelsFetcher backed by a real PaginatedResourceFetcher, so pagination
    is genuinely exercised rather than mocked away."""
    paginator = PaginatedResourceFetcher(
        edges_path=("data", "environment", "applied", "models", "edges"),
        page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        page_size=1,
        max_node_query_limit=10000,
    )
    return ModelsFetcher(paginator=paginator)


async def test_fetch_model_health_wraps_single_node_in_list(
    models_fetcher, mock_api_client, unit_discovery_config
):
    """fetch_model_health must return a list[dict], not a bare dict."""
    node = {
        "uniqueId": "model.project.my_model",
        "executionInfo": {"lastSuccessJobDefinitionId": 1, "lastRunStatus": "success"},
        "tests": [{"name": "not_null", "status": "pass"}],
        "ancestors": [],
    }
    mock_api_client.return_value = {
        "data": {"environment": {"applied": {"models": {"edges": [{"node": node}]}}}}
    }

    result = await models_fetcher.fetch_model_health(
        unique_id="model.project.my_model", config=unit_discovery_config
    )

    assert isinstance(result, list), (
        "fetch_model_health must return a list, not a bare dict"
    )
    assert len(result) == 1
    assert result[0] == node


async def test_fetch_model_health_empty_edges_returns_empty_list(
    models_fetcher, mock_api_client, unit_discovery_config
):
    """fetch_model_health returns [] when no model is found."""
    mock_api_client.return_value = {
        "data": {"environment": {"applied": {"models": {"edges": []}}}}
    }

    result = await models_fetcher.fetch_model_health(
        unique_id="model.project.nonexistent", config=unit_discovery_config
    )

    assert result == []


async def test_resolve_unique_ids_by_name_single_match(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    mock_api_client.side_effect = [
        _models_page(
            [{"name": "orders", "uniqueId": "model.jaffle.orders"}],
            has_next=False,
            end_cursor=None,
        )
    ]

    result = await paginated_models_fetcher.resolve_unique_ids_by_name(
        "orders", config=unit_discovery_config
    )

    assert result == ["model.jaffle.orders"]
    query, variables = mock_api_client.call_args[0]
    assert variables["modelsFilter"] == {"identifier": "orders"}


async def test_resolve_unique_ids_by_name_multi_match(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    """Multiple models can share an identifier/alias across packages; all
    real matches (same name) flow through."""
    mock_api_client.side_effect = [
        _models_page(
            [{"name": "orders", "uniqueId": "model.jaffle.orders"}],
            has_next=True,
            end_cursor="cursor-1",
        ),
        _models_page(
            [{"name": "orders", "uniqueId": "model.other_pkg.orders"}],
            has_next=False,
            end_cursor="cursor-2",
        ),
    ]

    result = await paginated_models_fetcher.resolve_unique_ids_by_name(
        "orders", config=unit_discovery_config
    )

    assert result == ["model.jaffle.orders", "model.other_pkg.orders"]
    assert mock_api_client.await_count == 2


async def test_resolve_unique_ids_by_name_paginates_across_pages(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    """Resolution must not silently truncate when the server has more matches
    than fit in a single page (the bug the DEFAULT_PAGE_SIZE-only helper had)."""
    mock_api_client.side_effect = [
        _models_page(
            [{"name": "orders", "uniqueId": "model.pkg_a.orders"}],
            has_next=True,
            end_cursor="c1",
        ),
        _models_page(
            [{"name": "orders", "uniqueId": "model.pkg_b.orders"}],
            has_next=True,
            end_cursor="c2",
        ),
        _models_page(
            [{"name": "orders", "uniqueId": "model.pkg_c.orders"}],
            has_next=False,
            end_cursor="c3",
        ),
    ]

    result = await paginated_models_fetcher.resolve_unique_ids_by_name(
        "orders", config=unit_discovery_config
    )

    assert result == [
        "model.pkg_a.orders",
        "model.pkg_b.orders",
        "model.pkg_c.orders",
    ]
    assert mock_api_client.await_count == 3


async def test_resolve_unique_ids_by_name_empty(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    mock_api_client.side_effect = [_models_page([], has_next=False, end_cursor=None)]

    result = await paginated_models_fetcher.resolve_unique_ids_by_name(
        "nonexistent", config=unit_discovery_config
    )

    assert result == []


async def test_resolve_unique_ids_by_name_filters_out_malformed_edge(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    """A node missing uniqueId (malformed edge) is dropped, not raised."""
    mock_api_client.side_effect = [
        _models_page(
            [{"name": "orders"}],
            has_next=False,
            end_cursor=None,
        )
    ]

    result = await paginated_models_fetcher.resolve_unique_ids_by_name(
        "orders", config=unit_discovery_config
    )

    assert result == []


async def test_resolve_unique_ids_by_name_filters_false_positive_alias_match(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    """The server's `identifier` filter matches on alias, not name, so a
    same-aliased-but-differently-named model could come back from the server.
    It must be filtered out client-side since it doesn't actually match the
    queried name -- the old Cartesian path could never return a wrongly-named
    model."""
    mock_api_client.side_effect = [
        _models_page(
            [{"name": "unrelated_model", "uniqueId": "model.jaffle.unrelated_model"}],
            has_next=False,
            end_cursor=None,
        )
    ]

    result = await paginated_models_fetcher.resolve_unique_ids_by_name(
        "orders", config=unit_discovery_config
    )

    assert result == []


async def test_resolve_unique_ids_by_name_case_insensitive_name_match(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    """Matching against the returned node's name is case-insensitive, mirroring
    the server-side identifier filter's own case-insensitivity."""
    mock_api_client.side_effect = [
        _models_page(
            [{"name": "orders", "uniqueId": "model.jaffle.orders"}],
            has_next=False,
            end_cursor=None,
        )
    ]

    result = await paginated_models_fetcher.resolve_unique_ids_by_name(
        "Orders", config=unit_discovery_config
    )

    assert result == ["model.jaffle.orders"]
