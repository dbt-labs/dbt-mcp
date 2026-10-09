from unittest.mock import Mock

import pytest

from dbt_mcp.discovery.client import ModelsFetcher, PaginatedResourceFetcher
from dbt_mcp.errors import InvalidParameterError


@pytest.fixture
def models_fetcher(unit_discovery_config):
    return ModelsFetcher(config=unit_discovery_config, paginator=Mock())


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
def paginated_models_fetcher(unit_discovery_config):
    """ModelsFetcher backed by a real PaginatedResourceFetcher, so pagination
    is genuinely exercised rather than mocked away."""
    paginator = PaginatedResourceFetcher(
        config=unit_discovery_config,
        edges_path=("data", "environment", "applied", "models", "edges"),
        page_info_path=("data", "environment", "applied", "models", "pageInfo"),
    )
    return ModelsFetcher(config=unit_discovery_config, paginator=paginator)


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

    result = await models_fetcher.fetch_model_health(unique_id="model.project.my_model")

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
        unique_id="model.project.nonexistent"
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

    result = await paginated_models_fetcher.resolve_unique_ids_by_name("orders")

    assert result == ["model.jaffle.orders"]
    query, variables = mock_api_client.call_args[0]
    assert variables["modelsFilter"] == {"identifier": "orders"}
    assert variables["first"] == 100
    assert mock_api_client.await_count == 1


async def test_resolve_unique_ids_by_name_multi_match(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    """Multiple models can share an identifier/alias across packages; all
    real matches (same name) flow through."""
    mock_api_client.side_effect = [
        _models_page(
            [
                {"name": "orders", "uniqueId": "model.jaffle.orders"},
                {"name": "orders", "uniqueId": "model.other_pkg.orders"},
            ],
            has_next=False,
            end_cursor="cursor-2",
        ),
    ]

    result = await paginated_models_fetcher.resolve_unique_ids_by_name("orders")

    assert result == ["model.jaffle.orders", "model.other_pkg.orders"]
    assert mock_api_client.await_count == 1


@pytest.mark.parametrize("candidate_count", [1, 50])
async def test_resolve_unique_ids_by_name_requires_id_when_candidates_remain(
    paginated_models_fetcher, mock_api_client, unit_discovery_config, candidate_count
):
    """An incomplete candidate set must not be returned as complete resolution."""
    mock_api_client.side_effect = [
        _models_page(
            [
                {"name": "orders", "uniqueId": f"model.pkg_{i}.orders"}
                for i in range(candidate_count)
            ],
            has_next=True,
            end_cursor="c1",
        ),
        _models_page([], has_next=False, end_cursor=None),
    ]

    with pytest.raises(InvalidParameterError, match="provide unique_id"):
        await paginated_models_fetcher.resolve_unique_ids_by_name("orders")

    assert mock_api_client.await_count == 1


async def test_resolve_unique_ids_by_name_accepts_complete_candidate_limit(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    ids = [f"model.pkg_{i}.orders" for i in range(100)]
    mock_api_client.return_value = _models_page(
        [{"name": "orders", "uniqueId": unique_id} for unique_id in ids],
        has_next=False,
        end_cursor="last",
    )

    result = await paginated_models_fetcher.resolve_unique_ids_by_name("orders")

    assert result == ids
    assert mock_api_client.await_count == 1


async def test_resolve_unique_ids_by_name_empty(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    mock_api_client.side_effect = [_models_page([], has_next=False, end_cursor=None)]

    result = await paginated_models_fetcher.resolve_unique_ids_by_name("nonexistent")

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

    result = await paginated_models_fetcher.resolve_unique_ids_by_name("orders")

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

    result = await paginated_models_fetcher.resolve_unique_ids_by_name("orders")

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

    result = await paginated_models_fetcher.resolve_unique_ids_by_name("Orders")

    assert result == ["model.jaffle.orders"]


async def test_resolve_unique_ids_by_name_handles_null_name_field(
    paginated_models_fetcher, mock_api_client, unit_discovery_config
):
    """A node with an explicit `name: null` (key present, value None) must not
    crash -- dict.get(key, default) only falls back for a *missing* key, not
    a null value, so a bare `.get("name", "").lower()` would raise
    AttributeError here."""
    mock_api_client.side_effect = [
        _models_page(
            [{"name": None, "uniqueId": "model.jaffle.orders"}],
            has_next=False,
            end_cursor=None,
        )
    ]

    result = await paginated_models_fetcher.resolve_unique_ids_by_name("orders")

    assert result == []
