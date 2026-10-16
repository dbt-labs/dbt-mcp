"""Request shape and sweep mechanics for `PublicMeshModelsFetcher`.

These prove what the fetcher *sends* and how it walks pages. Whether the
server honors the filters it sends is a separate question an unrecognised
GraphQL filter key makes invisible here -- it is dropped rather than
rejected, so only tests/integration can tell sent from honored.
"""

import pytest

from dbt_mcp.discovery.client import (
    MESH_SCAN_MAX_PAGES,
    MESH_SCAN_PAGE_SIZE,
    PaginatedResourceFetcher,
    PublicMeshModelsFetcher,
    RelationComponents,
)


def _fetcher(config) -> PublicMeshModelsFetcher:
    return PublicMeshModelsFetcher(
        config=config,
        paginator=PaginatedResourceFetcher(
            config=config,
            edges_path=("data", "environment", "applied", "models", "edges"),
            page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        ),
    )


def _page(nodes: list[dict], *, has_next: bool, end_cursor: str | None) -> dict:
    return {
        "data": {
            "environment": {
                "applied": {
                    "models": {
                        "pageInfo": {
                            "hasNextPage": has_next,
                            "endCursor": end_cursor,
                        },
                        "edges": [{"node": n} for n in nodes],
                    }
                }
            }
        }
    }


def _node(i: int) -> dict:
    return {"name": f"model_{i}", "uniqueId": f"model.p.model_{i}", "access": "public"}


def _dispatch(pages: list[dict], project_name_response):
    """Route each execute_query call by shape instead of call order.

    fetch_candidates fires the model query and the project-name sub-query
    concurrently via asyncio.gather, so which one reaches execute_query
    first isn't guaranteed. The model query always sends a `modelsFilter`
    variable; the project-name sub-query never does.
    """
    pages_iter = iter(pages)

    async def _side_effect(query, variables, *, config):
        del query, config
        if "modelsFilter" in variables:
            return next(pages_iter)
        if isinstance(project_name_response, Exception):
            raise project_name_response
        return project_name_response

    return _side_effect


def _mesh_calls(mock_api_client):
    return [c for c in mock_api_client.await_args_list if "modelsFilter" in c.args[1]]


async def test_scan_narrows_to_public_and_follows_the_cursor(
    mock_api_client, unit_discovery_config
):
    """The scan is internal -- callers get every page's nodes from one call,
    so page two must carry page one's cursor.

    The access narrowing must be *sent*, not applied after: filtering
    client-side would mean transferring every private and protected model
    only to discard it. That cost is what moved this fetcher off
    `applied.resources`, and if it regresses, `_is_public` in mesh.py keeps
    the output correct while the transfer silently returns.
    """
    mock_api_client.side_effect = _dispatch(
        [
            _page([_node(1)], has_next=True, end_cursor="c1"),
            _page([_node(2)], has_next=False, end_cursor="c2"),
        ],
        {"data": {"environment": {"dbtProjectName": "p"}}},
    )

    candidates = await _fetcher(unit_discovery_config).fetch_candidates()

    assert [n["name"] for n in candidates.nodes] == ["model_1", "model_2"]
    assert candidates.incomplete is False
    assert candidates.platform_project_name == "p"

    mesh_calls = _mesh_calls(mock_api_client)
    assert len(mesh_calls) == 2
    assert mesh_calls[0].args[1]["modelsFilter"] == {"access": "public"}
    assert mesh_calls[0].args[1]["first"] == MESH_SCAN_PAGE_SIZE
    # Most-queried first, so the tool's `limit` truncates the least-used
    # matches rather than an arbitrary slice.
    assert mesh_calls[0].args[1]["sort"] == {
        "field": "queryUsageCount",
        "direction": "desc",
    }
    assert "after" not in mesh_calls[0].args[1]
    assert mesh_calls[1].args[1]["after"] == "c1"


async def test_scan_reports_incomplete_when_the_page_budget_runs_out(
    mock_api_client, unit_discovery_config
):
    """A scan that stops with the server still offering a page must say so:
    a negative lookup over a partial scan is unproven, not false."""

    async def _side_effect(query, variables, *, config):
        del query, config
        if "modelsFilter" not in variables:
            return {"data": {"environment": {"dbtProjectName": "p"}}}
        # Always another page: the budget, not the server, ends this scan.
        return _page([_node(1)], has_next=True, end_cursor=f"c{id(variables)}")

    mock_api_client.side_effect = _side_effect

    candidates = await _fetcher(unit_discovery_config).fetch_candidates()

    assert candidates.incomplete is True
    assert len(candidates.nodes) == MESH_SCAN_MAX_PAGES


@pytest.mark.parametrize(
    ("components", "expected_filter"),
    [
        (
            RelationComponents(database="prod", schema="gold", identifier="fct_orders"),
            {
                "access": "public",
                "identifier": "fct_orders",
                "database": "prod",
                "schema": "gold",
            },
        ),
        # A 1-part relation_name must not send database/schema as literal
        # None -- that asks the API to filter on a null value rather than
        # not filter on that component at all.
        (
            RelationComponents(database=None, schema=None, identifier="fct_orders"),
            {"access": "public", "identifier": "fct_orders"},
        ),
    ],
    ids=["three_part", "identifier_only"],
)
async def test_relation_lookup_is_a_single_filtered_point_query(
    mock_api_client, unit_discovery_config, components, expected_filter
):
    """With `relation_components` the returned rows *are* the matches: one
    request, no cursor follow-up even when the server offers another page.
    `incomplete` stays wired to `has_more` rather than hardcoded False, so
    if `(database, schema, identifier)` ever stops being near-unique the
    uncertainty is reported instead of rows being silently dropped.
    """
    mock_api_client.side_effect = _dispatch(
        [_page([_node(1)], has_next=True, end_cursor="c1")],
        {"data": {"environment": {"dbtProjectName": "p"}}},
    )

    candidates = await _fetcher(unit_discovery_config).fetch_candidates(components)

    mesh_calls = _mesh_calls(mock_api_client)
    assert len(mesh_calls) == 1
    assert mesh_calls[0].args[1]["modelsFilter"] == expected_filter
    assert "after" not in mesh_calls[0].args[1]
    assert candidates.incomplete is True


@pytest.mark.parametrize(
    "project_name_outcome",
    [
        RuntimeError("boom"),
        # A null `environment` with no `errors` array is a well-formed
        # GraphQL response, so raise_gql_error passes it through -- chained
        # .get() on it raises AttributeError if unguarded.
        {"data": {"environment": None}},
        # Same hazard one level up.
        {"data": None},
    ],
    ids=["raises", "null_environment", "null_data"],
)
async def test_project_name_lookup_degrades_to_none_without_losing_the_scan(
    mock_api_client, unit_discovery_config, project_name_outcome
):
    """The project-name lookup is auxiliary. However it degrades, it must
    not propagate through the asyncio.gather and discard an
    otherwise-successful node scan."""
    mock_api_client.side_effect = _dispatch(
        [_page([_node(1)], has_next=False, end_cursor="c1")],
        project_name_outcome,
    )

    candidates = await _fetcher(unit_discovery_config).fetch_candidates()

    assert [n["name"] for n in candidates.nodes] == ["model_1"]
    assert candidates.platform_project_name is None
