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
)
from dbt_mcp.errors import DiscoveryToolCallError, GraphQLError

# Sentinel distinguishing "omit the dbtProjectName key" from `project_name=None`
# -- the two are different wire shapes and the fetcher must handle both.
_OMIT_PROJECT_NAME = object()


def _fetcher(config) -> PublicMeshModelsFetcher:
    return PublicMeshModelsFetcher(
        config=config,
        paginator=PaginatedResourceFetcher(
            config=config,
            edges_path=("data", "environment", "applied", "models", "edges"),
            page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        ),
    )


def _page(
    nodes: list[dict],
    *,
    has_next: bool,
    end_cursor: str | None,
    project_name: str | None | object = "p",
) -> dict:
    environment: dict = {
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
    if project_name is not _OMIT_PROJECT_NAME:
        environment["dbtProjectName"] = project_name
    return {"data": {"environment": environment}}


def _node(i: int) -> dict:
    return {"name": f"model_{i}", "uniqueId": f"model.p.model_{i}", "access": "public"}


async def test_scan_narrows_to_public_and_follows_the_cursor(
    mock_api_client, unit_discovery_config
):
    """The scan is internal -- callers get every page's nodes from one call,
    so page two must carry page one's cursor.

    The access narrowing must be *sent*, not applied after: filtering
    client-side would mean transferring every private and protected model
    only to discard it. That cost is what moved this fetcher off
    `applied.resources`; mesh.py trusts the server-side filter rather than
    re-checking it (see tests/integration for live confirmation the API
    honors it).
    """
    mock_api_client.side_effect = [
        _page([_node(1)], has_next=True, end_cursor="c1"),
        _page([_node(2)], has_next=False, end_cursor="c2"),
    ]

    candidates = await _fetcher(unit_discovery_config).fetch_candidates()

    assert [n["name"] for n in candidates.nodes] == ["model_1", "model_2"]
    assert candidates.incomplete is False
    assert candidates.platform_project_name == "p"

    mesh_calls = mock_api_client.await_args_list
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
        # Always another page: the budget, not the server, ends this scan.
        return _page([_node(1)], has_next=True, end_cursor=f"c{id(variables)}")

    mock_api_client.side_effect = _side_effect

    candidates = await _fetcher(unit_discovery_config).fetch_candidates()

    assert candidates.incomplete is True
    assert len(candidates.nodes) == MESH_SCAN_MAX_PAGES
    assert candidates.platform_project_name == "p"


@pytest.mark.parametrize(
    ("relation_filter", "expected_filter"),
    [
        (
            {"database": "prod", "schema": "gold", "identifier": "fct_orders"},
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
            {"identifier": "fct_orders"},
            {"access": "public", "identifier": "fct_orders"},
        ),
    ],
    ids=["three_part", "identifier_only"],
)
async def test_relation_lookup_is_a_single_filtered_point_query(
    mock_api_client, unit_discovery_config, relation_filter, expected_filter
):
    """With `relation_filter` the returned rows *are* the matches: one
    request, no cursor follow-up even when the server offers another page.
    `incomplete` stays wired to `has_more` rather than hardcoded False, so
    if `(database, schema, identifier)` ever stops being near-unique the
    uncertainty is reported instead of rows being silently dropped.
    """
    mock_api_client.side_effect = [_page([_node(1)], has_next=True, end_cursor="c1")]

    candidates = await _fetcher(unit_discovery_config).fetch_candidates(relation_filter)

    # The direct proof the project-identity fan-out is gone: one node page,
    # one network call, total.
    assert mock_api_client.await_count == 1
    mesh_call = mock_api_client.await_args_list[0]
    assert mesh_call.args[1]["modelsFilter"] == expected_filter
    assert "after" not in mesh_call.args[1]
    assert candidates.incomplete is True


@pytest.mark.parametrize(
    ("response_outcome", "expected_exception"),
    [
        (RuntimeError("boom"), RuntimeError),
        ({"errors": [{"message": "boom"}]}, GraphQLError),
        # A null `environment`/`data` with no `errors` array is a malformed
        # response for the strict edges/pageInfo path -- that's a different
        # failure mode than a missing `dbtProjectName`, and propagates as-is
        # rather than being wrapped.
        ({"data": {"environment": None}}, TypeError),
        ({"data": None}, TypeError),
    ],
    ids=["raises", "graphql_error", "null_environment", "null_data"],
)
async def test_malformed_response_propagates_as_is(
    mock_api_client, unit_discovery_config, response_outcome, expected_exception
):
    """These failures happen while parsing the node page itself, before the
    project name is ever looked at -- there is nothing to recover by then."""
    if isinstance(response_outcome, Exception):
        mock_api_client.side_effect = response_outcome
    else:
        mock_api_client.side_effect = [response_outcome]

    with pytest.raises(expected_exception):
        await _fetcher(unit_discovery_config).fetch_candidates()


@pytest.mark.parametrize(
    "project_name",
    [None, "", _OMIT_PROJECT_NAME],
    ids=["null_project_name", "empty_project_name", "missing_project_name"],
)
async def test_missing_project_name_loses_the_otherwise_successful_scan(
    mock_api_client, unit_discovery_config, project_name
):
    """The project name is load-bearing, not auxiliary: without a confirmed
    identity nothing in a node scan can be attributed to this producer. A
    well-formed page with no usable `dbtProjectName` must raise and discard
    the otherwise-successful scan rather than return it unverifiable -- the
    caller (`_fetch_one_project` in tools.py) turns this into
    `coverage.failed` with the real error message."""
    mock_api_client.side_effect = [
        _page([_node(1)], has_next=False, end_cursor="c1", project_name=project_name)
    ]

    with pytest.raises(DiscoveryToolCallError):
        await _fetcher(unit_discovery_config).fetch_candidates()
