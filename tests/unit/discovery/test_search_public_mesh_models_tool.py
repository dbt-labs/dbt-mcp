"""Fan-out behavior of the `search_public_mesh_models` tool.

The tool's own job is everything that happens *across* projects: resolving a
config per project_id, isolating one project's failure from the rest, and
merging the per-project answers into a single result whose coverage honestly
describes what was examined. Per-node classification lives in
test_search_public_mesh_models.py; request shape in
test_public_mesh_models_fetcher.py.
"""

import asyncio
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.config.config_providers import ProjectConfigProvider
from dbt_mcp.discovery.client import MeshCandidates, MeshModelNode
from dbt_mcp.discovery.tools import (
    MESH_PROJECT_FAN_OUT_CONCURRENCY,
    MeshToolContext,
    search_public_mesh_models,
)
from dbt_mcp.errors import ConfigurationError, InvalidParameterError
from dbt_mcp.errors.common import NotFoundError


@pytest.fixture
def mock_fetch_candidates():
    """Patch the PublicMeshModelsFetcher the tool constructs per project.

    The tool builds a fresh fetcher bound to that project's own config
    inside its fan-out rather than reusing one held on the context, so
    control goes through the class it constructs, not an attribute on the
    context.
    """
    with patch("dbt_mcp.discovery.tools.PublicMeshModelsFetcher") as mock_cls:
        mock_cls.return_value.fetch_candidates = AsyncMock()
        yield mock_cls.return_value.fetch_candidates


def _candidates(
    nodes: list[dict],
    *,
    incomplete: bool = False,
    project_name: str = "finance_analytics",
) -> MeshCandidates:
    return MeshCandidates(
        nodes=cast(list[MeshModelNode], nodes),
        incomplete=incomplete,
        platform_project_name=project_name,
    )


def _model_node(**overrides) -> dict:
    node = {
        "name": "fct_sales",
        "uniqueId": "model.finance_analytics.fct_sales",
        "description": "Gold finance sales model",
        "access": "public",
        "relationName": "ANALYTICS.GOLD_FINANCE.SALES",
        "version": None,
        "latestVersion": None,
    }
    node.update(overrides)
    return node


def _context(get_config=None) -> MeshToolContext:
    """`spec=ProjectConfigProvider` so the tool's own isinstance guard
    passes; `get_config` resolves a distinct config per project_id."""
    config_provider = MagicMock(spec=ProjectConfigProvider)
    config_provider.get_config = AsyncMock(
        side_effect=get_config if get_config is not None else (lambda **_: MagicMock())
    )
    return MeshToolContext(config_provider=config_provider)


def _finance_nodes(count: int) -> list[dict]:
    return [
        _model_node(
            name=f"fct_sales_{i}",
            uniqueId=f"model.finance_analytics.fct_sales_{i}",
        )
        for i in range(count)
    ]


@pytest.mark.parametrize(
    (
        "scans",
        "limit",
        "expected_model_count",
        "expected_omitted_count",
        "expected_dependency_names",
    ),
    [
        # Two producer projects, one model each -- the platform confirms
        # both names, so dependencies populate automatically.
        (
            [
                _candidates([_model_node()], project_name="finance_analytics"),
                _candidates(
                    [
                        _model_node(
                            name="inventory_daily",
                            uniqueId="model.inventory_analytics.inventory_daily",
                        )
                    ],
                    project_name="inventory_analytics",
                ),
            ],
            50,
            2,
            0,
            {"finance_analytics", "inventory_analytics"},
        ),
        # Many models from one project still collapse to a single
        # dependencies.yml entry -- that file needs a project declared once.
        (
            [_candidates(_finance_nodes(5), project_name="finance_analytics")],
            50,
            5,
            0,
            {"finance_analytics"},
        ),
        # The ordering invariant from truncate_mesh_models: dependencies are
        # deduped *before* truncation, so a producer whose only matching
        # model lands past `limit` must still be listed. Truncating first
        # would silently break the dependencies.yml suggestion.
        (
            [
                _candidates(_finance_nodes(3), project_name="finance_analytics"),
                _candidates(
                    [
                        _model_node(
                            name="dim_vendors",
                            uniqueId="model.inventory_analytics.dim_vendors",
                        )
                    ],
                    project_name="inventory_analytics",
                ),
            ],
            3,
            3,
            1,
            {"finance_analytics", "inventory_analytics"},
        ),
    ],
    ids=["two_projects", "one_project_many_models", "truncated_past_limit"],
)
async def test_merges_scans_truncating_models_but_not_dependencies(
    mock_fetch_candidates,
    scans,
    limit,
    expected_model_count,
    expected_omitted_count,
    expected_dependency_names,
):
    mock_fetch_candidates.side_effect = list(scans)

    result = await search_public_mesh_models.fn(
        context=_context(),
        project_ids=list(range(101, 101 + len(scans))),
        limit=limit,
    )

    assert len(result.models) == expected_model_count
    assert result.omitted_model_count == expected_omitted_count
    assert set(result.dependencies) == expected_dependency_names
    # No relation asked, no lookup answered.
    assert result.lookup is None


@pytest.mark.parametrize("failing_stage", ["get_config", "fetch_candidates"])
async def test_isolates_per_project_failures(mock_fetch_candidates, failing_stage):
    """One inaccessible producer project -- whether it fails resolving its
    config (unauthorized project_id, no prod environment) or fetching its
    candidates (a GraphQL/HTTP error on that project's endpoint) -- must
    land in coverage.failed rather than propagate out of asyncio.gather and
    abort the whole multi-project search."""
    if failing_stage == "get_config":

        async def get_config(*, project_id: int):
            if project_id == 1:
                return MagicMock()
            raise NotFoundError(f"No production environment for project {project_id}")

        context = _context(get_config)
        mock_fetch_candidates.return_value = _candidates(
            [_model_node()], project_name="finance_analytics"
        )
    else:
        context = _context()
        mock_fetch_candidates.side_effect = [
            RuntimeError("502 from metadata endpoint"),
            _candidates([_model_node()], project_name="finance_analytics"),
        ]

    result = await search_public_mesh_models.fn(context=context, project_ids=[1, 2])

    assert set(result.coverage.failed) | set(result.coverage.searched) == {"1", "2"}
    assert len(result.coverage.failed) == 1
    assert len(result.models) == 1


async def test_fan_out_dedupes_project_ids_and_stays_bounded(mock_fetch_candidates):
    """A duplicate project_id must not be scanned twice -- that would
    double-count it in coverage.searched/models_scanned and emit duplicate
    models for one underlying scan. And an unbounded gather over
    `project_ids` could burst one scan per project at the metadata API
    simultaneously, so no more than MESH_PROJECT_FAN_OUT_CONCURRENCY may be
    in flight at once.
    """
    concurrent = 0
    max_concurrent = 0

    async def _fetch(relation_components=None):
        del relation_components
        nonlocal concurrent, max_concurrent
        concurrent += 1
        max_concurrent = max(max_concurrent, concurrent)
        await asyncio.sleep(0)
        concurrent -= 1
        return _candidates([_model_node()], project_name="finance_analytics")

    mock_fetch_candidates.side_effect = _fetch
    distinct_count = MESH_PROJECT_FAN_OUT_CONCURRENCY * 3
    distinct_ids = list(range(101, 101 + distinct_count))

    result = await search_public_mesh_models.fn(
        context=_context(), project_ids=[101, *distinct_ids], limit=100
    )

    assert mock_fetch_candidates.await_count == distinct_count
    assert result.coverage.searched == [str(pid) for pid in distinct_ids]
    assert result.coverage.models_scanned == distinct_count
    # `<=` pins the guarantee that matters; exact equality would also pin
    # asyncio.Semaphore's fast-path scheduling, which could flip on an
    # unrelated asyncio change. `> 1` guards the opposite failure mode: a
    # bug that serializes the fan-out entirely.
    assert 1 < max_concurrent <= MESH_PROJECT_FAN_OUT_CONCURRENCY


@pytest.mark.parametrize(
    ("scans", "relation_name", "expected_found", "expected_conclusive"),
    [
        # Found in one of two fully searched projects.
        (
            [
                _candidates([_model_node()], project_name="finance_analytics"),
                _candidates([], project_name="inventory_analytics"),
            ],
            "ANALYTICS.GOLD_FINANCE.SALES",
            True,
            True,
        ),
        # Not found, every project fully searched -- conclusive. Both scans
        # are empty because fetch_candidates already returns the
        # server-filtered match set; a mock holding an unrelated model would
        # misrepresent what the server would return.
        (
            [
                _candidates([], project_name="finance_analytics"),
                _candidates([], project_name="inventory_analytics"),
            ],
            "prod.gold.absent_table",
            False,
            True,
        ),
        # A sibling project couldn't be searched at all, so the miss is
        # unproven -- that project might hold the relation.
        (
            [
                _candidates([], project_name="finance_analytics"),
                NotFoundError("no production environment"),
            ],
            "prod.gold.absent_table",
            False,
            False,
        ),
        # ...but a positive can't be undone by something unseen.
        (
            [
                _candidates([_model_node()], project_name="finance_analytics"),
                NotFoundError("unauthorized"),
            ],
            "analytics.gold_finance.sales",
            True,
            True,
        ),
        # A sibling's scan hit the page budget before finishing -- matching
        # models may never have been examined.
        (
            [
                _candidates([], project_name="finance_analytics"),
                _candidates([], incomplete=True, project_name="inventory_analytics"),
            ],
            "prod.gold.absent_table",
            False,
            False,
        ),
        # A node at the relation whose uniqueId carries no package to compare
        # against is unresolved, not ruled out -- this producer's ownership
        # of it is unknown, so without tracking unresolved identity
        # separately this would report `found: false, conclusive: true`
        # despite a real match sitting right there.
        (
            [
                _candidates(
                    [_model_node(uniqueId="malformed_id")],
                    project_name="finance_analytics",
                )
            ],
            "ANALYTICS.GOLD_FINANCE.SALES",
            False,
            False,
        ),
        # A node at the relation that the platform confirms belongs to an
        # installed package is a verified non-match, so this miss stays
        # conclusive -- otherwise no project with an installed package could
        # ever produce a trustworthy negative. Packages default to
        # `protected`, so a *public* one cannot be produced live; this row
        # is the only place the branch is exercised.
        (
            [
                _candidates(
                    [_model_node(uniqueId="model.some_installed_package.sales")],
                    project_name="finance_analytics",
                )
            ],
            "ANALYTICS.GOLD_FINANCE.SALES",
            False,
            True,
        ),
    ],
    ids=[
        "found",
        "absent_fully_searched",
        "absent_with_failed_sibling",
        "found_despite_failed_sibling",
        "absent_with_incomplete_sibling",
        "unresolved_identity",
        "confirmed_installed_package",
    ],
)
async def test_relation_lookup_conclusiveness_across_the_fan_out(
    mock_fetch_candidates, scans, relation_name, expected_found, expected_conclusive
):
    mock_fetch_candidates.side_effect = list(scans)

    result = await search_public_mesh_models.fn(
        context=_context(),
        project_ids=list(range(101, 101 + len(scans))),
        relation_name=relation_name,
    )

    assert result.lookup is not None
    assert (result.lookup.found, result.lookup.conclusive) == (
        expected_found,
        expected_conclusive,
    )


async def test_lookup_found_is_independent_of_query(mock_fetch_candidates):
    """Deliberately not a resolve_lookup test: that helper takes an
    already-computed relation_hits and never sees `query`. What matters is
    that the *tool* derives relation_hits from count_relation_matches rather
    than from len(models) -- using the filtered list would report
    found=false for a relation that does exist, telling a developer to
    duplicate a governed public model.

    Also pins models_scanned as None here: `relation_name` makes
    fetch_candidates a point query, not a scan, so the field that otherwise
    reports environment-wide scan size must read "not applicable" rather
    than the point query's own tiny row count.
    """
    mock_fetch_candidates.return_value = _candidates(
        [_model_node()], project_name="finance_analytics"
    )

    result = await search_public_mesh_models.fn(
        context=_context(),
        project_ids=[101],
        query="nonexistent_term",
        relation_name="analytics.gold_finance.sales",
    )

    assert result.models == []
    assert result.lookup is not None
    assert (result.lookup.found, result.lookup.conclusive) == (True, True)
    assert result.coverage.models_scanned is None


@pytest.mark.parametrize(
    ("context_factory", "kwargs", "expected_error"),
    [
        # Parsing happens before the fan-out, so a typo costs zero round
        # trips rather than one per configured project.
        (_context, {"relation_name": "a.b.c.d"}, InvalidParameterError),
        # Every project in the fan-out needs its own resolved config. A
        # provider that cannot resolve per project_id would silently scan
        # the same default project under every supplied id, mislabeling
        # coverage.searched. That is a server misconfiguration, not a caller
        # mistake, so it must not surface as a client error.
        (lambda: MeshToolContext(config_provider=MagicMock()), {}, ConfigurationError),
    ],
    ids=["malformed_relation_name", "non_project_aware_provider"],
)
async def test_rejects_bad_input_before_fanning_out(
    mock_fetch_candidates, context_factory, kwargs, expected_error
):
    with pytest.raises(expected_error):
        await search_public_mesh_models.fn(
            context=context_factory(), project_ids=[101, 102], **kwargs
        )

    mock_fetch_candidates.assert_not_called()
