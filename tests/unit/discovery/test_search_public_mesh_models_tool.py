import asyncio
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.config.config_providers import ProjectConfigProvider
from dbt_mcp.discovery.client import MeshCandidates, MeshModelNode
from dbt_mcp.discovery.mesh import MeshRelationFilter
from dbt_mcp.discovery.tools import (
    MESH_PROJECT_FAN_OUT_CONCURRENCY,
    search_public_mesh_models,
)
from dbt_mcp.errors import ConfigurationError
from dbt_mcp.errors.common import NotFoundError


@pytest.fixture
def mock_fetch_candidates():
    """The tool builds a fresh PublicMeshModelsFetcher per project rather
    than reusing one, so control goes through the class, not an instance."""
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
        "relationName": "ANALYTICS.GOLD_FINANCE.SALES",
        "version": None,
        "latestVersion": None,
    }
    node.update(overrides)
    return node


def _provider(get_config=None) -> ProjectConfigProvider:
    """`spec=ProjectConfigProvider` so the tool's own isinstance guard
    passes; `get_config` resolves a distinct config per project_id."""
    config_provider = MagicMock(spec=ProjectConfigProvider)
    config_provider.get_config = AsyncMock(
        side_effect=get_config if get_config is not None else (lambda **_: MagicMock())
    )
    return config_provider


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
        # Two projects, confirmed names -- dependencies populate automatically.
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
        # Many models, one project -- dependencies.yml still gets one entry.
        (
            [_candidates(_finance_nodes(5), project_name="finance_analytics")],
            50,
            5,
            0,
            {"finance_analytics"},
        ),
        # Dependencies dedupe before truncation, so a model past `limit` is
        # still listed.
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
        config_provider=_provider(),
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
    """A project failing at either stage -- config resolution or candidate
    fetch -- lands in coverage.failed rather than aborting the whole search."""
    if failing_stage == "get_config":

        async def get_config(*, project_id: int):
            if project_id == 1:
                return MagicMock()
            raise NotFoundError(f"No production environment for project {project_id}")

        provider = _provider(get_config)
        mock_fetch_candidates.return_value = _candidates(
            [_model_node()], project_name="finance_analytics"
        )
    else:
        provider = _provider()
        mock_fetch_candidates.side_effect = [
            RuntimeError("502 from metadata endpoint"),
            _candidates([_model_node()], project_name="finance_analytics"),
        ]

    result = await search_public_mesh_models.fn(
        config_provider=provider, project_ids=[1, 2]
    )

    assert set(result.coverage.failed) | set(result.coverage.searched) == {"1", "2"}
    assert len(result.coverage.failed) == 1
    assert len(result.models) == 1


async def test_fan_out_dedupes_project_ids_and_stays_bounded(mock_fetch_candidates):
    """Duplicate project_ids must be scanned once, not double-counted; the
    fan-out must also stay bounded at MESH_PROJECT_FAN_OUT_CONCURRENCY."""
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
        config_provider=_provider(), project_ids=[101, *distinct_ids], limit=100
    )

    assert mock_fetch_candidates.await_count == distinct_count
    assert result.coverage.searched == [str(pid) for pid in distinct_ids]
    assert result.coverage.models_scanned == distinct_count
    # `<=` avoids pinning Semaphore's exact scheduling; `>1` catches full
    # serialization.
    assert 1 < max_concurrent <= MESH_PROJECT_FAN_OUT_CONCURRENCY


_GOLD_FINANCE_SALES = MeshRelationFilter(
    database="ANALYTICS", schema="GOLD_FINANCE", identifier="SALES"
)
_GOLD_FINANCE_SALES_LOWER = MeshRelationFilter(
    database="analytics", schema="gold_finance", identifier="sales"
)
_ABSENT_TABLE = MeshRelationFilter(
    database="prod", schema="gold", identifier="absent_table"
)


@pytest.mark.parametrize(
    ("scans", "relation", "expected_found", "expected_conclusive"),
    [
        # Found in one of two fully searched projects.
        (
            [
                _candidates([_model_node()], project_name="finance_analytics"),
                _candidates([], project_name="inventory_analytics"),
            ],
            _GOLD_FINANCE_SALES,
            True,
            True,
        ),
        # Not found, fully searched -- conclusive.
        (
            [
                _candidates([], project_name="finance_analytics"),
                _candidates([], project_name="inventory_analytics"),
            ],
            _ABSENT_TABLE,
            False,
            True,
        ),
        # A sibling couldn't be searched -- miss is unproven.
        (
            [
                _candidates([], project_name="finance_analytics"),
                NotFoundError("no production environment"),
            ],
            _ABSENT_TABLE,
            False,
            False,
        ),
        # ...but a positive can't be undone by something unseen.
        (
            [
                _candidates([_model_node()], project_name="finance_analytics"),
                NotFoundError("unauthorized"),
            ],
            _GOLD_FINANCE_SALES_LOWER,
            True,
            True,
        ),
        # A sibling's scan hit the page budget -- matches may be unexamined.
        (
            [
                _candidates([], project_name="finance_analytics"),
                _candidates([], incomplete=True, project_name="inventory_analytics"),
            ],
            _ABSENT_TABLE,
            False,
            False,
        ),
        # Confirmed installed-package node -- verified non-match, miss stays
        # conclusive. Packages default to protected, so this can't occur
        # live; only this row exercises the branch.
        (
            [
                _candidates(
                    [_model_node(uniqueId="model.some_installed_package.sales")],
                    project_name="finance_analytics",
                )
            ],
            _GOLD_FINANCE_SALES,
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
        "confirmed_installed_package",
    ],
)
async def test_relation_lookup_conclusiveness_across_the_fan_out(
    mock_fetch_candidates, scans, relation, expected_found, expected_conclusive
):
    mock_fetch_candidates.side_effect = list(scans)

    result = await search_public_mesh_models.fn(
        config_provider=_provider(),
        project_ids=list(range(101, 101 + len(scans))),
        relation=relation,
    )

    assert result.lookup is not None
    assert (result.lookup.found, result.lookup.conclusive) == (
        expected_found,
        expected_conclusive,
    )


async def test_lookup_found_is_independent_of_query(mock_fetch_candidates):
    """The tool must derive relation_hits from count_relation_matches, not
    len(models) -- the query-filtered list would false-negative a relation
    that exists. Also pins models_scanned as None on the point-query path."""
    mock_fetch_candidates.return_value = _candidates(
        [_model_node()], project_name="finance_analytics"
    )

    result = await search_public_mesh_models.fn(
        config_provider=_provider(),
        project_ids=[101],
        query="nonexistent_term",
        relation=_GOLD_FINANCE_SALES_LOWER,
    )

    assert result.models == []
    assert result.lookup is not None
    assert (result.lookup.found, result.lookup.conclusive) == (True, True)
    assert result.coverage.models_scanned is None


async def test_rejects_non_project_aware_provider_before_fanning_out(
    mock_fetch_candidates,
):
    """A non-project-aware provider would silently scan one default project
    under every id -- a server misconfig, not a caller error."""
    with pytest.raises(ConfigurationError):
        await search_public_mesh_models.fn(
            config_provider=MagicMock(), project_ids=[101, 102]
        )

    mock_fetch_candidates.assert_not_called()
