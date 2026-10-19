from typing import cast

import pytest
from pydantic import ValidationError

from dbt_mcp.discovery.client import MeshModelNode
from dbt_mcp.discovery.mesh import (
    MeshCoverage,
    MeshRelationFilter,
    resolve_lookup,
    search_mesh_models,
)


def _node(**overrides) -> MeshModelNode:
    node = {
        "name": "fct_sales",
        "uniqueId": "model.finance_analytics.fct_sales",
        "description": "Gold finance sales model",
        "relationName": "ANALYTICS.GOLD_FINANCE.SALES",
        "version": None,
        "latestVersion": None,
    }
    node.update(overrides)
    return cast(MeshModelNode, node)


# --- MeshRelationFilter: caller input -> ModelFilter -------------------------
# Case passed through verbatim -- the server matches case-insensitively,
# unverified here. Absent database/schema (not None) since ModelFilter is
# total=False.


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        (
            {"database": "analytics", "schema": "gold_finance", "identifier": "sales"},
            {"database": "analytics", "schema": "gold_finance", "identifier": "sales"},
        ),
        # Partial forms accepted -- no requirement that schema/database be set.
        (
            {"schema": "gold_finance", "identifier": "sales"},
            {"schema": "gold_finance", "identifier": "sales"},
        ),
        ({"identifier": "sales"}, {"identifier": "sales"}),
        # database without schema -- newly expressible, no contiguity rule.
        (
            {"database": "analytics", "identifier": "sales"},
            {"database": "analytics", "identifier": "sales"},
        ),
        # Whitespace is stripped, not treated as a distinguishing character.
        ({"identifier": "  sales  "}, {"identifier": "sales"}),
    ],
)
def test_mesh_relation_filter_builds_expected_model_filter(kwargs, expected):
    assert MeshRelationFilter(**kwargs).to_model_filter() == expected


@pytest.mark.parametrize(
    "kwargs",
    [
        {},  # identifier is required
        {"identifier": ""},
        {"identifier": "   "},  # whitespace-only -- stripped before min_length
        {"identifier": "sales", "schema": ""},
        {"identifier": "sales", "database": "   "},
    ],
)
def test_mesh_relation_filter_rejects_empty_fields(kwargs):
    with pytest.raises(ValidationError):
        MeshRelationFilter(**kwargs)


# --- search_mesh_models: selection and normalization ------------------------
# `nodes` arrive already public with a confirmed platform_project_name (both
# trusted, not re-checked here). This only asks: does the node match `query`,
# and can the queried producer ref it?


@pytest.mark.parametrize(
    ("node", "query", "platform_project_name", "expected"),
    [
        # Confirmed producer -- dbt_project_name/ref populate.
        (
            _node(),
            None,
            "finance_analytics",
            {
                "kept": True,
                "project": "finance_analytics",
                "ref": "{{ ref('finance_analytics', 'fct_sales') }}",
                "warned": False,
            },
        ),
        # Query matches name or description, case-insensitively.
        (_node(), "FINANCE", "finance_analytics", {"kept": True}),
        (_node(), "nonexistent_term", "finance_analytics", {"kept": False}),
        # All terms must match, any order -- not one contiguous phrase.
        (_node(), "sales gold finance", "finance_analytics", {"kept": True}),
        # Different package -- can never be ref-ed, would only consume limit.
        (
            _node(uniqueId="model.some_installed_package.fct_sales"),
            None,
            "finance_analytics",
            {"kept": False},
        ),
        # No package segment to compare -- same outcome as a non-producer.
        (
            _node(uniqueId="malformed_id"),
            None,
            "finance_analytics",
            {"kept": False},
        ),
        # last_run_* is raw passthrough, no staleness logic.
        (
            _node(
                executionInfo={
                    "lastRunStatus": "success",
                    "lastRunGeneratedAt": "2026-01-01T00:00:00Z",
                }
            ),
            None,
            "finance_analytics",
            {
                "kept": True,
                "last_run_status": "success",
                "last_run_generated_at": "2026-01-01T00:00:00Z",
            },
        ),
    ],
)
def test_search_mesh_models_selects_refable_public_matches(
    node, query, platform_project_name, expected
):
    results = search_mesh_models(
        [node],
        project_id=101,
        query=query,
        platform_project_name=platform_project_name,
    )

    if not expected["kept"]:
        assert results == []
        return
    assert len(results) == 1
    model = results[0]
    if "project" in expected:
        assert model.dbt_project_name == expected["project"]
    if "ref" in expected:
        assert model.ref == expected["ref"]
    if "last_run_status" in expected:
        assert model.last_run_status == expected["last_run_status"]
    if "last_run_generated_at" in expected:
        assert model.last_run_generated_at == expected["last_run_generated_at"]
    # relationName passed through verbatim, not rebuilt from database/schema.
    assert model.relation_name == "ANALYTICS.GOLD_FINANCE.SALES"


# count_relation_matches shares _is_producer_node, so identity is covered
# above; query-independence is pinned in the tool test.


@pytest.mark.parametrize(
    ("version", "latest_version", "expected_ref"),
    [
        (None, None, "{{ ref('finance_analytics', 'fct_sales') }}"),
        (2, 2, "{{ ref('finance_analytics', 'fct_sales') }}"),
        (1, 2, "{{ ref('finance_analytics', 'fct_sales', version=1) }}"),
        ("v1", "v2", "{{ ref('finance_analytics', 'fct_sales', version=\"v1\") }}"),
        (
            'rc"1',
            "v2",
            "{{ ref('finance_analytics', 'fct_sales', version=\"rc\\\"1\") }}",
        ),
        (
            "rc\\1",
            "v2",
            "{{ ref('finance_analytics', 'fct_sales', version=\"rc\\\\1\") }}",
        ),
    ],
    ids=[
        "unversioned",
        "version_is_latest",
        "numeric_pin",
        "non_numeric_pin",
        "quote_in_version_is_escaped",
        "backslash_in_version_is_escaped",
    ],
)
def test_ref_pins_the_version_only_when_it_is_not_latest(
    version, latest_version, expected_ref
):
    results = search_mesh_models(
        [_node(version=version, latestVersion=latest_version)],
        project_id=101,
        platform_project_name="finance_analytics",
    )

    assert [m.ref for m in results] == [expected_ref]


# --- resolve_lookup: the `conclusive` rule -----------------------------------


@pytest.mark.parametrize(
    ("relation_hits", "failed", "incomplete", "found", "conclusive"),
    [
        # A hit is always conclusive -- nothing unseen can unmake it.
        (1, {}, [], True, True),
        (1, {"102": "unauthorized"}, ["101"], True, True),
        # Only a miss can be undermined: by an incomplete or failed sibling.
        (0, {}, [], False, True),
        (0, {}, ["101"], False, False),
        (0, {"102": "unauthorized"}, [], False, False),
    ],
)
def test_resolve_lookup_conclusive_rule(
    relation_hits: int,
    failed: dict[str, str],
    incomplete: list[str],
    found: bool,
    conclusive: bool,
) -> None:
    """Truth table for `conclusive`. Access narrowing is server-side, so it's
    not a dimension here; a non-public hit isn't a ref-able match to hide."""
    relation = MeshRelationFilter(
        database="prod", schema="gold", identifier="fct_orders"
    )
    lookup = resolve_lookup(
        relation,
        relation_hits=relation_hits,
        coverage=MeshCoverage(
            searched=["101"],
            failed=failed,
            incomplete=incomplete,
        ),
    )

    assert lookup is not None
    # The caller's input is echoed verbatim, not the normalized form.
    assert lookup.relation == relation
    assert (lookup.found, lookup.conclusive) == (found, conclusive)
