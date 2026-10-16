"""Pure-logic coverage for `discovery/mesh.py`.

Everything here is table-driven on purpose: the module's whole job is
classifying a node against a producer identity and deciding whether an
absence was actually proven, so the branches matter far more than the
scenarios wrapped around them.
"""

from typing import cast

import pytest

from dbt_mcp.discovery.client import MeshModelNode, RelationComponents
from dbt_mcp.discovery.mesh import (
    MeshCoverage,
    count_relation_matches,
    parse_relation_name,
    resolve_lookup,
    search_mesh_models,
)
from dbt_mcp.errors import InvalidParameterError


def _node(**overrides) -> MeshModelNode:
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
    return cast(MeshModelNode, node)


# --- parse_relation_name: caller input -> RelationComponents ----------------
#
# Case is passed through verbatim: the Discovery API's
# identifier/database/schema filters compare server-side, exactly and
# case-insensitively (pinned live in tests/integration). Quote chars are
# stripped defensively, as copy-paste junk, not as a case-pinning signal.


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "analytics.gold_finance.sales",
            RelationComponents(
                database="analytics", schema="gold_finance", identifier="sales"
            ),
        ),
        # Quote chars stripped per component, whichever dialect, plus a
        # stray single quote that is no dialect's identifier quote at all.
        (
            '"ANALYTICS".`GOLD_FINANCE`.[SALES]',
            RelationComponents(
                database="ANALYTICS", schema="GOLD_FINANCE", identifier="SALES"
            ),
        ),
        (
            "  'analytics'.gold_finance.sales.  ",
            RelationComponents(
                database="analytics", schema="gold_finance", identifier="sales"
            ),
        ),
        # Partial forms are right-aligned, not rejected.
        (
            "gold_finance.sales",
            RelationComponents(
                database=None, schema="gold_finance", identifier="sales"
            ),
        ),
        ("sales", RelationComponents(database=None, schema=None, identifier="sales")),
    ],
)
def test_parse_relation_name_accepts_supported_forms(raw, expected):
    assert parse_relation_name(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected_message_substring"),
    [
        # Empty must raise, not degrade to a no-op filter: a vacuously true
        # predicate would report "found" for a relation never actually
        # checked.
        ("", "must not be empty"),
        ("   ", "must not be empty"),
        # Quotes strip to nothing, which is an empty *component*, not an
        # absent relation -- different message, same refusal.
        ('""', "empty component"),
        ("...", "must not be empty"),
        # `prod..sales` is ambiguous -- not silently reinterpreted as
        # schema.identifier.
        ("prod..sales", "empty component"),
        # More than 3 parts: raising beats returning "no match", since a
        # fabricated negative is indistinguishable from a real one.
        ("a.b.c.d", "database.schema.identifier"),
    ],
)
def test_parse_relation_name_rejects_malformed_input(raw, expected_message_substring):
    with pytest.raises(InvalidParameterError, match=expected_message_substring):
        parse_relation_name(raw)


# --- search_mesh_models: selection and normalization ------------------------
#
# Relation matching is server-side now (client.py's _fetch_relation_match),
# and so is the public-access narrowing -- both fetchers send `access:
# public` and the live Discovery API honors it (pinned in
# tests/integration/discovery/test_public_mesh_models_fetcher.py), so
# `search_mesh_models` trusts `nodes` are already public rather than
# re-filtering. `platform_project_name` is likewise always a confirmed,
# non-empty name by the time nodes arrive here -- a project whose own
# identity lookup failed never reaches this far (see
# PublicMeshModelsFetcher._fetch_platform_project_name). What is left for
# this function: does the node match `query`, and can the queried producer
# actually ref it?


@pytest.mark.parametrize(
    ("node", "query", "platform_project_name", "expected"),
    [
        # Platform confirms the project name -- dbt_project_name and ref
        # populate, and nothing is flagged.
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
        # Every term must match, in any order -- not the phrase as one
        # contiguous substring.
        (_node(), "sales gold finance", "finance_analytics", {"kept": True}),
        # A node the platform confirms belongs elsewhere (an installed
        # package, typically) can never be ref-ed by this producer, so
        # keeping it would only consume `limit`.
        (
            _node(uniqueId="model.some_installed_package.fct_sales"),
            None,
            "finance_analytics",
            {"kept": False},
        ),
        # Unlike a confirmed non-producer, a malformed uniqueId leaves
        # ownership unknown rather than ruled out -- kept, with a warning and
        # no derived project name or ref.
        (
            _node(uniqueId="malformed_id"),
            None,
            "finance_analytics",
            {
                "kept": True,
                "project": None,
                "ref": None,
                "warned": True,
                "package": None,
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
    if "package" in expected:
        # Null, never "" -- an empty string would read as a real answer.
        assert model.package_name == expected["package"]
    assert bool(model.warnings) is expected.get("warned", False)
    # relationName is passed through verbatim rather than rebuilt from
    # database/schema/identifier, which the response no longer carries.
    assert model.relation_name == "ANALYTICS.GOLD_FINANCE.SALES"


# --- count_relation_matches: producer-identity classification ---------------
#
# `nodes` here is what _fetch_relation_match already narrowed server-side
# (public-only, included), so this classifies producer identity, it does not
# re-match the relation or re-check access. Deliberately independent of
# `query`, so a narrowing query cannot turn `lookup.found` into a false
# negative.


@pytest.mark.parametrize(
    ("node", "platform_project_name", "hits", "unresolved"),
    [
        # The queried producer owns it: ref-able, a real hit.
        (_node(), "finance_analytics", 1, False),
        # A mismatch the *platform* confirms is evidence, not a guess -- an
        # installed package really isn't this producer's model. Were this
        # unresolved instead, any project with an installed package could
        # never produce a trustworthy negative.
        (
            _node(uniqueId="model.some_installed_package.fct_sales"),
            "finance_analytics",
            0,
            False,
        ),
        # No package segment to compare the platform's answer against.
        (_node(uniqueId="malformed_id"), "finance_analytics", 0, True),
    ],
)
def test_count_relation_matches_classifies_by_producer_identity(
    node, platform_project_name, hits, unresolved
):
    result_hits, result_unresolved = count_relation_matches(
        [node], platform_project_name=platform_project_name
    )

    assert (result_hits, result_unresolved) == (hits, unresolved)


# --- resolve_lookup: the `conclusive` rule -----------------------------------


@pytest.mark.parametrize(
    (
        "relation_hits",
        "failed",
        "incomplete",
        "unresolved_identity",
        "found",
        "conclusive",
    ),
    [
        # A hit is always conclusive: a partial scan, an unsearchable
        # sibling project, or an unresolved-identity match elsewhere can
        # only hide *further* models, not unmake one already found.
        (1, {}, [], [], True, True),
        (1, {"102": "unauthorized"}, ["101"], ["103"], True, True),
        # Only a miss can be undermined -- by a partly scanned project
        # (matching models may never have been examined), by one that
        # couldn't be searched at all, or by one holding a node at the
        # relation whose producer identity couldn't be verified.
        (0, {}, [], [], False, True),
        (0, {}, ["101"], [], False, False),
        (0, {"102": "unauthorized"}, [], [], False, False),
        (0, {}, [], ["103"], False, False),
    ],
)
def test_resolve_lookup_conclusive_rule(
    relation_hits: int,
    failed: dict[str, str],
    incomplete: list[str],
    unresolved_identity: list[str],
    found: bool,
    conclusive: bool,
) -> None:
    """Truth table for the `conclusive` rule.

    The public-access narrowing is deliberately absent as a dimension: it
    happens server-side, and a non-public model at the relation is not a
    hidden match, since it isn't ref-able. `incomplete` is wired to
    `has_more` on both the scan and point-query paths, so that split needs
    no dimension either.
    """
    lookup = resolve_lookup(
        "prod.gold.fct_orders",
        relation_hits=relation_hits,
        coverage=MeshCoverage(
            searched=["101"],
            failed=failed,
            incomplete=incomplete,
            unresolved_identity=unresolved_identity,
        ),
    )

    assert lookup is not None
    # The caller's input is echoed verbatim, not the normalized form.
    assert lookup.relation_name == "prod.gold.fct_orders"
    assert (lookup.found, lookup.conclusive) == (found, conclusive)
