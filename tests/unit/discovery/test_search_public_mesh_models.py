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
    build_mesh_lookup,
    count_relation_matches,
    parse_relation_name,
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
# so by the time nodes arrive here a relation lookup has already been
# narrowed. What is left is: is this node public, does it match `query`, and
# can the queried producer actually ref it?


@pytest.mark.parametrize(
    ("node", "query", "platform_project_name", "expected"),
    [
        # Platform confirms the project name -- dbt_project_name populates
        # and nothing is flagged.
        (
            _node(),
            None,
            "finance_analytics",
            {"kept": True, "project": "finance_analytics", "warned": False},
        ),
        # Scoped to public Mesh interfaces, not a general model browser.
        (_node(access="private"), None, "finance_analytics", {"kept": False}),
        (_node(access="protected"), None, "finance_analytics", {"kept": False}),
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
        # Unlike a confirmed non-producer, an identity that could not be
        # read is unknown rather than ruled out -- kept, with a warning and
        # no derived project name. Both falsy-identity causes below.
        (_node(), None, None, {"kept": True, "project": None, "warned": True}),
        (_node(), None, "", {"kept": True, "project": None, "warned": True}),
        (
            _node(uniqueId="malformed_id"),
            None,
            "finance_analytics",
            {"kept": True, "project": None, "warned": True, "package": None},
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
    if "package" in expected:
        # Null, never "" -- an empty string would read as a real answer.
        assert model.package_name == expected["package"]
    assert bool(model.warnings) is expected.get("warned", False)
    # relationName is passed through verbatim rather than rebuilt from
    # database/schema/identifier, which the response no longer carries.
    assert model.relation_name == "ANALYTICS.GOLD_FINANCE.SALES"


# --- count_relation_matches: producer-identity classification ---------------
#
# `nodes` here is what _fetch_relation_match already narrowed server-side;
# this classifies producer identity, it does not re-match the relation.
# Deliberately independent of `query`, so a narrowing query cannot turn
# `lookup.found` into a false negative.


@pytest.mark.parametrize(
    ("node", "platform_project_name", "hits", "unresolved"),
    [
        # The queried producer owns it: ref-able, a real hit.
        (_node(), "finance_analytics", 1, False),
        # Not public, so not a Mesh interface and not a hidden match.
        (_node(access="private"), "finance_analytics", 0, False),
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
        # Both falsy-identity causes: the platform named nothing, or it
        # named something empty. Comparing either would let "" stand as
        # proof that every node belongs to another project.
        (_node(), None, 0, True),
        (_node(), "", 0, True),
        # No package segment to compare the platform's answer against.
        (_node(uniqueId="malformed_id"), "finance_analytics", 0, True),
    ],
)
def test_count_relation_matches_classifies_by_producer_identity(
    node, platform_project_name, hits, unresolved
):
    result = count_relation_matches([node], platform_project_name=platform_project_name)

    assert (result.hits, result.unresolved) == (hits, unresolved)


# --- build_mesh_lookup: the `conclusive` rule --------------------------------


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
def test_build_mesh_lookup_conclusive_rule(
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
    lookup = build_mesh_lookup(
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
