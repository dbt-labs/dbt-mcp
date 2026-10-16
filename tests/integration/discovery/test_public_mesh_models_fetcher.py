"""Live assertions that only the real Discovery API can settle.

Unit tests can prove the fetcher *sends* `access`, `database`, `schema` and
`identifier`, but not that the server honors them: an unrecognised GraphQL
filter key is dropped rather than rejected, and the result is the
*unfiltered* set. Likewise, nothing client-side normalizes relation case, so
a case-sensitive API would break `relation_name` lookups silently.
"""

import pytest

from dbt_mcp.discovery.client import (
    ModelsFetcher,
    PublicMeshModelsFetcher,
    RelationComponents,
)
from dbt_mcp.discovery.mesh import search_mesh_models


@pytest.mark.asyncio
async def test_scan_filter_is_honored_and_project_identity_resolves(
    public_mesh_models_fetcher: PublicMeshModelsFetcher,
    models_fetcher: ModelsFetcher,
):
    candidates = await public_mesh_models_fetcher.fetch_candidates()
    nodes = candidates.nodes
    platform_project_name = candidates.platform_project_name

    assert len(nodes) > 0
    # The `access` filter really narrows, and pins the real-world casing:
    # the API's AccessLevel enum rejects `PUBLIC`.
    assert {node["access"] for node in nodes if node.get("access")} == {"public"}

    # Unconditional on purpose. Guarding this behind `if own_project_models:`
    # would make the test pass in exactly the scenario it exists to rule out.
    assert platform_project_name is not None, (
        "the platform returned no dbtProjectName for this environment -- "
        "ref generation cannot be verified"
    )

    # Deliberately a separate, UNFILTERED query: deriving packages from the
    # public-only scan would assert that this project publishes a public
    # model of its own, a property of the fixture rather than of identity
    # resolution. This is the premise the whole feature rests on -- if
    # dbtProjectName is not dbt_project.yml's `name:`, producer-identity
    # resolution is broken for every project.
    all_models = await models_fetcher.fetch_models(limit=100)
    packages = {
        model["uniqueId"].split(".")[1]
        for model in all_models.result
        if model.get("uniqueId") and model["uniqueId"].count(".") >= 2
    }
    assert platform_project_name in packages, (
        f"dbtProjectName {platform_project_name!r} matches no package "
        f"namespace in this environment's models ({sorted(packages)})"
    )

    # Conditional on purpose: whether this environment publishes a public
    # model of its own is a fixture property, not a property of the code.
    results = search_mesh_models(
        nodes, project_id=1, platform_project_name=platform_project_name
    )
    own_project_models = [r for r in results if r.package_name == platform_project_name]
    assert all(r.dbt_project_name is not None for r in own_project_models)


@pytest.mark.asyncio
async def test_relation_point_query_is_exact_and_case_insensitive(
    public_mesh_models_fetcher: PublicMeshModelsFetcher,
):
    """`relation_name` is documented as an exact, case-insensitive match.

    A prefix- or substring-matching API would return a model the caller
    never asked about, and `build_mesh_lookup` treats every hit as
    conclusive, so a false positive is caught nowhere downstream. A
    case-*sensitive* API fails the other way: an upper-cased relation would
    miss and report `found: false, conclusive: true`, an authoritative wrong
    answer telling a developer to declare a duplicate source.
    """
    scan = await public_mesh_models_fetcher.fetch_candidates()
    assert scan.nodes, "no public model to probe against"
    target = scan.nodes[0]
    identifier = target.get("alias") or target.get("name")
    assert identifier
    database = target.get("database")
    schema = target.get("schema")

    # Upper-cased on every component: proves both that the point query
    # finds the target at all and that case does not matter.
    point = await public_mesh_models_fetcher.fetch_candidates(
        RelationComponents(
            database=database.upper() if database else None,
            schema=schema.upper() if schema else None,
            identifier=identifier.upper(),
        )
    )
    assert any(n["uniqueId"] == target["uniqueId"] for n in point.nodes), (
        f"upper-cased lookup of {identifier!r} did not return "
        f"{target['uniqueId']!r} -- the filter is case-sensitive, so "
        "parse_relation_name must normalize case"
    )
    assert all(n.get("access") == "public" for n in point.nodes)
    for node in point.nodes:
        returned = node.get("alias") or node.get("name") or ""
        assert returned.casefold() == identifier.casefold(), (
            f"point query for {identifier!r} returned {returned!r} -- the "
            "filter matches on more than exact equality"
        )

    # The loop above only catches prefix-leniency if a near-miss model
    # happens to exist here, so probe for one directly: a truncated
    # identifier must not match. Without this the test can pass vacuously.
    truncated = identifier[1:]
    assert truncated, f"identifier {identifier!r} too short to probe"
    near_miss = await public_mesh_models_fetcher.fetch_candidates(
        RelationComponents(database=database, schema=schema, identifier=truncated)
    )
    assert not any(n["uniqueId"] == target["uniqueId"] for n in near_miss.nodes), (
        f"identifier {truncated!r} matched {target['uniqueId']!r}, whose "
        f"identifier is {identifier!r} -- the filter matches on substring or "
        "suffix, not exact equality, and no coverage flag will catch it"
    )
