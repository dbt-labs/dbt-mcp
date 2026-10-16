"""Normalization and filtering for dbt Mesh cross-project model search.

Backs `search_public_mesh_models` (`discovery/tools.py`), which fans out across
caller-supplied `project_ids` rather than binding to one resolved project: a
cross-project ref is only actionable when the caller names the producer projects
it was resolved against.
"""

from enum import StrEnum
from typing import assert_never

from pydantic import BaseModel

from dbt_mcp.discovery.client import MeshModelNode, RelationComponents
from dbt_mcp.errors import InvalidParameterError

# Snowflake/Postgres `"`, BigQuery backtick, SQL Server `[ ]`, stray `'`.
_RELATION_QUOTE_TABLE = str.maketrans("", "", "\"`[]'")


class MeshModel(BaseModel):
    model_name: str
    unique_id: str
    #: None when `unique_id` is not `model.<package>.<name>` and the owning
    #: package therefore could not be read. Never an empty string.
    package_name: str | None = None
    version: str | None = None
    latest_version: str | None = None
    project_id: int
    relation_name: str | None = None
    dbt_project_name: str | None = None
    #: The ready-to-use `{{ ref(...) }}` string. Null exactly when
    #: `dbt_project_name` is null -- there is nothing else needed to render it.
    ref: str | None = None
    warnings: list[str] = []


class MeshCoverage(BaseModel):
    """Which parts of the requested scope were actually examined.

    `incomplete` lists projects whose scan stopped with pages still available.
    `unresolved_identity` lists projects where a node at the requested relation
    was found but its owning package could not be read from its `uniqueId` --
    a single malformed node, not a project-wide failure. A project whose own
    identity lookup failed is not listed here: it lands in `failed` instead,
    since none of its nodes could be attributed to it at all.
    `models_scanned` is `None` on a `relation_name` lookup, which is a
    server-side point query rather than a scan.
    """

    searched: list[str] = []
    failed: dict[str, str] = {}
    incomplete: list[str] = []
    unresolved_identity: list[str] = []
    models_scanned: int | None = 0


class MeshLookup(BaseModel):
    """Answer to an exact `relation_name` lookup.

    `found` is true only when a public model at that relation is owned by one of
    the searched projects; a node there belonging to a different project (an
    installed package, say) cannot be ref-ed and does not count. It is
    independent of `query`, so narrowing `query` can empty `models` without
    making `found` false.

    `conclusive` is false when absence could not be proven -- a project in
    `coverage.failed`, `coverage.incomplete`, or `coverage.unresolved_identity`
    could still hold the relation. Treat `found: false, conclusive: false` as
    "not ruled out", never as licence to declare a new source.
    """

    relation_name: str
    found: bool
    conclusive: bool = True


class MeshModelSearchResult(BaseModel):
    type: str = "mesh_model_search_result"
    models: list[MeshModel]
    coverage: MeshCoverage
    #: One producer project name per entry, deduped -- the `dependencies.yml`
    #: list.
    dependencies: list[str] = []
    lookup: MeshLookup | None = None
    omitted_model_count: int = 0


def _matches_mesh_query(node: MeshModelNode, query: str) -> bool:
    """Match every whitespace-separated term of `query`, in any order, as a
    case-insensitive substring of the node's name or description."""
    terms = query.strip().lower().split()
    if not terms:
        return True
    haystack = f"{node.get('name') or ''} {node.get('description') or ''}".lower()
    return all(term in haystack for term in terms)


def parse_relation_name(relation_name: str) -> RelationComponents:
    """Parse a warehouse relation into `RelationComponents`, right-aligned.

    Accepts `identifier`, `schema.identifier`, or
    `database.schema.identifier`. Raises rather than returning an empty result:
    an empty identifier would filter on nothing, which is not the same question
    as "no relation was requested".

    Splitting on every literal `.` happens before quote stripping, so a
    component containing a `.` inside its own quotes mis-splits rather than
    erroring. dbt names don't contain those characters in practice.
    """
    accepted = (
        "expected `identifier`, `schema.identifier`, or `database.schema.identifier`"
    )
    raw_parts = relation_name.strip().split(".")
    # A leading/trailing dot is an unambiguous typo; an interior one
    # (`prod..fct_orders`) is ambiguous and must not be reinterpreted.
    while raw_parts and not raw_parts[0].strip():
        raw_parts.pop(0)
    while raw_parts and not raw_parts[-1].strip():
        raw_parts.pop()
    if not raw_parts:
        raise InvalidParameterError(
            f"relation_name must not be empty -- {accepted}, got {relation_name!r}"
        )
    parts = [p.strip().translate(_RELATION_QUOTE_TABLE) for p in raw_parts]
    if any(not part for part in parts):
        raise InvalidParameterError(
            f"relation_name has an empty component -- {accepted}, got {relation_name!r}"
        )
    if len(parts) > 3:
        raise InvalidParameterError(
            f"relation_name has {len(parts)} dot-separated parts -- {accepted}, "
            f"got {relation_name!r}"
        )
    identifier = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    database = parts[-3] if len(parts) >= 3 else None
    return RelationComponents(database=database, schema=schema, identifier=identifier)


class ProducerVerdict(StrEnum):
    """Whether the queried producer owns a node, and how well we know it."""

    #: The node's package matches the producer's confirmed name: ref-able.
    PRODUCER = "producer"
    #: The platform named this project, and it is not this node's package.
    #: Evidence of a non-match -- an installed package, typically.
    CONFIRMED_OTHER = "confirmed_other"
    #: The node's `uniqueId` carries no package to compare the platform's
    #: answer against. Ownership unknown, not ruled out.
    UNRESOLVED = "unresolved"


def _package_name(node: MeshModelNode) -> str | None:
    """The middle segment of `model.<package>.<name>`. Evidence of the owning
    project, not proof: installed packages share the same namespace shape.

    None when `uniqueId` has no such segment -- an unknown package, which must
    never be compared as though it were a real one.
    """
    unique_id = node["uniqueId"]
    return unique_id.split(".")[1] if unique_id.count(".") >= 2 else None


def _producer_verdict(
    node: MeshModelNode, *, platform_project_name: str
) -> ProducerVerdict:
    """Classify `node` against the Discovery API's own answer for the queried
    environment's identity (`environment.dbtProjectName`). `platform_project_name`
    is always a confirmed, non-empty name here: a project whose own identity
    lookup failed never reaches this far (see
    `PublicMeshModelsFetcher._fetch_platform_project_name`), so only the
    node's own identity -- a malformed `uniqueId` -- can still be unresolved.

    Only PRODUCER can carry a ref; only CONFIRMED_OTHER is strong enough to
    support a conclusive absence.
    """
    package_name = _package_name(node)
    if not package_name:
        return ProducerVerdict.UNRESOLVED
    if platform_project_name == package_name:
        return ProducerVerdict.PRODUCER
    return ProducerVerdict.CONFIRMED_OTHER


def _render_ref(
    dbt_project_name: str,
    model_name: str,
    *,
    version: str | None,
    latest_version: str | None,
) -> str:
    """Render the `{{ ref(...) }}` Jinja call. `version` is added only when
    it differs from `latest_version` -- omitting it already means "latest" --
    and rendered unquoted when it looks like a plain integer, quoted
    otherwise (dbt also accepts non-numeric version strings)."""
    if version is None or version == latest_version:
        return f"{{{{ ref('{dbt_project_name}', '{model_name}') }}}}"
    rendered_version = version if version.isdigit() else f'"{version}"'
    return (
        f"{{{{ ref('{dbt_project_name}', '{model_name}', "
        f"version={rendered_version}) }}}}"
    )


def _to_mesh_model(
    node: MeshModelNode,
    *,
    platform_project_name: str,
    project_id: int,
    verdict: ProducerVerdict,
) -> MeshModel:
    """Normalize `node` into a `MeshModel` using the caller's already-computed
    `verdict` -- recomputing it here would classify the same node twice for
    no benefit. Callers must have already dropped CONFIRMED_OTHER nodes;
    passing one is a programming error, not a data condition."""
    package_name = _package_name(node)
    raw_version = node.get("version")
    raw_latest_version = node.get("latestVersion")
    version = str(raw_version) if raw_version is not None else None
    latest_version = str(raw_latest_version) if raw_latest_version is not None else None

    warnings: list[str] = []
    dbt_project_name: str | None = None
    ref: str | None = None

    match verdict:
        case ProducerVerdict.PRODUCER:
            dbt_project_name = platform_project_name
            ref = _render_ref(
                dbt_project_name,
                node["name"],
                version=version,
                latest_version=latest_version,
            )
        case ProducerVerdict.UNRESOLVED:
            warnings.append(
                f"project identity not verified -- unique ID '{node['uniqueId']}' "
                "is not of the form model.<package>.<name>, so this model's owning "
                "package is unknown"
            )
        case ProducerVerdict.CONFIRMED_OTHER:
            raise ValueError(
                f"{node['uniqueId']} belongs to package '{package_name}', not to "
                f"project '{platform_project_name}' -- callers must drop "
                "CONFIRMED_OTHER nodes before building a MeshModel"
            )
        case _ as unreachable:
            assert_never(unreachable)

    return MeshModel(
        model_name=node["name"],
        unique_id=node["uniqueId"],
        package_name=package_name,
        version=version,
        latest_version=latest_version,
        project_id=project_id,
        relation_name=node.get("relationName"),
        dbt_project_name=dbt_project_name,
        ref=ref,
        warnings=warnings,
    )


def search_mesh_models(
    nodes: list[MeshModelNode],
    *,
    project_id: int,
    query: str | None = None,
    platform_project_name: str,
) -> list[MeshModel]:
    """Filter nodes to ref-able public models matching `query`, then normalize.

    `nodes` is already public -- the caller's server-side `access: public`
    filter is the only access narrowing applied. A node the platform confirms
    belongs to a different project is dropped: it can never be ref-ed by the
    queried producer, so keeping it would only consume `limit`. A node whose
    identity couldn't be resolved is kept with a warning -- its ownership is
    unknown, not ruled out.
    """
    matches: list[MeshModel] = []
    for node in nodes:
        if not _matches_mesh_query(node, query or ""):
            continue
        verdict = _producer_verdict(node, platform_project_name=platform_project_name)
        if verdict is ProducerVerdict.CONFIRMED_OTHER:
            continue
        matches.append(
            _to_mesh_model(
                node,
                platform_project_name=platform_project_name,
                project_id=project_id,
                verdict=verdict,
            )
        )
    return matches


def count_relation_matches(
    nodes: list[MeshModelNode],
    *,
    platform_project_name: str,
) -> tuple[int, bool]:
    """Count models at this relation that this producer can actually supply.

    `nodes` is already the server-filtered, public-only match set for one
    relation (see `fetch_candidates`), so this only classifies by producer
    identity. Deliberately independent of `query`: a narrowing `query` must
    not turn `lookup.found` into a false negative.

    Returns `(hits, unresolved)`. `unresolved` is True when a node at the
    relation exists whose owning package couldn't be read from its
    `uniqueId`; `hits == 0` is not a proven absence while it is set.
    """
    hits = 0
    unresolved = False
    for node in nodes:
        match _producer_verdict(node, platform_project_name=platform_project_name):
            case ProducerVerdict.PRODUCER:
                hits += 1
            case ProducerVerdict.UNRESOLVED:
                unresolved = True
            case ProducerVerdict.CONFIRMED_OTHER:
                pass
            case _ as unreachable:
                assert_never(unreachable)
    return hits, unresolved


def dedupe_mesh_dependencies(models: list[MeshModel]) -> list[str]:
    """Collapse `dbt_project_name` to one entry per producer project, in
    first-seen order, skipping models whose identity wasn't resolved."""
    seen: set[str] = set()
    dependencies: list[str] = []
    for model in models:
        if model.dbt_project_name is None or model.dbt_project_name in seen:
            continue
        seen.add(model.dbt_project_name)
        dependencies.append(model.dbt_project_name)
    return dependencies


def truncate_mesh_models(
    models: list[MeshModel], limit: int
) -> tuple[list[MeshModel], int]:
    """Cap the merged model list at `limit`.

    Must run after `dedupe_mesh_dependencies`: dependencies.yml needs one entry
    per producer project that matched anything, and truncating first could drop
    a project whose only matches landed past the cutoff.
    """
    omitted_count = max(0, len(models) - limit)
    return models[:limit], omitted_count


def resolve_lookup(
    relation_name: str | None,
    *,
    relation_hits: int,
    coverage: MeshCoverage,
) -> MeshLookup | None:
    """Build the `lookup` answer, or None when no relation was requested.

    A hit is always conclusive -- nothing unseen can unmake a model already
    found -- so only a miss can be undermined.

    Invariant: any input that can hide a genuine match from `relation_hits`
    must feed into `coverage` and so into `conclusive` here. `project_ids`
    qualifies (it narrows which nodes are examined at all) and so does a failed
    platform name lookup (ownership becomes unknown rather than ruled out).
    `query` and `limit` do not -- they filter after every node has been
    classified. Nor does the public-access narrowing: a non-public model at the
    relation is not a hidden match, since it isn't ref-able.
    """
    if relation_name is None:
        return None
    found = relation_hits > 0
    return MeshLookup(
        relation_name=relation_name,
        found=found,
        conclusive=found
        or (
            not coverage.failed
            and not coverage.incomplete
            and not coverage.unresolved_identity
        ),
    )
