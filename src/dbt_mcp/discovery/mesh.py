"""Normalization and filtering for dbt Mesh cross-project model search.

Backs `search_public_mesh_models` (`discovery/tools.py`), which fans out across
caller-supplied `project_ids` rather than binding to one resolved project: a
cross-project ref is only actionable when the caller names the producer projects
it was resolved against.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import assert_never

from pydantic import BaseModel

from dbt_mcp.discovery.client import MeshModelNode, RelationComponents
from dbt_mcp.errors import InvalidParameterError
from dbt_mcp.tools.parameters import ModelAccess

# Snowflake/Postgres `"`, BigQuery backtick, SQL Server `[ ]`, stray `'`.
_RELATION_QUOTE_CHARS = "\"`[]'"


class MeshDependency(BaseModel):
    name: str


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
    warnings: list[str] = []


class MeshCoverage(BaseModel):
    """Which parts of the requested scope were actually examined.

    `incomplete` lists projects whose scan stopped with pages still available.
    `unresolved_identity` lists projects where a node at the requested relation
    was found but its owner could not be verified -- the platform returned no
    project name, or the node's `uniqueId` carried no package to compare it
    against. Distinct from a node confirmed to belong to a different project.
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
    dependencies: list[MeshDependency] = []
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


def _parse_access(node: MeshModelNode) -> ModelAccess | None:
    """None means the API reported no level we recognise, which must never be
    mistaken for `public`."""
    raw_access = node.get("access")
    if raw_access is None:
        return None
    try:
        return ModelAccess(raw_access)
    except ValueError:
        return None


def _is_public(node: MeshModelNode) -> bool:
    """Redundant with the server-side `access: public` filter, by design: an
    unrecognised GraphQL filter key is ignored rather than rejected, and the
    result is the *unfiltered* set -- here, private models emitted as public."""
    return _parse_access(node) is ModelAccess.PUBLIC


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
    parts = [p.strip().strip(_RELATION_QUOTE_CHARS) for p in raw_parts]
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
    #: The platform lookup failed. Ownership unknown, not ruled out.
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
    node: MeshModelNode, *, platform_project_name: str | None
) -> ProducerVerdict:
    """Classify `node` against the Discovery API's own answer for the queried
    environment's identity (`environment.dbtProjectName`), the only identity
    source. Only PRODUCER can carry a ref; only CONFIRMED_OTHER is strong
    enough to support a conclusive absence.

    Both identities are tested for falsiness rather than `is None`: the
    platform can answer with an empty `dbtProjectName`, and a malformed
    `uniqueId` yields no package. Comparing either one would make an empty
    string stand as proof of a non-match, and CONFIRMED_OTHER is exactly the
    verdict that turns a relation miss conclusive.
    """
    package_name = _package_name(node)
    if not platform_project_name or not package_name:
        return ProducerVerdict.UNRESOLVED
    if platform_project_name == package_name:
        return ProducerVerdict.PRODUCER
    return ProducerVerdict.CONFIRMED_OTHER


def build_mesh_model(
    node: MeshModelNode,
    *,
    platform_project_name: str | None,
    project_id: int,
) -> MeshModel:
    package_name = _package_name(node)
    verdict = _producer_verdict(node, platform_project_name=platform_project_name)
    if verdict is ProducerVerdict.CONFIRMED_OTHER:
        raise ValueError(
            f"{node['uniqueId']} belongs to package '{package_name}', not to "
            f"project '{platform_project_name}' -- callers must drop "
            "CONFIRMED_OTHER nodes before building a MeshModel"
        )

    raw_version = node.get("version")
    raw_latest_version = node.get("latestVersion")
    version = str(raw_version) if raw_version is not None else None
    latest_version = str(raw_latest_version) if raw_latest_version is not None else None

    warnings: list[str] = []
    dbt_project_name: str | None = None

    if verdict is ProducerVerdict.PRODUCER:
        dbt_project_name = platform_project_name
    elif not platform_project_name:
        # package_name can also be None here (both identities unreadable at
        # once); rendering it would put a bare 'None' in user-facing text.
        subject = f"package '{package_name}'" if package_name else "this model"
        warnings.append(
            f"project identity not verified for {subject} -- "
            "the platform did not return a project name for this environment"
        )
    else:
        warnings.append(
            f"project identity not verified -- unique ID '{node['uniqueId']}' "
            "is not of the form model.<package>.<name>, so this model's owning "
            "package is unknown"
        )

    return MeshModel(
        model_name=node["name"],
        unique_id=node["uniqueId"],
        package_name=package_name,
        version=version,
        latest_version=latest_version,
        project_id=project_id,
        relation_name=node.get("relationName"),
        dbt_project_name=dbt_project_name,
        warnings=warnings,
    )


def search_mesh_models(
    nodes: list[MeshModelNode],
    *,
    project_id: int,
    query: str | None = None,
    platform_project_name: str | None = None,
) -> list[MeshModel]:
    """Filter nodes to ref-able public models matching `query`, then normalize.

    A node the platform confirms belongs to a different project is dropped: it
    can never be ref-ed by the queried producer, so keeping it would only
    consume `limit`. A node whose identity couldn't be resolved is kept with a
    warning -- its ownership is unknown, not ruled out.
    """
    matches: list[MeshModel] = []
    for node in nodes:
        if not (_is_public(node) and _matches_mesh_query(node, query or "")):
            continue
        verdict = _producer_verdict(node, platform_project_name=platform_project_name)
        if verdict is ProducerVerdict.CONFIRMED_OTHER:
            continue
        matches.append(
            build_mesh_model(
                node,
                platform_project_name=platform_project_name,
                project_id=project_id,
            )
        )
    return matches


@dataclass(frozen=True, slots=True)
class RelationMatchResult:
    """`unresolved` is True when a node at the relation exists whose owner
    couldn't be determined. `hits == 0` is not a proven absence while it is
    set."""

    hits: int
    unresolved: bool


def count_relation_matches(
    nodes: list[MeshModelNode],
    *,
    platform_project_name: str | None,
) -> RelationMatchResult:
    """Count models at this relation that this producer can actually supply.

    `nodes` is already the server-filtered match set for one relation (see
    `fetch_candidates`), so this only classifies by producer identity.
    Deliberately independent of `query`: a narrowing `query` must not turn
    `lookup.found` into a false negative.
    """
    hits = 0
    unresolved = False
    for node in nodes:
        if not _is_public(node):
            continue
        match _producer_verdict(node, platform_project_name=platform_project_name):
            case ProducerVerdict.PRODUCER:
                hits += 1
            case ProducerVerdict.UNRESOLVED:
                unresolved = True
            case ProducerVerdict.CONFIRMED_OTHER:
                pass
            case _ as unreachable:
                assert_never(unreachable)
    return RelationMatchResult(hits=hits, unresolved=unresolved)


def dedupe_mesh_dependencies(models: list[MeshModel]) -> list[MeshDependency]:
    """Collapse `dbt_project_name` to one entry per producer project, in
    first-seen order, skipping models whose identity wasn't resolved."""
    seen: set[str] = set()
    dependencies: list[MeshDependency] = []
    for model in models:
        if model.dbt_project_name is None or model.dbt_project_name in seen:
            continue
        seen.add(model.dbt_project_name)
        dependencies.append(MeshDependency(name=model.dbt_project_name))
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


def build_mesh_lookup(
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
