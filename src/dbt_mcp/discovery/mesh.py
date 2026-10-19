"""Normalization and filtering for dbt Mesh cross-project model search."""

from pydantic import BaseModel

from dbt_mcp.discovery.client import MeshModelNode, ModelFilter
from dbt_mcp.errors import InvalidParameterError

# Snowflake/Postgres `"`, BigQuery backtick, SQL Server `[ ]`, stray `'`.
_RELATION_QUOTE_TABLE = str.maketrans("", "", "\"`[]'")


class MeshModel(BaseModel):
    model_name: str
    unique_id: str
    # Middle segment of `unique_id` (`model.<package>.<name>`); equals
    # `dbt_project_name`, since a node reaches `MeshModel` only after its
    # package is confirmed to match the queried producer.
    package_name: str
    version: str | None = None
    latest_version: str | None = None
    last_run_status: str | None = None
    last_run_generated_at: str | None = None
    project_id: int
    relation_name: str | None = None
    dbt_project_name: str
    ref: str


class MeshCoverage(BaseModel):
    """Which parts of the requested scope were actually examined.

    A project whose own identity lookup failed lands in `failed`, since none
    of its nodes could be attributed to it. `models_scanned` is `None` on a
    `relation_name` lookup -- a point query, not a scan.
    """

    searched: list[str] = []
    failed: dict[str, str] = {}
    incomplete: list[str] = []
    models_scanned: int | None = 0


class MeshLookup(BaseModel):
    """Answer to an exact `relation_name` lookup.

    `found` is true only when a model at that relation is owned by one of the
    searched projects -- a node belonging to a different project (an installed
    package, say) can't be ref-ed and doesn't count. Independent of `query`,
    so narrowing `query` can empty `models` without making `found` false.

    `conclusive` is false when absence couldn't be proven -- a project in
    `coverage.failed` or `coverage.incomplete` could still hold the relation.
    Treat `found: false, conclusive: false` as "not ruled out", never as
    licence to declare a new source.
    """

    relation_name: str
    found: bool
    conclusive: bool = True


class MeshModelSearchResult(BaseModel):
    type: str = "mesh_model_search_result"
    models: list[MeshModel]
    coverage: MeshCoverage
    # deduped `dependencies.yml` list
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


def parse_relation_name(relation_name: str) -> ModelFilter:
    """Parse a warehouse relation into a `ModelFilter`, right-aligned onto
    `database`/`schema`/`identifier`.

    Accepts `identifier`, `schema.identifier`, or `database.schema.identifier`.
    Raises on an empty identifier rather than filtering on nothing.

    Splits on every literal `.` before quote stripping, so a `.` inside a
    component's own quotes mis-splits rather than erroring (not an issue for
    real dbt names).

    `database`/`schema` are omitted (not `None`) when absent, since
    `ModelFilter` is `total=False` and a missing key means "don't filter on
    this". `access` is never included here -- that's the fetcher's own
    policy, not a property of the relation string.
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
    model_filter: ModelFilter = {"identifier": identifier}
    if schema is not None:
        model_filter["schema"] = schema
    if database is not None:
        model_filter["database"] = database
    return model_filter


def _is_producer_node(node: MeshModelNode, *, platform_project_name: str) -> bool:
    """Whether the queried producer owns `node`, and so can ref it.

    Compares the package segment of `model.<package>.<name>` against the
    platform's own answer for this environment (`environment.dbtProjectName`,
    always confirmed and non-empty by the time nodes reach here). A mismatch
    is an installed package, typically -- evidence strong enough to support a
    conclusive absence.
    """
    parts = node["uniqueId"].split(".")
    return len(parts) >= 3 and parts[1] == platform_project_name


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
) -> MeshModel:
    """Normalize `node` into a `MeshModel`. Callers must have already
    confirmed the producer owns `node` (see `_is_producer_node`): any other
    node cannot be attributed to `platform_project_name` at all."""
    raw_version = node.get("version")
    raw_latest_version = node.get("latestVersion")
    version = str(raw_version) if raw_version is not None else None
    latest_version = str(raw_latest_version) if raw_latest_version is not None else None
    execution_info = node.get("executionInfo") or {}
    last_run_status = execution_info.get("lastRunStatus")
    last_run_generated_at = execution_info.get("lastRunGeneratedAt")

    return MeshModel(
        model_name=node["name"],
        unique_id=node["uniqueId"],
        package_name=platform_project_name,
        version=version,
        latest_version=latest_version,
        last_run_status=last_run_status,
        last_run_generated_at=last_run_generated_at,
        project_id=project_id,
        relation_name=node.get("relationName"),
        dbt_project_name=platform_project_name,
        ref=_render_ref(
            platform_project_name,
            node["name"],
            version=version,
            latest_version=latest_version,
        ),
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
    queried producer, so keeping it would only consume `limit`.
    """
    matches: list[MeshModel] = []
    for node in nodes:
        if not _matches_mesh_query(node, query or ""):
            continue
        if not _is_producer_node(node, platform_project_name=platform_project_name):
            continue
        matches.append(
            _to_mesh_model(
                node,
                platform_project_name=platform_project_name,
                project_id=project_id,
            )
        )
    return matches


def count_relation_matches(
    nodes: list[MeshModelNode],
    *,
    platform_project_name: str,
) -> int:
    """Count models at this relation that this producer can actually supply.

    `nodes` is already the server-filtered, public-only match set for one
    relation (see `fetch_candidates`), so this only classifies by producer
    identity. Deliberately independent of `query`: a narrowing `query` must
    not turn `lookup.found` into a false negative.
    """
    return sum(
        1
        for node in nodes
        if _is_producer_node(node, platform_project_name=platform_project_name)
    )


def resolve_lookup(
    relation_name: str | None,
    *,
    relation_hits: int,
    coverage: MeshCoverage,
) -> MeshLookup | None:
    """Build the `lookup` answer, or None when no relation was requested.

    A hit is always conclusive; only a miss can be undermined.

    Invariant: any input that can hide a genuine match from `relation_hits`
    must feed into `coverage` (e.g. `project_ids`, a failed platform name
    lookup). `query`/`limit` don't qualify -- they filter after every node is
    already classified -- nor does public-access narrowing, since a
    non-public model at the relation isn't ref-able anyway.
    """
    if relation_name is None:
        return None
    found = relation_hits > 0
    return MeshLookup(
        relation_name=relation_name,
        found=found,
        conclusive=found or (not coverage.failed and not coverage.incomplete),
    )
