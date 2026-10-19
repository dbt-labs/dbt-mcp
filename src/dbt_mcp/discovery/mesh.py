"""Normalization and filtering for dbt Mesh cross-project model search."""

from pydantic import BaseModel, ConfigDict, Field, field_validator

from dbt_mcp.discovery.client import MeshModelNode, ModelFilter


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
    `relation` lookup -- a point query, not a scan.
    """

    searched: list[str] = []
    failed: dict[str, str] = {}
    incomplete: list[str] = []
    models_scanned: int | None = 0


class MeshRelationFilter(BaseModel):
    """Exact warehouse relation to look up, as separate fields instead of one
    dotted string -- each supplied field is matched independently against a
    model's own database/schema/identifier, case-insensitively, with no
    quoting or parsing involved. `database` without `schema` is accepted:
    the underlying filter fields are independent, nothing requires
    contiguity between them.
    """

    model_config = ConfigDict(populate_by_name=True)

    identifier: str = Field(
        min_length=1, description="The relation's identifier (table/view name)."
    )
    schema_: str | None = Field(
        default=None,
        alias="schema",
        min_length=1,
        description="The relation's schema, if known.",
    )
    database: str | None = Field(
        default=None,
        min_length=1,
        description="The relation's database, if known.",
    )

    @field_validator("identifier", "schema_", "database", mode="before")
    @classmethod
    def _strip(cls, v: str | None) -> str | None:
        # Runs before min_length, so a whitespace-only value still fails
        # the length check instead of silently stripping down to "".
        return v.strip() if isinstance(v, str) else v

    def to_model_filter(self) -> ModelFilter:
        model_filter: ModelFilter = {"identifier": self.identifier}
        if self.schema_ is not None:
            model_filter["schema"] = self.schema_
        if self.database is not None:
            model_filter["database"] = self.database
        return model_filter


class MeshLookup(BaseModel):
    """Answer to an exact `relation` lookup.

    `found` is true only when a model at that relation is owned by one of the
    searched projects -- a node belonging to a different project (an installed
    package, say) can't be ref-ed and doesn't count. Independent of `query`,
    so narrowing `query` can empty `models` without making `found` false.

    `conclusive` is false when absence couldn't be proven -- a project in
    `coverage.failed` or `coverage.incomplete` could still hold the relation.
    Treat `found: false, conclusive: false` as "not ruled out", never as
    licence to declare a new source.
    """

    relation: MeshRelationFilter
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
    relation: MeshRelationFilter | None,
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
    if relation is None:
        return None
    found = relation_hits > 0
    return MeshLookup(
        relation=relation,
        found=found,
        conclusive=found or (not coverage.failed and not coverage.incomplete),
    )
