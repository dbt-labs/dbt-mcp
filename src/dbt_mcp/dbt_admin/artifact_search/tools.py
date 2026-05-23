"""Artifact search tool definitions, context, and registration.

Mirrors dbt_mcp/dbt_admin/tools.py: one ToolContext dataclass, N tool
functions decorated with the dbt-mcp tool decorator, a flat tool list,
and a register function.
"""

import json
import logging
import time
from dataclasses import dataclass
from typing import Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import AdminApiConfig, ConfigProvider
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.dbt_admin.run_artifacts.artifacts.parsers import ArtifactType
from dbt_mcp.dbt_admin.run_artifacts.store import ArtifactStore
from dbt_mcp.errors.admin_api import ArtifactRetrievalError
from dbt_mcp.errors.artifact_search import ArtifactValidationError
from dbt_mcp.errors.common import NotFoundError
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)


# ── Context ──────────────────────────────────────────────────────────────


@dataclass
class ArtifactSearchToolContext:
    """Context for artifact search tools.

    Holds both the Admin API client (for on-demand artifact fetching)
    and the ArtifactStore (in-memory DuckDB for querying).

    Mirrors ``AdminToolContext`` from ``dbt_mcp.dbt_admin.tools``.
    """

    admin_api_config_provider: ConfigProvider[AdminApiConfig]
    admin_client: DbtAdminAPIClient
    store: ArtifactStore

    def __init__(
        self,
        admin_api_config_provider: ConfigProvider[AdminApiConfig],
        store: ArtifactStore,
    ):
        self.admin_api_config_provider = admin_api_config_provider
        self.admin_client = DbtAdminAPIClient(admin_api_config_provider)
        self.store = store


# ── Tools ────────────────────────────────────────────────────────────────


@dbt_mcp_tool(
    description=(
        "Load dbt artifacts for a run into the searchable in-memory database. "
        "Clears any previously loaded run first — only one run is held at a time. "
        "Defaults to all four artifact types; pass artifact_types to load a subset "
        '(e.g. ["manifest.json", "run_results.json"] for a quick failure diagnosis). '
        "Indexes are built once after all artifacts are loaded for efficiency."
    ),
    title="Load Artifacts",
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=True,
)
async def load_artifacts(
    context: ArtifactSearchToolContext,
    run_id: int = Field(description="The dbt Cloud job run ID"),
    artifact_types: list[str] = Field(
        default=["manifest.json", "catalog.json", "run_results.json", "sources.json"],
        description=(
            "Which artifacts to load. Defaults to all four: "
            "manifest.json, catalog.json, run_results.json, sources.json"
        ),
    ),
) -> dict[str, Any]:
    """Clear the store, fetch artifacts from the API, and load them into DuckDB."""
    config = await context.admin_api_config_provider.get_config()

    # Clear any previously loaded run before inserting
    context.store.reset()

    all_tables: dict[str, int] = {}
    all_timing: dict[str, dict[str, int]] = {}
    errors: dict[str, str] = {}

    for artifact_str in artifact_types:
        try:
            art_type = ArtifactType(artifact_str)
        except ValueError:
            errors[artifact_str] = f"Invalid artifact type: {artifact_str}"
            continue
        try:
            raw_text = await context.admin_client.get_job_run_artifact(
                config.account_id, run_id, art_type.value
            )
        except NotFoundError as e:
            errors[artifact_str] = (
                f"Artifact '{artifact_str}' not found for run {run_id}. "
                f"Use list_job_run_artifacts to see available artifacts."
            )
            logger.warning(f"Artifact not found: {e}")
            continue
        except ArtifactRetrievalError as e:
            errors[artifact_str] = (
                f"Failed to fetch '{artifact_str}' for run {run_id}: {e}"
            )
            logger.warning(f"Artifact retrieval error: {e}")
            continue

        try:
            data = json.loads(raw_text)
        except (json.JSONDecodeError, TypeError) as e:
            errors[artifact_str] = (
                f"Invalid JSON in '{artifact_str}' for run {run_id}: {e}"
            )
            continue

        if not isinstance(data, dict):
            errors[artifact_str] = (
                f"Expected JSON object for '{artifact_str}', got {type(data).__name__}"
            )
            continue

        logger.info(
            f"Fetched {artifact_str} for run {run_id} ({len(raw_text):,} bytes)"
        )

        try:
            result = context.store.load_artifact(run_id, art_type, data, reindex=False)
            all_tables.update(result["tables"])
            all_timing[artifact_str] = result["timing"]
        except (ArtifactValidationError, Exception) as e:
            errors[artifact_str] = str(e)

    # Build all indexes once after all artifacts are loaded
    t_index = time.perf_counter()
    indexed = context.store.build_all_indexes()
    index_build_ms = round((time.perf_counter() - t_index) * 1000)

    return {
        "status": "loaded",
        "run_id": run_id,
        "tables_loaded": all_tables,
        "indexes_built": indexed,
        "timing_ms": all_timing,
        "index_build_ms": index_build_ms,
        "errors": errors,
    }


@dbt_mcp_tool(
    description=(
        "Drop all loaded artifact tables and reset the in-memory store to empty. "
        "Use this to free memory or wipe the store without loading a new run. "
        "Note: load_artifacts already clears the previous run automatically."
    ),
    title="Clear Artifact Store",
    read_only_hint=False,
    destructive_hint=True,
    idempotent_hint=True,
)
async def clear_artifact_store(
    context: ArtifactSearchToolContext,
) -> dict[str, Any]:
    """Reset the in-memory store to empty by dropping all artifact tables."""
    dropped = context.store.reset()
    return {"status": "cleared", "tables_dropped": dropped}


@dbt_mcp_tool(
    description=get_prompt("admin_api/artifact_search/list_artifact_tables"),
    title="List Artifact Tables",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_artifact_tables(
    context: ArtifactSearchToolContext,
) -> list[dict[str, Any]]:
    """List all loaded dbt artifact tables and their row counts."""
    return context.store.list_tables()


@dbt_mcp_tool(
    description=get_prompt("admin_api/artifact_search/describe_artifact_table"),
    title="Describe Artifact Table",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def describe_artifact_table(
    context: ArtifactSearchToolContext,
    table_name: str = Field(description="Name of the artifact table to describe"),
) -> list[dict[str, str]]:
    """Show column names and types for a loaded artifact table."""
    return context.store.describe_table(table_name)


@dbt_mcp_tool(
    description=get_prompt("admin_api/artifact_search/query_artifacts"),
    title="Query Artifacts",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def query_artifacts(
    context: ArtifactSearchToolContext,
    sql: str = Field(
        description=(
            "Read-only SQL query (SELECT only) against the in-memory artifact database. "
            "Supports JOINs, aggregations, CTEs, window functions. Results capped at 500 rows."
        )
    ),
) -> list[dict[str, Any]]:
    """Execute a read-only SQL query against loaded artifact data."""
    return context.store.query(sql)


@dbt_mcp_tool(
    description=get_prompt("admin_api/artifact_search/search_artifacts"),
    title="Search Artifacts",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def search_artifacts(
    context: ArtifactSearchToolContext,
    table_name: str = Field(
        description="Table to search (e.g. nodes, macros, run_results)"
    ),
    search_query: str = Field(
        description="Search terms for BM25 full-text search across indexed columns"
    ),
    limit: int = Field(default=20, description="Maximum number of results to return"),
) -> list[dict[str, Any]]:
    """Full-text BM25 keyword search on a loaded artifact table."""
    return context.store.search(
        table_name=table_name, query_text=search_query, limit=limit
    )


# ── Tool list ────────────────────────────────────────────────────────────

ARTIFACT_SEARCH_TOOLS = [
    load_artifacts,
    clear_artifact_store,
    list_artifact_tables,
    describe_artifact_table,
    query_artifacts,
    search_artifacts,
]


# ── Registration ─────────────────────────────────────────────────────────


def register_artifact_search_tools(
    dbt_mcp: FastMCP,
    admin_config_provider: ConfigProvider[AdminApiConfig],
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> ArtifactStore:
    """Register artifact search tools with the MCP server.

    Mirrors ``register_admin_api_tools()`` from ``dbt_mcp.dbt_admin.tools``.

    Returns the shared ``ArtifactStore`` instance so callers can
    reference it (e.g. for cleanup).
    """
    shared_store = ArtifactStore()

    def bind_context() -> ArtifactSearchToolContext:
        return ArtifactSearchToolContext(
            admin_api_config_provider=admin_config_provider,
            store=shared_store,
        )

    register_tools(
        dbt_mcp,
        tool_definitions=[
            tool.adapt_context(bind_context) for tool in ARTIFACT_SEARCH_TOOLS
        ],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    return shared_store
