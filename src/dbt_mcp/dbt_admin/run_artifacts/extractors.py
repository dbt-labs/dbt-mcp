"""Extraction functions that convert plain dicts to DuckDB row tuples.

Each function accepts a ``dict[str, Any]`` returned by ``ARTIFACT_PARSERS`` and
returns a dict mapping table names to lists of row tuples.

``ARTIFACT_PARSERS`` guarantees the input is always a plain Python dict — either
a ``model_dump(mode="json")`` result from dbt-artifacts-parser (happy path) or the
raw artifact JSON (fallback for dbt Fusion / preview builds).  Both have the same
key structure so all field access uses ``.get()`` with safe defaults.

``ArtifactType`` is re-exported here so callers can import it from one place.
"""

import json
import logging
from collections.abc import Callable
from typing import Any

from dbt_mcp.dbt_admin.run_artifacts.artifacts.parsers import ArtifactType

logger = logging.getLogger(__name__)


# ── Helpers ─────────────────────────────────────────────────────────────


def _json(data: Any) -> str:
    """Serialize ``data`` to a JSON string; returns empty string for falsy values."""
    if data is None:
        return ""
    if isinstance(data, str):
        return json.dumps(data) if data else ""
    if isinstance(data, (int, float, bool)):
        return json.dumps(data)
    if not data:
        return ""
    try:
        return json.dumps(data)
    except Exception:
        logger.debug(f"Failed to JSON-serialize artifact data: {type(data).__name__}")
        return ""


def _owner_email_str(email: Any) -> str:
    """Normalize an owner email to a plain string (groups may use ``list[str]``)."""
    if isinstance(email, list):
        return ", ".join(str(e) for e in email)
    return str(email) if email else ""


# ── Manifest extraction ─────────────────────────────────────────────────


def _map_node(idx: int, node: dict[str, Any]) -> tuple:
    """Map a manifest node or source dict to a ``nodes`` table row."""
    config = node.get("config") or {}
    depends_on = node.get("depends_on") or {}
    contract = node.get("contract") or {}
    docs = node.get("docs") or {}
    checksum = node.get("checksum") or {}

    checksum_str = (
        checksum.get("checksum", "")
        if isinstance(checksum, dict)
        else (str(checksum) if checksum else "")
    )
    node_enabled = node.get("enabled")
    config_enabled = config.get("enabled")
    unique_key = config.get("unique_key")

    return (
        idx,
        node.get("unique_id", ""),
        node.get("name", ""),
        node.get("resource_type", ""),
        node.get("package_name", ""),
        node.get("path") or node.get("file_path") or "",
        node.get("original_file_path") or "",
        _json(node.get("fqn") or []),
        node.get("alias") or "",
        checksum_str,
        node.get("description") or "",
        node.get("language") or "",
        node.get("raw_code") or node.get("raw_sql") or "",
        node.get("database") or "",
        node.get("schema") or "",  # JSON key is "schema", not "schema_"
        node.get("relation_name") or "",
        node.get("identifier") or node.get("alias") or "",
        node_enabled if node_enabled is not None else config_enabled,
        node.get("materialized") or config.get("materialized") or "",
        config.get("incremental_strategy") or "",
        config.get("on_schema_change") or "",
        _json(unique_key) if unique_key else "",
        config.get("full_refresh"),
        _json(config),
        node.get("access") or "",
        node.get("group") or "",
        contract.get("enforced"),
        str(node.get("version")) if node.get("version") is not None else "",
        str(node.get("latest_version"))
        if node.get("latest_version") is not None
        else "",
        node.get("deprecation_date") or None,
        _json(node.get("constraints") or []),
        _json(node.get("tags") or []),
        _json(node.get("meta") or {}),
        node.get("source_name") or "",
        node.get("source_description") or "",
        node.get("loader") or "",
        node.get("loaded_at_field") or "",
        _json(node.get("freshness")),
        node.get("compiled_code") or node.get("compiled_sql") or "",
        node.get("compiled_path") or "",
        _json(node.get("extra_ctes") or []),
        node.get("patch_path") or "",
        docs.get("show"),
        _json(node.get("quoting") or {}),
        _json(depends_on.get("nodes") or []),
        _json(depends_on.get("macros") or []),
    )


def _extract_node_columns(node: dict[str, Any]) -> list[tuple]:
    """Extract column rows from a manifest node dict."""
    rows = []
    columns = node.get("columns") or {}
    for idx, (col_name, col) in enumerate(columns.items()):
        if not isinstance(col, dict):
            continue
        rows.append(
            (
                node.get("unique_id", ""),
                col.get("name") or col_name,
                idx,
                col.get("data_type") or col.get("type") or "",
                None,  # catalog_type — filled later by catalog merge
                None,  # data_type resolved
                col.get("description") or "",
                _json(col.get("tags") or []),
                _json(col.get("meta") or {}),
                _json(col.get("tests") or []),
                None,  # catalog_comment
            )
        )
    return rows


def _extract_edges(node: dict[str, Any]) -> list[tuple]:
    """Extract dependency edges from a manifest node dict."""
    depends_on = node.get("depends_on") or {}
    dep_nodes = depends_on.get("nodes") or []
    unique_id = node.get("unique_id", "")
    return [(parent_id, unique_id, "ref") for parent_id in dep_nodes]


def _extract_test_metadata(node: dict[str, Any]) -> tuple | None:
    """Extract test metadata if this is a test node."""
    if node.get("resource_type") != "test":
        return None
    tm = node.get("test_metadata")
    if not tm or not isinstance(tm, dict):
        return None
    config = node.get("config") or {}
    depends_on = node.get("depends_on") or {}
    dep_nodes = depends_on.get("nodes") or []
    attached = next((n for n in dep_nodes if not n.startswith("test.")), "")
    kwargs = tm.get("kwargs") or {}

    return (
        node.get("unique_id", ""),
        tm.get("name", ""),
        tm.get("namespace"),
        _json(kwargs),
        kwargs.get("column_name", "") if isinstance(kwargs, dict) else "",
        attached,
        config.get("severity") or "",
        config.get("warn_if") or "",
        config.get("error_if") or "",
        config.get("fail_calc") or "",
        config.get("store_failures"),
        config.get("store_failures_as") or "",
    )


def _map_exposure(idx: int, exp: dict[str, Any]) -> tuple:
    """Map an exposure dict to a row."""
    depends_on = exp.get("depends_on") or {}
    owner = exp.get("owner") or {}
    return (
        idx,
        exp.get("unique_id", ""),
        exp.get("name", ""),
        exp.get("type"),
        exp.get("label") or "",
        owner.get("name") or "",
        _owner_email_str(owner.get("email") or ""),
        exp.get("url") or "",
        exp.get("maturity") or "",
        exp.get("description") or "",
        exp.get("package_name") or "",
        exp.get("path") or "",
        exp.get("original_file_path") or "",
        _json(exp.get("fqn") or []),
        _json(depends_on.get("nodes") or []),
        _json(depends_on.get("macros") or []),
        _json(exp.get("tags") or []),
        _json(exp.get("meta") or {}),
        _json(exp.get("config") or {}),
    )


def _map_metric(idx: int, metric: dict[str, Any]) -> tuple:
    """Map a metric dict to a row."""
    depends_on = metric.get("depends_on") or {}
    type_params = metric.get("type_params") or {}
    measure = type_params.get("measure") or {} if isinstance(type_params, dict) else {}
    semantic_model_name = measure.get("name", "") if isinstance(measure, dict) else ""
    return (
        idx,
        metric.get("unique_id", ""),
        metric.get("name", ""),
        metric.get("label") or "",
        metric.get("type") or metric.get("calculation_method") or "",
        metric.get("description") or "",
        metric.get("package_name") or "",
        metric.get("path") or "",
        metric.get("original_file_path") or "",
        _json(metric.get("fqn") or []),
        _json(type_params),
        metric.get("time_granularity") or "",
        semantic_model_name,
        _json(depends_on.get("nodes") or []),
        _json(depends_on.get("macros") or []),
        metric.get("group") or "",
        _json(metric.get("tags") or []),
        _json(metric.get("meta") or {}),
        _json(metric.get("config") or {}),
    )


def _map_group(idx: int, group: dict[str, Any]) -> tuple:
    """Map a group dict to a row."""
    owner = group.get("owner") or {}
    return (
        idx,
        group.get("unique_id", ""),
        group.get("name", ""),
        group.get("description") or "",
        group.get("package_name") or "",
        group.get("path") or "",
        group.get("original_file_path") or "",
        owner.get("name") or "",
        _owner_email_str(owner.get("email") or ""),
    )


def _map_macro(idx: int, macro: dict[str, Any]) -> tuple:
    """Map a macro dict to a row."""
    depends_on = macro.get("depends_on") or {}
    return (
        idx,
        macro.get("unique_id", ""),
        macro.get("name", ""),
        macro.get("package_name", ""),
        macro.get("path") or "",
        macro.get("original_file_path") or "",
        macro.get("macro_sql") or "",
        macro.get("description") or "",
        _json(depends_on.get("macros") or []),
        _json(macro.get("arguments") or []),
        _json(macro.get("meta") or {}),
    )


def extract_from_manifest(data: dict[str, Any]) -> dict[str, list[tuple]]:
    """Extract all tables from a manifest dict."""
    nodes = data.get("nodes") or {}
    sources = data.get("sources") or {}
    exposures = data.get("exposures") or {}
    metrics = data.get("metrics") or {}
    groups = data.get("groups") or {}
    macros = data.get("macros") or {}

    all_nodes = list(nodes.values()) + list(sources.values())

    node_rows: list[tuple] = []
    column_rows: list[tuple] = []
    edge_rows: list[tuple] = []
    test_rows: list[tuple] = []

    for idx, node in enumerate(all_nodes):
        if not isinstance(node, dict):
            continue
        node_rows.append(_map_node(idx, node))
        column_rows.extend(_extract_node_columns(node))
        edge_rows.extend(_extract_edges(node))
        tm = _extract_test_metadata(node)
        if tm:
            test_rows.append(tm)

    # Exposure → model edges
    for exp in exposures.values():
        if not isinstance(exp, dict):
            continue
        dep_nodes = (exp.get("depends_on") or {}).get("nodes") or []
        exp_uid = exp.get("unique_id", "")
        for parent_id in dict.fromkeys(dep_nodes):
            edge_rows.append((parent_id, exp_uid, "exposure_ref"))

    # Metric → model edges
    for metric in metrics.values():
        if not isinstance(metric, dict):
            continue
        dep_nodes = (metric.get("depends_on") or {}).get("nodes") or []
        metric_uid = metric.get("unique_id", "")
        for parent_id in dict.fromkeys(dep_nodes):
            edge_rows.append((parent_id, metric_uid, "metric_ref"))

    # Prepend sequential ids to tables that extracted without them
    column_rows = [(i, *row) for i, row in enumerate(column_rows)]
    edge_rows = [(i, *row) for i, row in enumerate(edge_rows)]
    test_rows = [(i, *row) for i, row in enumerate(test_rows)]

    exposure_rows = [
        _map_exposure(i, e)
        for i, e in enumerate(exposures.values())
        if isinstance(e, dict)
    ]
    metric_rows = [
        _map_metric(i, m) for i, m in enumerate(metrics.values()) if isinstance(m, dict)
    ]
    group_rows = [
        _map_group(i, g) for i, g in enumerate(groups.values()) if isinstance(g, dict)
    ]
    macro_rows = [
        _map_macro(i, m) for i, m in enumerate(macros.values()) if isinstance(m, dict)
    ]

    return {
        "nodes": node_rows,
        "node_columns": column_rows,
        "edges": edge_rows,
        "test_metadata": test_rows,
        "exposures": exposure_rows,
        "metrics": metric_rows,
        "groups": group_rows,
        "macros": macro_rows,
    }


# ── Catalog extraction ──────────────────────────────────────────────────


def extract_from_catalog(data: dict[str, Any]) -> dict[str, list[tuple]]:
    """Extract tables from a catalog dict."""
    nodes = data.get("nodes") or {}
    sources = data.get("sources") or {}
    # Iterate over (unique_id, entry) pairs — unique_id is the dict key, not a field in the value
    all_entries: list[tuple[str, dict[str, Any]]] = [
        (uid, entry)
        for uid, entry in list(nodes.items()) + list(sources.items())
        if isinstance(entry, dict)
    ]

    table_rows: list[tuple] = []
    stat_rows: list[tuple] = []
    column_updates: list[tuple] = []
    stat_idx = 0

    for idx, (unique_id, entry) in enumerate(all_entries):
        metadata = entry.get("metadata") or {}
        table_rows.append(
            (
                idx,
                unique_id,
                metadata.get("type"),
                metadata.get("database"),
                metadata.get("schema"),  # JSON key is "schema"
                metadata.get("name"),
                metadata.get("owner"),
                metadata.get("comment") or "",
            )
        )

        stats = entry.get("stats") or {}
        for stat_id, stat in stats.items():
            if not isinstance(stat, dict):
                continue
            stat_rows.append(
                (
                    stat_idx,
                    unique_id,
                    stat.get("id") or stat_id,
                    stat.get("label"),
                    str(stat.get("value", "")),
                    stat.get("description") or "",
                    stat.get("include"),
                )
            )
            stat_idx += 1

        columns = entry.get("columns") or {}
        for col_name, col in columns.items():
            if not isinstance(col, dict):
                continue
            column_updates.append(
                (
                    unique_id,
                    col.get("name") or col_name,
                    col.get("index"),
                    col.get("type"),
                    col.get("comment") or "",
                )
            )

    return {
        "catalog_tables": table_rows,
        "catalog_stats": stat_rows,
        "_node_columns_update": column_updates,
    }


# ── Run results extraction ──────────────────────────────────────────────


def extract_from_run_results(data: dict[str, Any]) -> dict[str, list[tuple]]:
    """Extract tables from a run_results dict."""
    metadata = data.get("metadata") or {}
    invocation_id = metadata.get("invocation_id", "")
    args = data.get("args") or {}
    results = data.get("results") or []

    which = args.get("which", "") or args.get("command", "") or ""
    select = args.get("select", "") or ""

    invocation_rows: list[tuple] = [
        (
            0,
            invocation_id,
            which,
            select,
            metadata.get("dbt_version", ""),
            metadata.get("generated_at", ""),
            data.get("elapsed_time", 0.0),
            _json(args),
            len(results),
        )
    ]

    result_rows: list[tuple] = []
    for idx, result in enumerate(results):
        if not isinstance(result, dict):
            continue
        status_str = (result.get("status") or "").lower()
        timing = result.get("timing") or []
        adapter_response = result.get("adapter_response") or {}

        result_rows.append(
            (
                idx,
                result.get("unique_id", ""),
                invocation_id,
                status_str,
                result.get("execution_time", 0.0),
                result.get("thread_id", ""),
                result.get("message") or "",
                result.get("relation_name") or "",
                _json(adapter_response),
                _json(timing),
            )
        )

    return {
        "invocations": invocation_rows,
        "run_results": result_rows,
    }


# ── Sources extraction ──────────────────────────────────────────────────


def extract_from_sources(data: dict[str, Any]) -> dict[str, list[tuple]]:
    """Extract tables from a sources (freshness) dict."""
    metadata = data.get("metadata") or {}
    invocation_id = metadata.get("invocation_id", "")
    results = data.get("results") or []

    rows: list[tuple] = []
    for idx, result in enumerate(results):
        if not isinstance(result, dict):
            continue
        unique_id = result.get("unique_id", "")
        parts = unique_id.split(".")
        criteria = result.get("criteria") or {}
        status_str = (result.get("status") or "").lower()

        rows.append(
            (
                idx,
                unique_id,
                parts[2] if len(parts) > 2 else "",
                parts[3] if len(parts) > 3 else "",
                invocation_id,
                status_str,
                result.get("max_loaded_at") or "",
                result.get("snapshotted_at") or "",
                result.get("max_loaded_at_time_ago_in_s") or 0.0,
                result.get("execution_time", 0.0),
                result.get("thread_id") or "",
                result.get("error") or "",
                criteria.get("warn_after", {}).get("count")
                if isinstance(criteria, dict)
                else None,
                criteria.get("warn_after", {}).get("period", "")
                if isinstance(criteria, dict)
                else "",
                criteria.get("error_after", {}).get("count")
                if isinstance(criteria, dict)
                else None,
                criteria.get("error_after", {}).get("period", "")
                if isinstance(criteria, dict)
                else "",
                _json(result.get("adapter_response") or {}),
                _json(result.get("timing") or []),
            )
        )

    return {"source_freshness": rows}


# ── Pipeline mappings ───────────────────────────────────────────────────

ARTIFACT_EXTRACTORS: dict[
    ArtifactType, Callable[[dict[str, Any]], dict[str, list[tuple]]]
] = {
    ArtifactType.MANIFEST: extract_from_manifest,
    ArtifactType.CATALOG: extract_from_catalog,
    ArtifactType.RUN_RESULTS: extract_from_run_results,
    ArtifactType.SOURCES: extract_from_sources,
}
