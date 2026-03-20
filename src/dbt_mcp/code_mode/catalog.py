"""Build a minimal tool catalog for code mode search (name, description, param names)."""

from typing import Any


def build_catalog_from_tools(tools: list[Any]) -> list[dict[str, Any]]:
    """Build a compact catalog from tool-like objects for token-efficient search.

    Each entry has name, description, and param_names only (no full JSON schema).
    Accepts MCP Tool (inputSchema) or InternalTool (parameters).
    """
    catalog: list[dict[str, Any]] = []
    for tool in tools:
        params: list[str] = []
        schema = getattr(tool, "inputSchema", None) or getattr(tool, "parameters", None)
        if isinstance(schema, dict):
            props = schema.get("properties") or {}
            params = list(props.keys())
        catalog.append(
            {
                "name": getattr(tool, "name", str(tool)),
                "description": (getattr(tool, "description", None) or "")[:500],
                "param_names": params,
            }
        )
    return catalog
