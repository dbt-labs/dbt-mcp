"""Build a rich tool spec from registered tools, prompts, and human descriptions.

The spec supports progressive disclosure: the model can list tools cheaply,
then drill into parameter schemas and usage guides only for the tools it needs.
"""

import logging
from pathlib import Path
from typing import Any

from dbt_mcp.tools.readme_mappings import HUMAN_DESCRIPTIONS
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import TOOL_TO_TOOLSET, Toolset, toolsets

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"

_TOOLSET_TO_PROMPT_DIR: dict[Toolset, str] = {
    Toolset.DISCOVERY: "discovery",
    Toolset.SEMANTIC_LAYER: "semantic_layer",
    Toolset.ADMIN_API: "admin_api",
    Toolset.DBT_CLI: "dbt_cli",
    Toolset.DBT_CODEGEN: "dbt_codegen",
    Toolset.DBT_LSP: "lsp",
    Toolset.PRODUCT_DOCS: "product_docs",
}


def _load_prompt(tool_name: str) -> str | None:
    """Try to load the prompt .md file for a tool by name."""
    try:
        tool_enum = ToolName(tool_name)
    except ValueError:
        return None
    ts = TOOL_TO_TOOLSET.get(tool_enum)
    if ts is None:
        return None
    prompt_subdir = _TOOLSET_TO_PROMPT_DIR.get(ts)
    if prompt_subdir is None:
        return None
    prompt_path = PROMPTS_DIR / prompt_subdir / f"{tool_name}.md"
    if prompt_path.is_file():
        return prompt_path.read_text(encoding="utf-8")
    return None


def _extract_params_from_schema(schema: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract parameter info from a JSON schema (inputSchema or parameters)."""
    props = schema.get("properties") or {}
    required = set(schema.get("required") or [])
    params: list[dict[str, Any]] = []
    for name, prop in props.items():
        entry: dict[str, Any] = {
            "name": name,
            "type": prop.get("type", "any"),
            "required": name in required,
        }
        if "description" in prop:
            entry["description"] = prop["description"]
        if "enum" in prop:
            entry["enum"] = prop["enum"]
        if "default" in prop:
            entry["default"] = prop["default"]
        params.append(entry)
    return params


class ToolSpec:
    """In-memory spec built at startup from registered tools + prompts."""

    def __init__(self) -> None:
        self._tools: dict[str, dict[str, Any]] = {}

    def build_from_internal_tools(self, tools: dict[str, Any]) -> None:
        """Populate the spec from the server's _tool_manager._tools dict."""
        for name, tool in tools.items():
            try:
                tool_enum = ToolName(name)
            except ValueError:
                continue

            ts = TOOL_TO_TOOLSET.get(tool_enum)
            human_desc = HUMAN_DESCRIPTIONS.get(tool_enum, "")

            schema = getattr(tool, "inputSchema", None) or getattr(tool, "parameters", None)
            params = _extract_params_from_schema(schema) if isinstance(schema, dict) else []

            self._tools[name] = {
                "name": name,
                "category": ts.value if ts else "unknown",
                "summary": human_desc,
                "params": params,
            }

    def list_categories(self) -> list[dict[str, Any]]:
        """Return categories with tool counts (cheapest query)."""
        cats: dict[str, list[str]] = {}
        for entry in self._tools.values():
            cat = entry["category"]
            cats.setdefault(cat, []).append(entry["name"])
        return [{"category": c, "tools": sorted(t)} for c, t in sorted(cats.items())]

    def list_tools(self, *, category: str | None = None) -> list[dict[str, str]]:
        """Return tool names + one-liner summaries, optionally filtered by category."""
        result: list[dict[str, str]] = []
        for entry in self._tools.values():
            if category and entry["category"] != category:
                continue
            result.append({"name": entry["name"], "summary": entry["summary"]})
        return sorted(result, key=lambda x: x["name"])

    def get_tool_detail(self, tool_name: str) -> dict[str, Any] | None:
        """Return full parameter schema for a specific tool."""
        entry = self._tools.get(tool_name)
        if entry is None:
            return None
        return {
            "name": entry["name"],
            "category": entry["category"],
            "summary": entry["summary"],
            "params": entry["params"],
        }

    def get_tool_guide(self, tool_name: str) -> str | None:
        """Return the rich prompt/usage guide for a tool (from prompts/*.md)."""
        return _load_prompt(tool_name)

    @property
    def tool_count(self) -> int:
        return len(self._tools)
