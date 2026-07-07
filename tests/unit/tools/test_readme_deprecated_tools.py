"""Tests that the README deprecated-tools marker mapping stays in sync with
the actual deprecation signal set via `deprecation_meta()` on tool definitions.

Scope: only the Discovery toolset (single- and multi-project) currently marks
tools deprecated. If another toolset starts deprecating tools, extend the
`_deprecated_replacements_from` call sites below.
"""

from dbt_mcp.discovery.tools import DISCOVERY_TOOLS
from dbt_mcp.discovery.tools_multiproject import MULTIPROJECT_DISCOVERY_TOOLS
from dbt_mcp.tools.readme_mappings import DEPRECATED_TOOLS


def _deprecated_replacements_from(tools) -> dict:
    """Read the real deprecation signal off tool definitions' `meta`."""
    replacements = {}
    for tool in tools:
        meta = tool.meta or {}
        if meta.get("deprecated"):
            replacements[tool.get_name()] = meta["replacement"]
    return replacements


def test_deprecated_tools_matches_tool_meta():
    """DEPRECATED_TOOLS (used to mark deprecated tools in the generated README)
    must match every tool's actual deprecation meta, so the README can't
    silently drift from the real signal."""
    actual = {
        **_deprecated_replacements_from(DISCOVERY_TOOLS),
        **_deprecated_replacements_from(MULTIPROJECT_DISCOVERY_TOOLS),
    }
    assert DEPRECATED_TOOLS == actual
