"""Helpers for marking tools as deprecated without removing them.

Tools in the dbt MCP server follow a deprecate-then-remove lifecycle:
add the replacement, mark the old tool deprecated, monitor usage, then remove.
Use these helpers to apply the deprecation signal consistently.
"""

from typing import Any


def deprecation_meta(*, replacement: str) -> dict[str, Any]:
    """Return a meta dict marking a tool as deprecated.

    Pass as the ``meta=`` kwarg on ``@dbt_mcp_tool``. Surfaces as
    ``Tool._meta`` in the MCP protocol so clients can inspect it.

    Args:
        replacement: Name of the tool that replaces this one.
    """
    return {"deprecated": True, "replacement": replacement}


def deprecated_description(
    *,
    replacement: str,
    removal_version: str | None = None,
    arg_mapping: str | None = None,
) -> str:
    """Build a short, blunt description for a deprecated tool.

    Pass the result as the ``description=`` kwarg on ``@dbt_mcp_tool``, replacing
    the tool's original description entirely — do not prepend to it. A short,
    blunt description makes a model less likely to pick the tool, which speeds
    the usage soak before removal.

    Args:
        replacement: Name of the tool that replaces this one.
        removal_version: Version the tool will be removed in/after, if known.
        arg_mapping: One line describing how args map to ``replacement``, only
            needed when ``replacement`` is not a drop-in replacement.
    """
    when = f" after {removal_version}" if removal_version else " in a future release"
    line = f"**DEPRECATED — use `{replacement}` instead.** This tool will be removed{when}."
    if arg_mapping:
        line += f"\n\n{arg_mapping}"
    return line
