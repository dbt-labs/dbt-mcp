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


def deprecated_description(description: str, *, replacement: str) -> str:
    """Prepend a deprecation banner to a tool description.

    Pass the result as the ``description=`` kwarg on ``@dbt_mcp_tool``.

    Args:
        description: Original tool description (e.g. from ``get_prompt(...)``).
        replacement: Name of the tool that replaces this one.
    """
    banner = (
        f"**DEPRECATED — use `{replacement}` instead.** "
        "This tool will be removed in a future release.\n\n"
    )
    return banner + description
