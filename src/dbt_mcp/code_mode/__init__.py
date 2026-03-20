"""Code mode: expose dbt MCP as search + execute tools to reduce token usage.

Inspired by Cloudflare's Code Mode: the agent writes code against a minimal
catalog (search) or a dbt tool proxy (execute) instead of loading every
tool schema into context. See: https://blog.cloudflare.com/code-mode-mcp
"""

from dbt_mcp.code_mode.tools import register_code_mode_tools

__all__ = ["register_code_mode_tools"]
