import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ContentBlock

from dbt_mcp.config.config_providers import ProxiedToolConfig
from dbt_mcp.config.headers import ProxiedToolHeadersProvider
from dbt_mcp.config.settings import CredentialsProvider
from dbt_mcp.errors import RemoteToolError
from dbt_mcp.project.environment_resolver import get_environments_for_project
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.proxy.tools import ProxiedToolsManager
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)


@dataclass
class SqlForProjectToolContext:
    credentials_provider: CredentialsProvider


async def _get_proxied_tool_config_for_project(
    context: SqlForProjectToolContext,
    project_id: int,
) -> ProxiedToolConfig:
    """Resolve a ProxiedToolConfig for a specific project by fetching its environments."""
    settings, token_provider = await context.credentials_provider.get_credentials()
    assert settings.actual_host and settings.dbt_account_id

    auth_headers = {"Authorization": f"Bearer {token_provider.get_token()}"}
    dbt_platform_url = f"https://{settings.actual_host}"
    prod_env, dev_env = get_environments_for_project(
        dbt_platform_url=dbt_platform_url,
        account_id=settings.dbt_account_id,
        project_id=project_id,
        headers=auth_headers,
    )

    is_local = settings.actual_host.startswith("localhost")
    path = "/v1/mcp/" if is_local else "/api/ai/v1/mcp/"
    scheme = "http://" if is_local else "https://"
    prefix = f"{settings.actual_host_prefix}." if settings.actual_host_prefix else ""
    url = f"{scheme}{prefix}{settings.actual_host}{path}"

    return ProxiedToolConfig(
        user_id=settings.dbt_user_id,
        dev_environment_id=dev_env.id if dev_env else None,
        prod_environment_id=prod_env.id if prod_env else None,
        url=url,
        headers_provider=ProxiedToolHeadersProvider(token_provider=token_provider),
    )


async def _call_remote_sql_tool(
    config: ProxiedToolConfig,
    tool_name: str,
    arguments: dict[str, Any],
) -> Sequence[ContentBlock]:
    """Call a remote SQL tool with the given configuration."""
    headers = config.headers_provider.get_headers()
    if config.prod_environment_id:
        headers["x-dbt-prod-environment-id"] = str(config.prod_environment_id)
    if config.dev_environment_id:
        headers["x-dbt-dev-environment-id"] = str(config.dev_environment_id)
    if config.user_id:
        headers["x-dbt-user-id"] = str(config.user_id)

    proxied_tools_manager = ProxiedToolsManager()
    try:
        session = await proxied_tools_manager.get_remote_mcp_session(
            config.url, headers
        )
        await session.initialize()
        result = await session.call_tool(tool_name, arguments)
        if result.isError:
            raise RemoteToolError(
                f"Tool {tool_name} reported an error: {result.content}"
            )
        return result.content
    finally:
        try:
            await proxied_tools_manager.close()
        except Exception:
            logger.exception("Error closing proxied tools manager")


@dbt_mcp_tool(
    name="text_to_sql",
    description=get_prompt("sql/text_to_sql"),
    title="Text to SQL",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def text_to_sql_multiproject(
    context: SqlForProjectToolContext,
    project_id: int,
    query: str,
) -> Sequence[ContentBlock]:
    """Generate SQL from natural language for a specific project."""
    config = await _get_proxied_tool_config_for_project(context, project_id)
    if not config.prod_environment_id:
        raise ValueError(
            f"Project {project_id} does not have a production environment configured. "
            "A production environment is required for text_to_sql."
        )
    return await _call_remote_sql_tool(config, "text_to_sql", {"query": query})


@dbt_mcp_tool(
    name="execute_sql",
    description=get_prompt("sql/execute_sql"),
    title="Execute SQL",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=False,
)
async def execute_sql_multiproject(
    context: SqlForProjectToolContext,
    project_id: int,
    sql_query: str,
    limit: int | None = None,
) -> Sequence[ContentBlock]:
    """Execute SQL for a specific project."""
    config = await _get_proxied_tool_config_for_project(context, project_id)
    if not config.dev_environment_id:
        raise ValueError(
            f"Project {project_id} does not have a development environment configured. "
            "A development environment is required for execute_sql."
        )
    arguments: dict[str, Any] = {"sql_query": sql_query}
    if limit is not None:
        arguments["limit"] = limit
    return await _call_remote_sql_tool(config, "execute_sql", arguments)


MULTIPROJECT_SQL_TOOLS = [
    text_to_sql_multiproject,
    execute_sql_multiproject,
]


def register_multiproject_sql_tools(
    dbt_mcp: FastMCP,
    credentials_provider: CredentialsProvider,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    """Register multi-project SQL tools."""

    def bind_context() -> SqlForProjectToolContext:
        return SqlForProjectToolContext(credentials_provider=credentials_provider)

    register_tools(
        dbt_mcp,
        tool_definitions=[
            tool.adapt_context(bind_context) for tool in MULTIPROJECT_SQL_TOOLS
        ],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
