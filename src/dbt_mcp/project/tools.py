import logging
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.settings import CredentialsProvider
from dbt_mcp.project.environment_resolver import get_environments_for_project
from dbt_mcp.project.project_resolver import (
    get_all_accounts,
    get_all_projects_for_account,
)
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)


@dataclass
class ProjectToolContext:
    credentials_provider: CredentialsProvider

    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider


@dataclass
class ProjectEnvironmentInfo:
    project_id: int
    project_name: str
    account_id: int
    account_name: str
    prod_environment_id: int | None
    prod_environment_name: str | None
    dev_environment_id: int | None
    dev_environment_name: str | None


@dbt_mcp_tool(
    description=get_prompt("project/list_projects_and_environments"),
    title="List Projects and Environments",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_projects_and_environments(
    context: ProjectToolContext,
) -> list[ProjectEnvironmentInfo]:
    settings, token_provider = await context.credentials_provider.get_credentials()
    assert settings.actual_host

    dbt_platform_url = f"https://{settings.actual_host}"
    if settings.actual_host_prefix:
        dbt_platform_url = (
            f"https://{settings.actual_host_prefix}.{settings.actual_host}"
        )

    auth_headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token_provider.get_token()}",
    }

    accounts = get_all_accounts(
        dbt_platform_url=dbt_platform_url,
        headers=auth_headers,
    )

    results: list[ProjectEnvironmentInfo] = []
    for account in accounts:
        projects = get_all_projects_for_account(
            dbt_platform_url=dbt_platform_url,
            account=account,
            headers=auth_headers,
        )
        for project in projects:
            prod_env, dev_env = get_environments_for_project(
                dbt_platform_url=dbt_platform_url,
                account_id=account.id,
                project_id=project.id,
                headers=auth_headers,
            )
            results.append(
                ProjectEnvironmentInfo(
                    project_id=project.id,
                    project_name=project.name,
                    account_id=account.id,
                    account_name=account.name,
                    prod_environment_id=prod_env.id if prod_env else None,
                    prod_environment_name=prod_env.name if prod_env else None,
                    dev_environment_id=dev_env.id if dev_env else None,
                    dev_environment_name=dev_env.name if dev_env else None,
                )
            )

    return results


PROJECT_TOOLS = [
    list_projects_and_environments,
]


def register_project_tools(
    dbt_mcp: FastMCP,
    credentials_provider: CredentialsProvider,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    def bind_context() -> ProjectToolContext:
        return ProjectToolContext(credentials_provider=credentials_provider)

    register_tools(
        dbt_mcp,
        [tool.adapt_context(bind_context) for tool in PROJECT_TOOLS],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
