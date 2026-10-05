from collections.abc import Awaitable, Callable
from typing import Annotated

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import DiscoveryConfig, MultiProjectConfigProvider
from dbt_mcp.config.config_providers.base import StaticConfigProvider
from dbt_mcp.discovery.param_descriptions import DISCOVERY_PROJECT_ID_DESCRIPTION
from dbt_mcp.discovery.tools import DISCOVERY_TOOLS, DiscoveryToolContext
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.targets import Permission, ProjectTarget
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset


def discovery_context_mapper(
    config_provider: MultiProjectConfigProvider[DiscoveryConfig],
) -> Callable[..., Awaitable[DiscoveryToolContext]]:
    async def bind_context(
        project_id: Annotated[
            int,
            ProjectTarget(requires=Permission.METADATA_READ),
            Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION),
        ],
    ) -> DiscoveryToolContext:
        config = await config_provider.get_config(project_id=project_id)
        return DiscoveryToolContext(config_provider=StaticConfigProvider(config))

    return bind_context


def register_multiproject_discovery_tools(
    dbt_mcp: FastMCP,
    config_provider: MultiProjectConfigProvider[DiscoveryConfig],
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    mapper = discovery_context_mapper(config_provider)
    register_tools(
        dbt_mcp,
        [tool.adapt_context(mapper) for tool in DISCOVERY_TOOLS],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
