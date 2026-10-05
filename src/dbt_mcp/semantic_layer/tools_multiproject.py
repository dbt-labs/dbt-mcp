from collections.abc import Awaitable, Callable
from typing import Annotated

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import (
    MultiProjectConfigProvider,
    SemanticLayerConfig,
)
from dbt_mcp.config.config_providers.base import StaticConfigProvider
from dbt_mcp.semantic_layer.client import SemanticLayerClientProvider
from dbt_mcp.semantic_layer.param_descriptions import SEMANTIC_LAYER_PROJECT_ID
from dbt_mcp.semantic_layer.tools import SEMANTIC_LAYER_TOOLS, SemanticLayerToolContext
from dbt_mcp.tools.injection import BoundContext
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.targets import Permission, ProjectTarget
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset


def semantic_layer_context_mapper(
    config_provider: MultiProjectConfigProvider[SemanticLayerConfig],
    client_provider: SemanticLayerClientProvider,
) -> Callable[..., Awaitable[BoundContext[SemanticLayerToolContext]]]:
    async def bind_context(
        project_id: Annotated[
            int,
            ProjectTarget(requires=Permission.SEMANTIC_LAYER_CONFIGURATION_READ),
            Field(description=SEMANTIC_LAYER_PROJECT_ID),
        ],
    ) -> BoundContext[SemanticLayerToolContext]:
        config = await config_provider.get_config(project_id=project_id)
        return BoundContext(
            context=SemanticLayerToolContext(
                config_provider=StaticConfigProvider(config),
                client_provider=client_provider,
            ),
            arguments={"environment_id": config.prod_environment_id},
        )

    return bind_context


def register_multiproject_sl_tools(
    dbt_mcp: FastMCP,
    config_provider: MultiProjectConfigProvider[SemanticLayerConfig],
    client_provider: SemanticLayerClientProvider,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    mapper = semantic_layer_context_mapper(config_provider, client_provider)
    register_tools(
        dbt_mcp,
        [
            tool.adapt_context(mapper, bound_arguments=frozenset({"environment_id"}))
            for tool in SEMANTIC_LAYER_TOOLS
        ],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
