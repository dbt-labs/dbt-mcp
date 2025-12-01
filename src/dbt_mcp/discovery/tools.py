import logging
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import ConfigProvider, DiscoveryConfig
from dbt_mcp.discovery.client import (
    AppliedResourceType,
    ExposuresFetcher,
    MetadataAPIClient,
    ModelsFetcher,
    PaginatedResourceFetcher,
    ResourceDetailsFetcher,
    SourcesFetcher,
)
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)

UNIQUE_ID_FIELD = Field(
    default=None,
    description="Fully-qualified unique ID of the resource. "
    "This will follow the format `<resource_type>.<package_name>.<resource_name>` "
    "(e.g. `model.analytics.stg_orders`). "
    "Strongly preferred over the `name` parameter for deterministic lookups.",
)
NAME_FIELD = Field(
    default=None,
    description="The name of the resource. "
    "This is not required if `unique_id` is provided. "
    "Only use name when `unique_id` is unknown.",
)


@dataclass
class DiscoveryToolContext:
    models_fetcher: ModelsFetcher
    exposures_fetcher: ExposuresFetcher
    sources_fetcher: SourcesFetcher
    resource_details_fetcher: ResourceDetailsFetcher

    def __init__(self, config_provider: ConfigProvider[DiscoveryConfig]):
        api_client = MetadataAPIClient(config_provider=config_provider)
        self.models_fetcher = ModelsFetcher(
            api_client=api_client,
            paginator=PaginatedResourceFetcher(
                api_client=api_client,
                edges_path=("data", "environment", "applied", "models", "edges"),
                page_info_path=("data", "environment", "applied", "models", "pageInfo"),
            ),
        )
        self.exposures_fetcher = ExposuresFetcher(
            api_client=api_client,
            paginator=PaginatedResourceFetcher(
                api_client=api_client,
                edges_path=("data", "environment", "definition", "exposures", "edges"),
                page_info_path=(
                    "data",
                    "environment",
                    "definition",
                    "exposures",
                    "pageInfo",
                ),
            ),
        )
        self.sources_fetcher = SourcesFetcher(
            api_client=api_client,
            paginator=PaginatedResourceFetcher(
                api_client,
                edges_path=("data", "environment", "applied", "sources", "edges"),
                page_info_path=(
                    "data",
                    "environment",
                    "applied",
                    "sources",
                    "pageInfo",
                ),
            ),
        )
        self.resource_details_fetcher = ResourceDetailsFetcher(api_client=api_client)


@dbt_mcp_tool(
    description=get_prompt("discovery/get_mart_models"),
    title="Get Mart Models",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_mart_models(context: DiscoveryToolContext) -> list[dict]:
    mart_models = await context.models_fetcher.fetch_models(
        model_filter={"modelingLayer": "marts"}
    )
    return [m for m in mart_models if m["name"] != "metricflow_time_spine"]


@dbt_mcp_tool(
    description=get_prompt("discovery/get_all_models"),
    title="Get All Models",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_all_models(context: DiscoveryToolContext) -> list[dict]:
    return await context.models_fetcher.fetch_models()


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_details"),
    title="Get Model Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_details(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.MODEL,
        unique_id=unique_id,
        name=name,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_parents"),
    title="Get Model Parents",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_parents(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.models_fetcher.fetch_model_parents(name, unique_id)


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_children"),
    title="Get Model Children",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_children(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.models_fetcher.fetch_model_children(name, unique_id)


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_health"),
    title="Get Model Health",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_health(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.models_fetcher.fetch_model_health(name, unique_id)


@dbt_mcp_tool(
    description=get_prompt("discovery/get_exposures"),
    title="Get Exposures",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_exposures(context: DiscoveryToolContext) -> list[dict]:
    return await context.exposures_fetcher.fetch_exposures()


@dbt_mcp_tool(
    description=get_prompt("discovery/get_exposure_details"),
    title="Get Exposure Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_exposure_details(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.EXPOSURE,
        unique_id=unique_id,
        name=name,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_all_sources"),
    title="Get All Sources",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_all_sources(
    context: DiscoveryToolContext,
    source_names: list[str] | None = None,
    unique_ids: list[str] | None = None,
) -> list[dict]:
    return await context.sources_fetcher.fetch_sources(source_names, unique_ids)


@dbt_mcp_tool(
    description=get_prompt("discovery/get_source_details"),
    title="Get Source Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_source_details(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.SOURCE,
        unique_id=unique_id,
        name=name,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_macro_details"),
    title="Get Macro Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_macro_details(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.MACRO,
        unique_id=unique_id,
        name=name,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_seed_details"),
    title="Get Seed Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_seed_details(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.SEED,
        unique_id=unique_id,
        name=name,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_semantic_model_details"),
    title="Get Semantic Model Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_semantic_model_details(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.SEMANTIC_MODEL,
        unique_id=unique_id,
        name=name,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_snapshot_details"),
    title="Get Snapshot Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_snapshot_details(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.SNAPSHOT,
        unique_id=unique_id,
        name=name,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_test_details"),
    title="Get Test Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_test_details(
    context: DiscoveryToolContext,
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.TEST,
        unique_id=unique_id,
        name=name,
    )


DISCOVERY_TOOLS = [
    get_mart_models,
    get_all_models,
    get_model_details,
    get_model_parents,
    get_model_children,
    get_model_health,
    get_exposures,
    get_exposure_details,
    get_all_sources,
    get_source_details,
    get_macro_details,
    get_seed_details,
    get_semantic_model_details,
    get_snapshot_details,
    get_test_details,
]


def register_discovery_tools(
    dbt_mcp: FastMCP,
    discovery_config_provider: ConfigProvider[DiscoveryConfig],
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName],
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    def bind_context() -> DiscoveryToolContext:
        return DiscoveryToolContext(config_provider=discovery_config_provider)

    register_tools(
        dbt_mcp,
        tool_definitions=[tool.adapt_context(bind_context) for tool in DISCOVERY_TOOLS],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
