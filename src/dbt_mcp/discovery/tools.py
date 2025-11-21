import logging
from collections.abc import Sequence
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import ConfigProvider, DiscoveryConfig
from dbt_mcp.discovery.client import (
    ExposuresFetcher,
    MetadataAPIClient,
    ModelsFetcher,
    PaginatedResourceFetcher,
    SourcesFetcher,
)
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryToolContext:
    models_fetcher: ModelsFetcher
    exposures_fetcher: ExposuresFetcher
    sources_fetcher: SourcesFetcher

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
    model_name: str | None = None,
    unique_id: str | None = None,
) -> dict:
    return await context.models_fetcher.fetch_model_details(model_name, unique_id)


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_parents"),
    title="Get Model Parents",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_parents(
    context: DiscoveryToolContext,
    model_name: str | None = None,
    unique_id: str | None = None,
) -> list[dict]:
    return await context.models_fetcher.fetch_model_parents(model_name, unique_id)


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_children"),
    title="Get Model Children",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_children(
    context: DiscoveryToolContext,
    model_name: str | None = None,
    unique_id: str | None = None,
) -> list[dict]:
    return await context.models_fetcher.fetch_model_children(model_name, unique_id)


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_health"),
    title="Get Model Health",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_health(
    context: DiscoveryToolContext,
    model_name: str | None = None,
    unique_id: str | None = None,
) -> list[dict]:
    return await context.models_fetcher.fetch_model_health(model_name, unique_id)


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
    exposure_name: str | None = None,
    unique_ids: list[str] | None = None,
) -> list[dict]:
    return await context.exposures_fetcher.fetch_exposure_details(
        exposure_name, unique_ids
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
    source_name: str | None = None,
    unique_id: str | None = None,
) -> dict:
    return await context.sources_fetcher.fetch_source_details(source_name, unique_id)


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
]


def register_discovery_tools(
    dbt_mcp: FastMCP,
    discovery_config_provider: ConfigProvider[DiscoveryConfig],
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    def bind_context() -> DiscoveryToolContext:
        return DiscoveryToolContext(config_provider=discovery_config_provider)

    register_tools(
        dbt_mcp,
        [tool.adapt_context(bind_context) for tool in DISCOVERY_TOOLS],
        exclude_tools,
    )
