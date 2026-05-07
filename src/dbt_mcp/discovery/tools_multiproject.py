import logging
from dataclasses import dataclass
from typing import Annotated

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import (
    DiscoveryConfig,
    MultiProjectConfigProvider,
    MultiProjectDiscoveryConfigProvider,
)
from dbt_mcp.discovery.client import (
    AppliedResourceType,
    ExposuresFetcher,
    LineageFetcher,
    MacrosFetcher,
    ModelPerformanceFetcher,
    ModelsFetcher,
    PaginatedResourceFetcher,
    ResourceDetailsFetcher,
    SourcesFetcher,
)
from dbt_mcp.discovery.param_descriptions import (
    DISCOVERY_PROJECT_ID_DESCRIPTION,
    MACRO_INCLUDE_DEFAULT_DBT_PACKAGES,
    MACRO_PACKAGE_NAMES,
    MACRO_RETURN_PACKAGE_NAMES_ONLY,
    MODEL_PERF_INCLUDE_TESTS,
    MODEL_PERF_NUM_RUNS,
    SOURCE_NAMES_FILTER,
    SOURCE_UNIQUE_IDS_FILTER,
)
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.fields import (
    DEPTH_FIELD,
    NAME_FIELD,
    TYPES_FIELD,
    UNIQUE_ID_FIELD,
    UNIQUE_ID_REQUIRED_FIELD,
)
from dbt_mcp.tools.parameters import LineageResourceType
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)


@dataclass
class MultiProjectDiscoveryToolContext:
    config_provider: MultiProjectConfigProvider[DiscoveryConfig]
    models_fetcher: ModelsFetcher
    exposures_fetcher: ExposuresFetcher
    sources_fetcher: SourcesFetcher
    macros_fetcher: MacrosFetcher
    resource_details_fetcher: ResourceDetailsFetcher
    lineage_fetcher: LineageFetcher
    model_performance_fetcher: ModelPerformanceFetcher

    def __init__(
        self,
        *,
        config_provider: MultiProjectConfigProvider[DiscoveryConfig],
    ):
        self.config_provider = config_provider
        self.models_fetcher = ModelsFetcher(
            paginator=PaginatedResourceFetcher(
                edges_path=("data", "environment", "applied", "models", "edges"),
                page_info_path=("data", "environment", "applied", "models", "pageInfo"),
            ),
        )
        self.exposures_fetcher = ExposuresFetcher(
            paginator=PaginatedResourceFetcher(
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
            paginator=PaginatedResourceFetcher(
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
        self.macros_fetcher = MacrosFetcher(
            paginator=PaginatedResourceFetcher(
                edges_path=("data", "environment", "applied", "resources", "edges"),
                page_info_path=(
                    "data",
                    "environment",
                    "applied",
                    "resources",
                    "pageInfo",
                ),
            ),
        )
        self.resource_details_fetcher = ResourceDetailsFetcher()
        self.lineage_fetcher = LineageFetcher()
        self.model_performance_fetcher = ModelPerformanceFetcher(
            resource_details_fetcher=self.resource_details_fetcher,
        )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_mart_models"),
    title="Get Mart Models",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_mart_models(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    mart_models = await context.models_fetcher.fetch_models(
        model_filter={"modelingLayer": "marts"},
        config=config,
    )
    return [m for m in mart_models if m["name"] != "metricflow_time_spine"]


@dbt_mcp_tool(
    description=get_prompt("discovery/get_all_models"),
    title="Get All Models",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_all_models(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
) -> list[dict]:
    # TODO: push code into fetchers
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.models_fetcher.fetch_models(config=config)


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_details"),
    title="Get Model Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_details(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.MODEL,
        unique_id=unique_id,
        name=name,
        config=config,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_parents"),
    title="Get Model Parents",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_parents(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.models_fetcher.fetch_model_parents(
        name, unique_id, config=config
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_children"),
    title="Get Model Children",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_children(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.models_fetcher.fetch_model_children(
        model_name=name, unique_id=unique_id, config=config
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_health"),
    title="Get Model Health",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_health(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.models_fetcher.fetch_model_health(
        model_name=name,
        unique_id=unique_id,
        config=config,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_model_performance"),
    title="Get Model Performance",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_performance(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
    num_runs: int = Field(
        default=1,
        description=MODEL_PERF_NUM_RUNS,
        ge=1,
        le=100,
    ),
    include_tests: bool = Field(
        default=False,
        description=MODEL_PERF_INCLUDE_TESTS,
    ),
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.model_performance_fetcher.fetch_performance(
        name=name,
        unique_id=unique_id,
        num_runs=num_runs,
        include_tests=include_tests,
        config=config,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_lineage"),
    title="Get Lineage",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_lineage(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    unique_id: str = UNIQUE_ID_REQUIRED_FIELD,
    types: list[LineageResourceType] | None = TYPES_FIELD,
    depth: int = DEPTH_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.lineage_fetcher.fetch_lineage(
        unique_id=unique_id, types=types, depth=depth, config=config
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_exposures"),
    title="Get Exposures",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_exposures(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.exposures_fetcher.fetch_exposures(config=config)


@dbt_mcp_tool(
    description=get_prompt("discovery/get_exposure_details"),
    title="Get Exposure Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_exposure_details(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.EXPOSURE,
        unique_id=unique_id,
        name=name,
        config=config,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_all_sources"),
    title="Get All Sources",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_all_sources(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    source_names: Annotated[
        list[str] | None, Field(description=SOURCE_NAMES_FILTER)
    ] = None,
    unique_ids: Annotated[
        list[str] | None, Field(description=SOURCE_UNIQUE_IDS_FILTER)
    ] = None,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.sources_fetcher.fetch_sources(
        source_names, unique_ids, config=config
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_source_details"),
    title="Get Source Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_source_details(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.SOURCE,
        unique_id=unique_id,
        name=name,
        config=config,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_all_macros"),
    title="Get All Macros",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_all_macros(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    package_names: Annotated[
        list[str] | None, Field(description=MACRO_PACKAGE_NAMES)
    ] = None,
    return_package_names_only: Annotated[
        bool, Field(description=MACRO_RETURN_PACKAGE_NAMES_ONLY)
    ] = False,
    include_default_dbt_packages: Annotated[
        bool, Field(description=MACRO_INCLUDE_DEFAULT_DBT_PACKAGES)
    ] = False,
) -> list[dict] | list[str]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.macros_fetcher.fetch_macros(
        package_names=package_names,
        return_package_names_only=return_package_names_only,
        include_default_dbt_packages=include_default_dbt_packages,
        config=config,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_macro_details"),
    title="Get Macro Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_macro_details(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.MACRO,
        unique_id=unique_id,
        name=name,
        config=config,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_seed_details"),
    title="Get Seed Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_seed_details(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.SEED,
        unique_id=unique_id,
        name=name,
        config=config,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_semantic_model_details"),
    title="Get Semantic Model Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_semantic_model_details(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.SEMANTIC_MODEL,
        unique_id=unique_id,
        name=name,
        config=config,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_snapshot_details"),
    title="Get Snapshot Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_snapshot_details(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.SNAPSHOT,
        unique_id=unique_id,
        name=name,
        config=config,
    )


@dbt_mcp_tool(
    description=get_prompt("discovery/get_test_details"),
    title="Get Test Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_test_details(
    context: MultiProjectDiscoveryToolContext,
    project_id: Annotated[int, Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)],
    name: str | None = NAME_FIELD,
    unique_id: str | None = UNIQUE_ID_FIELD,
) -> list[dict]:
    config = await context.config_provider.get_config(project_id=project_id)
    return await context.resource_details_fetcher.fetch_details(
        resource_type=AppliedResourceType.TEST,
        unique_id=unique_id,
        name=name,
        config=config,
    )


MULTIPROJECT_DISCOVERY_TOOLS = [
    get_mart_models,
    get_all_models,
    get_model_details,
    get_model_parents,
    get_model_children,
    get_model_health,
    get_model_performance,
    get_lineage,
    get_exposures,
    get_exposure_details,
    get_all_sources,
    get_source_details,
    get_all_macros,
    get_macro_details,
    get_seed_details,
    get_semantic_model_details,
    get_snapshot_details,
    get_test_details,
]


def register_multiproject_discovery_tools(
    dbt_mcp: FastMCP,
    config_provider: MultiProjectDiscoveryConfigProvider,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    def bind_context() -> MultiProjectDiscoveryToolContext:
        return MultiProjectDiscoveryToolContext(
            config_provider=config_provider,
        )

    register_tools(
        dbt_mcp,
        tool_definitions=[
            tool.adapt_context(bind_context) for tool in MULTIPROJECT_DISCOVERY_TOOLS
        ],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
