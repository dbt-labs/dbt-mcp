import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import ConfigProvider, DiscoveryConfig
from dbt_mcp.discovery.client import (
    AppliedResourceType,
    ExposuresFetcher,
    LineageFetcher,
    LineageDirection,
    LineageResourceType,
    MetadataAPIClient,
    ModelsFetcher,
    PaginatedResourceFetcher,
    ResourceDetailsFetcher,
    SourcesFetcher,
)
from dbt_mcp.errors import InvalidParameterError
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

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
    lineage_fetcher: LineageFetcher

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
        self.lineage_fetcher = LineageFetcher(api_client=api_client)


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


async def _fetch_all_lineage_trees(
    context: DiscoveryToolContext,
    matches: list[dict],
    direction: LineageDirection,
    types: list[LineageResourceType] | None,
) -> dict:
    """Fetch lineage for all matched resources in parallel.

    Args:
        context: Discovery tool context with lineage fetcher
        matches: List of matching resources with 'uniqueId' keys
        direction: Direction for lineage traversal
        types: Optional list of resource types to filter

    Returns:
        Dict with status, message, and list of lineages for each match
    """
    lineage_tasks = [
        context.lineage_fetcher.fetch_lineage(
            unique_id=match["uniqueId"],
            direction=direction,
            types=types,
        )
        for match in matches
    ]
    lineages = await asyncio.gather(*lineage_tasks)

    return {
        "status": "multiple_matches",
        "message": f"Found {len(matches)} resources. Returning lineage for all matches.",
        "lineages": [
            {
                "resource": match,
                "lineage": lineage,
            }
            for match, lineage in zip(matches, lineages)
        ],
    }


@dbt_mcp_tool(
    description=get_prompt("discovery/get_lineage"),
    title="Get Lineage",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_lineage(
    context: DiscoveryToolContext,
    name: str | None = None,
    unique_id: str | None = None,
    direction: str = "both",
    types: list[str] | None = None,
) -> dict:
    normalized_name = name.strip().lower() if name else None
    normalized_unique_id = unique_id.strip().lower() if unique_id else None

    if not normalized_name and not normalized_unique_id:
        raise InvalidParameterError("Either name or unique_id must be provided")
    if normalized_name and normalized_unique_id:
        raise InvalidParameterError(
            "Only one of name or unique_id should be provided, not both"
        )

    # Convert direction string to enum
    try:
        direction_enum = LineageDirection(direction)
    except ValueError:
        raise InvalidParameterError(
            f"Invalid direction: '{direction}'. "
            f"Must be one of: {', '.join(d.value for d in LineageDirection)}"
        )

    # Convert types strings to enums
    types_enum: list[LineageResourceType] | None = None
    if types is not None:
        types_enum = []
        for t in types:
            try:
                types_enum.append(LineageResourceType(t))
            except ValueError:
                raise InvalidParameterError(
                    f"Invalid resource type: '{t}'. "
                    f"Valid types are: {', '.join(rt.value for rt in LineageResourceType)}"
                )

    resolved_unique_id = normalized_unique_id
    if not normalized_unique_id:
        assert normalized_name is not None, "Name must be provided"
        matches = await context.lineage_fetcher.search_all_resources(normalized_name)
        if not matches:
            raise InvalidParameterError(
                f"No resource found with name '{normalized_name}' in searchable resource types "
                f"(models, sources, seeds, snapshots).\n\n"
                f"If this is an exposure, test, or metric, you must use the full unique_id instead:\n"
                f"  • For exposures: get_lineage(unique_id='exposure.project.{normalized_name}')\n"
                f"  • For tests: get_lineage(unique_id='test.project.{normalized_name}')\n"
                f"  • For metrics: get_lineage(unique_id='metric.project.{normalized_name}')\n\n"
                f"Note: The Discovery API does not support searching exposures, tests, or metrics by name. "
            )
        if len(matches) == 1:
            resolved_unique_id = matches[0]["uniqueId"]
        else:
            return await _fetch_all_lineage_trees(context, matches, direction_enum, types_enum)

    assert resolved_unique_id is not None

    return await context.lineage_fetcher.fetch_lineage(
        unique_id=resolved_unique_id,
        direction=direction_enum,
        types=types_enum,
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
    get_lineage,
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