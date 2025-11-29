import logging
from collections.abc import Sequence
from dataclasses import dataclass

from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel, Field

from dbt_mcp.config.config_providers import ConfigProvider, DiscoveryConfig
from dbt_mcp.discovery.client import (
    ExposuresFetcher,
    LineageFetcher,
    MetadataAPIClient,
    ModelsFetcher,
    SourcesFetcher,
    VALID_DIRECTIONS,
    VALID_RESOURCE_TYPES,
)
from dbt_mcp.errors import InvalidParameterError
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
    lineage_fetcher: LineageFetcher

    def __init__(self, config_provider: ConfigProvider[DiscoveryConfig]):
        api_client = MetadataAPIClient(config_provider=config_provider)
        self.models_fetcher = ModelsFetcher(api_client=api_client)
        self.exposures_fetcher = ExposuresFetcher(api_client=api_client)
        self.sources_fetcher = SourcesFetcher(api_client=api_client)
        self.lineage_fetcher = LineageFetcher(api_client=api_client)


class ResourceSelection(BaseModel):
    """Schema for eliciting resource selection from user."""
    unique_id: str = Field(description="The unique ID of the selected resource")


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


@dbt_mcp_tool(
    description=get_prompt("discovery/get_lineage"),
    title="Get Lineage",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_lineage(
    context: DiscoveryToolContext,
    ctx: Context,
    name: str | None = None,
    unique_id: str | None = None,
    direction: str = "both",
    types: list[str] | None = None,
) -> dict:
    # Validate mutual exclusivity
    if name is None and unique_id is None:
        raise InvalidParameterError(
            "Either 'name' or 'unique_id' must be provided"
        )
    if name is not None and unique_id is not None:
        raise InvalidParameterError(
            "Only one of 'name' or 'unique_id' should be provided, not both"
        )

    # Validate direction
    if direction not in VALID_DIRECTIONS:
        raise InvalidParameterError(
            f"direction must be one of: {', '.join(repr(d) for d in VALID_DIRECTIONS)}"
        )

    # Validate types
    if types is not None:
        invalid = set(types) - VALID_RESOURCE_TYPES
        if invalid:
            raise InvalidParameterError(
                f"Invalid resource type(s): {invalid}. "
                f"Valid types are: {', '.join(sorted(VALID_RESOURCE_TYPES))}"
            )

    # Resolve name to unique_id if needed
    resolved_unique_id = unique_id
    if name is not None:
        matches = await context.lineage_fetcher.search_all_resources(name)
        if not matches:
            raise InvalidParameterError(
                f"No resource found with name '{name}' in searchable resource types "
                f"(models, sources, seeds, snapshots).\n\n"
                f"If this is an exposure, test, or metric, you must use the full unique_id instead:\n"
                f"  • For exposures: get_lineage(unique_id='exposure.project.{name}')\n"
                f"  • For tests: get_lineage(unique_id='test.project.{name}')\n"
                f"  • For metrics: get_lineage(unique_id='metric.project.{name}')\n\n"
                f"Note: The Discovery API does not support searching exposures, tests, or metrics by name. "
                f"You can find unique IDs in your dbt Cloud project or manifest.json."
            )
        if len(matches) == 1:
            resolved_unique_id = matches[0]["uniqueId"]
        else:
            # Multiple matches - try elicitation first, fallback to disambiguation
            try:
                # Format matches for display
                match_descriptions = [
                    f"{m['resourceType']}: {m['uniqueId']}" for m in matches
                ]
                message = (
                    f"Multiple resources found with name '{name}':\n"
                    + "\n".join(f"  {i+1}. {desc}" for i, desc in enumerate(match_descriptions))
                    + "\n\nSelect the unique_id of the resource you want:"
                )

                result = await ctx.elicit(
                    message=message,
                    schema=ResourceSelection
                )

                if result.action == "accept":
                    # Validate the selected unique_id is in matches
                    selected_id = result.data.unique_id
                    if selected_id in [m["uniqueId"] for m in matches]:
                        resolved_unique_id = selected_id
                    else:
                        raise InvalidParameterError(
                            f"Selected unique_id '{selected_id}' not in available matches"
                        )
                else:
                    # User declined or cancelled
                    return {
                        "status": "disambiguation_declined",
                        "message": f"User {result.action}ed resource selection",
                        "matches": matches,
                    }
            except Exception as e:
                # Elicitation failed, timed out, or not supported - return disambiguation response
                logger.debug(f"Elicitation not completed: {type(e).__name__}: {e}")
                return {
                    "status": "disambiguation_required",
                    "message": f"Multiple resources found with name '{name}'",
                    "matches": matches,
                    "instruction": "Please call get_lineage again with the unique_id parameter set to one of the matches above.",
                }

    return await context.lineage_fetcher.fetch_lineage(
        unique_id=resolved_unique_id,  # type: ignore[arg-type]
        direction=direction,
        types=types,
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
