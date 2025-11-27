import logging
from collections.abc import Sequence
from dataclasses import dataclass

import mcp.types as mcp_types
from mcp.server.elicitation import (
    AcceptedElicitation,
    DeclinedElicitation,
)
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


class ResourceSelection(BaseModel):
    """Schema for user to select from multiple matching resources."""

    unique_id: str = Field(description="The unique_id of the selected resource")


def create_discovery_tool_definitions(
    config_provider: ConfigProvider[DiscoveryConfig],
) -> list[ToolDefinition]:
    api_client = MetadataAPIClient(config_provider=config_provider)
    models_fetcher = ModelsFetcher(api_client=api_client)
    exposures_fetcher = ExposuresFetcher(api_client=api_client)
    sources_fetcher = SourcesFetcher(api_client=api_client)
    lineage_fetcher = LineageFetcher(api_client=api_client)

    async def get_lineage(
        name: str | None = None,
        unique_id: str | None = None,
        direction: str = "both",
        types: list[str] | None = None,
        ctx: Context = None,  # type: ignore[assignment]
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
            matches = await lineage_fetcher.search_all_resources(name)
            if not matches:
                raise InvalidParameterError(f"No resource found with name '{name}'")
            if len(matches) == 1:
                resolved_unique_id = matches[0]["uniqueId"]
            else:
                # Multiple matches - check if client supports elicitation
                has_elicitation = False
                if ctx is not None:
                    has_elicitation = (
                        ctx.request_context.session.check_client_capability(
                            mcp_types.ClientCapabilities(elicitation={})
                        )
                    )

                if ctx is not None and has_elicitation:
                    try:
                        # Build user-friendly message
                        options_text = "\n".join(
                            f"  • {m['uniqueId']} ({m['resourceType']})"
                            for m in matches
                        )
                        message = (
                            f"Multiple resources found with name '{name}':\n\n"
                            f"{options_text}\n\n"
                            "Enter the unique_id of the resource you want:"
                        )

                        result = await ctx.elicit(
                            message=message,
                            schema=ResourceSelection,
                        )

                        if isinstance(result, AcceptedElicitation):
                            # Validate the selection exists in matches
                            selected_id = result.data.unique_id
                            if selected_id not in [m["uniqueId"] for m in matches]:
                                raise InvalidParameterError(
                                    f"Invalid selection '{selected_id}'. "
                                    f"Must be one of: {', '.join(m['uniqueId'] for m in matches)}"
                                )
                            resolved_unique_id = selected_id
                        elif isinstance(result, DeclinedElicitation):
                            raise InvalidParameterError(
                                "User declined to select a resource"
                            )
                        else:  # CancelledElicitation
                            raise InvalidParameterError("Operation cancelled by user")
                    except InvalidParameterError:
                        raise  # Re-raise our own errors (declined/cancelled/invalid)
                    except Exception:
                        # Elicitation failed (timeout, unsupported, etc.)
                        # Return helpful response instead of error
                        return {
                            "status": "disambiguation_required",
                            "message": f"Multiple resources found with name '{name}'",
                            "matches": matches,
                            "instruction": "Please call get_lineage again with the unique_id parameter set to one of the matches above.",
                        }
                else:
                    # No context or client doesn't support elicitation
                    # Return helpful response instead of error
                    return {
                        "status": "disambiguation_required",
                        "message": f"Multiple resources found with name '{name}'",
                        "matches": matches,
                        "instruction": "Please call get_lineage again with the unique_id parameter set to one of the matches above.",
                    }

        return await lineage_fetcher.fetch_lineage(
            unique_id=resolved_unique_id,  # type: ignore[arg-type]
            direction=direction,
            types=types,
        )

    async def get_mart_models() -> list[dict]:
        mart_models = await models_fetcher.fetch_models(
            model_filter={"modelingLayer": "marts"}
        )
        return [m for m in mart_models if m["name"] != "metricflow_time_spine"]

    async def get_all_models() -> list[dict]:
        return await models_fetcher.fetch_models()

    async def get_model_details(
        model_name: str | None = None, unique_id: str | None = None
    ) -> dict:
        return await models_fetcher.fetch_model_details(model_name, unique_id)

    async def get_model_parents(
        model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict]:
        return await models_fetcher.fetch_model_parents(model_name, unique_id)

    async def get_model_children(
        model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict]:
        return await models_fetcher.fetch_model_children(model_name, unique_id)

    async def get_model_health(
        model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict]:
        return await models_fetcher.fetch_model_health(model_name, unique_id)

    async def get_exposures() -> list[dict]:
        return await exposures_fetcher.fetch_exposures()

    async def get_exposure_details(
        exposure_name: str | None = None, unique_ids: list[str] | None = None
    ) -> list[dict]:
        return await exposures_fetcher.fetch_exposure_details(exposure_name, unique_ids)

    async def get_all_sources(
        source_names: list[str] | None = None,
        unique_ids: list[str] | None = None,
    ) -> list[dict]:
        return await sources_fetcher.fetch_sources(source_names, unique_ids)

    async def get_source_details(
        source_name: str | None = None, unique_id: str | None = None
    ) -> dict:
        return await sources_fetcher.fetch_source_details(source_name, unique_id)

    return [
        ToolDefinition(
            description=get_prompt("discovery/get_lineage"),
            fn=get_lineage,
            annotations=create_tool_annotations(
                title="Get Lineage",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_mart_models"),
            fn=get_mart_models,
            annotations=create_tool_annotations(
                title="Get Mart Models",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_all_models"),
            fn=get_all_models,
            annotations=create_tool_annotations(
                title="Get All Models",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_model_details"),
            fn=get_model_details,
            annotations=create_tool_annotations(
                title="Get Model Details",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_model_parents"),
            fn=get_model_parents,
            annotations=create_tool_annotations(
                title="Get Model Parents",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_model_children"),
            fn=get_model_children,
            annotations=create_tool_annotations(
                title="Get Model Children",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_model_health"),
            fn=get_model_health,
            annotations=create_tool_annotations(
                title="Get Model Health",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_exposures"),
            fn=get_exposures,
            annotations=create_tool_annotations(
                title="Get Exposures",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_exposure_details"),
            fn=get_exposure_details,
            annotations=create_tool_annotations(
                title="Get Exposure Details",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_all_sources"),
            fn=get_all_sources,
            annotations=create_tool_annotations(
                title="Get All Sources",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_source_details"),
            fn=get_source_details,
            annotations=create_tool_annotations(
                title="Get Source Details",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
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
