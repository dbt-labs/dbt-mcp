import logging
from collections.abc import Sequence

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import (
    ConfigProvider,
    DiscoveryConfig,
)
from dbt_mcp.discovery.client import ExposuresFetcher, MetadataAPIClient, ModelsFetcher
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)


def create_discovery_tool_definitions(
    config_provider: ConfigProvider[DiscoveryConfig],
) -> list[ToolDefinition]:
    api_client = MetadataAPIClient(config_provider=config_provider)
    models_fetcher = ModelsFetcher(api_client=api_client)
    exposures_fetcher = ExposuresFetcher(api_client=api_client)

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

    return [
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
    ]


def register_discovery_tools(
    dbt_mcp: FastMCP,
    config_provider: ConfigProvider[DiscoveryConfig],
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    register_tools(
        dbt_mcp,
        create_discovery_tool_definitions(config_provider),
        exclude_tools,
    )
