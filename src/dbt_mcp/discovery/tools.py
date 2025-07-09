import logging

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import DiscoveryConfig
from dbt_mcp.discovery.client import MetadataAPIClient, ModelsFetcher
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import ToolDefinition

logger = logging.getLogger(__name__)


def create_tool_definitions(config: DiscoveryConfig) -> list[ToolDefinition]:
    api_client = MetadataAPIClient(
        url=config.url,
        headers=config.headers,
    )
    models_fetcher = ModelsFetcher(
        api_client=api_client, environment_id=config.environment_id
    )

    def get_mart_models() -> list[dict] | str:
        try:
            mart_models = models_fetcher.fetch_models(
                model_filter={"modelingLayer": "marts"}
            )
            return [m for m in mart_models if m["name"] != "metricflow_time_spine"]
        except Exception as e:
            return str(e)

    def get_all_models() -> list[dict] | str:
        try:
            return models_fetcher.fetch_models()
        except Exception as e:
            return str(e)

    def get_model_details(model_name: str, unique_id: str | None = None) -> dict | str:
        try:
            return models_fetcher.fetch_model_details(model_name, unique_id)
        except Exception as e:
            return str(e)

    def get_model_parents(
        model_name: str, unique_id: str | None = None
    ) -> list[dict] | str:
        try:
            return models_fetcher.fetch_model_parents(model_name, unique_id)
        except Exception as e:
            return str(e)

    def get_model_children(
        model_name: str, unique_id: str | None = None
    ) -> list[dict] | str:
        try:
            return models_fetcher.fetch_model_children(model_name, unique_id)
        except Exception as e:
            return str(e)

    return [
        ToolDefinition(
            description=get_prompt("discovery/get_mart_models"),
            fn=get_mart_models,
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_all_models"),
            fn=get_all_models,
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_model_details"),
            fn=get_model_details,
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_model_parents"),
            fn=get_model_parents,
        ),
        ToolDefinition(
            description=get_prompt("discovery/get_model_children"),
            fn=get_model_children,
        ),
    ]


def register_discovery_tools(dbt_mcp: FastMCP, config: DiscoveryConfig) -> None:
    for tool_definition in create_tool_definitions(config):
        dbt_mcp.tool(
            tool_definition.get_name(),
            description=tool_definition.description,
        )(tool_definition.fn)
