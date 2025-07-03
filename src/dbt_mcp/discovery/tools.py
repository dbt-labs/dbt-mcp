import logging
from collections.abc import Callable
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import DiscoveryConfig
from dbt_mcp.discovery.client import MetadataAPIClient, ModelsFetcher
from dbt_mcp.prompts.prompts import get_prompt

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryTool:
    description: str
    fn: Callable


def create_tools(config: DiscoveryConfig) -> dict[str, DiscoveryTool]:
    api_client = MetadataAPIClient(
        url=config.url,
        headers=config.headers,
    )
    models_fetcher = ModelsFetcher(
        api_client=api_client, environment_id=config.environment_id
    )

    def get_mart_models() -> list[dict] | str:
        mart_models = models_fetcher.fetch_models(
            model_filter={"modelingLayer": "marts"}
        )
        return [m for m in mart_models if m["name"] != "metricflow_time_spine"]

    def get_all_models() -> list[dict] | str:
        return models_fetcher.fetch_models()

    def get_model_details(model_name: str, unique_id: str | None = None) -> dict | str:
        return models_fetcher.fetch_model_details(model_name, unique_id)

    def get_model_parents(
        model_name: str, unique_id: str | None = None
    ) -> list[dict] | str:
        return models_fetcher.fetch_model_parents(model_name, unique_id)

    def get_model_children(
        model_name: str, unique_id: str | None = None
    ) -> list[dict] | str:
        return models_fetcher.fetch_model_children(model_name, unique_id)

    tools: dict[str, DiscoveryTool] = {
        "get_mart_models": DiscoveryTool(
            description=get_prompt("discovery/get_mart_models"),
            fn=get_mart_models,
        ),
        "get_all_models": DiscoveryTool(
            description=get_prompt("discovery/get_all_models"),
            fn=get_all_models,
        ),
        "get_model_details": DiscoveryTool(
            description=get_prompt("discovery/get_model_details"),
            fn=get_model_details,
        ),
        "get_model_parents": DiscoveryTool(
            description=get_prompt("discovery/get_model_parents"),
            fn=get_model_parents,
        ),
        "get_model_children": DiscoveryTool(
            description=get_prompt("discovery/get_model_children"),
            fn=get_model_children,
        ),
    }
    return tools


def register_discovery_tools(dbt_mcp: FastMCP, config: DiscoveryConfig) -> None:
    for tool_name, tool in create_tools(config).items():
        dbt_mcp.tool(tool_name, description=tool.description)(tool.fn)
