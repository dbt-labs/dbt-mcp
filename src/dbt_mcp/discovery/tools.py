import logging

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import Config
from dbt_mcp.discovery.client import MetadataAPIClient, ModelsFetcher
from dbt_mcp.prompts.prompts import get_prompt

logger = logging.getLogger(__name__)


def register_discovery_tools(dbt_mcp: FastMCP, config: Config) -> None:
    if not config.host or not config.token or not config.prod_environment_id:
        raise ValueError(
            "Host, token, and environment ID are required to use discovery tools. To disable discovery tools, set DISABLE_DISCOVERY=true in your environment."
        )
    api_client = MetadataAPIClient(
        host=config.host,
        token=config.token,
        multicell_account_prefix=config.multicell_account_prefix,
    )
    models_fetcher = ModelsFetcher(
        api_client=api_client, environment_id=config.prod_environment_id
    )

    @dbt_mcp.tool(description=get_prompt("discovery/get_mart_models"))
    def get_mart_models() -> list[dict] | str:
        try:
            mart_models = models_fetcher.fetch_models(
                model_filter={"modelingLayer": "marts"}
            )
            return [m for m in mart_models if m["name"] != "metricflow_time_spine"]
        except Exception as e:
            logger.error(f"Error getting mart models: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("discovery/get_all_models"))
    def get_all_models() -> list[dict] | str:
        try:
            return models_fetcher.fetch_models()
        except Exception as e:
            logger.error(f"Error getting all models: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("discovery/get_model_details"))
    def get_model_details(model_name: str, unique_id: str | None = None) -> dict | str:
        try:
            return models_fetcher.fetch_model_details(model_name, unique_id)
        except Exception as e:
            logger.error(f"Error getting model details: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("discovery/get_model_parents"))
    def get_model_parents(
        model_name: str, unique_id: str | None = None
    ) -> list[dict] | str:
        try:
            return models_fetcher.fetch_model_parents(model_name, unique_id)
        except Exception as e:
            logger.error(f"Error getting model parents: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("discovery/get_model_children"))
    def get_model_children(
        model_name: str, unique_id: str | None = None
    ) -> list[dict] | str:
        try:
            return models_fetcher.fetch_model_children(model_name, unique_id)
        except Exception as e:
            logger.error(f"Error getting model children: {e}")
            return str(e)
