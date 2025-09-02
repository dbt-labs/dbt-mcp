import logging
from collections.abc import Sequence

from mcp.server.fastmcp import FastMCP

from dbt_mcp.discovery.client import MetadataAPIClient, ModelsFetcher
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.config import DbtMcpContext
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)


def get_fetcher(ctx: DbtMcpContext) -> ModelsFetcher:
    discovery_config = ctx.get_discovery_config()
    if discovery_config is None:
        raise ValueError("Discovery config is not set")
    api_client = MetadataAPIClient(
        url=discovery_config.url,
        headers=discovery_config.headers,
    )
    models_fetcher = ModelsFetcher(
        api_client=api_client, environment_id=discovery_config.environment_id
    )
    return models_fetcher


def create_discovery_tool_definitions() -> list[ToolDefinition]:

    def get_mart_models(ctx: DbtMcpContext) -> list[dict] | str:
        try:
            models_fetcher = get_fetcher(ctx)
            mart_models = models_fetcher.fetch_models(
                model_filter={"modelingLayer": "marts"}
            )
            return [m for m in mart_models if m["name"] != "metricflow_time_spine"]
        except Exception as e:
            return str(e)

    def get_all_models(ctx: DbtMcpContext) -> list[dict] | str:
        try:
            models_fetcher = get_fetcher(ctx)
            return models_fetcher.fetch_models()
        except Exception as e:
            return str(e)

    def get_model_details(
        ctx: DbtMcpContext, model_name: str | None = None, unique_id: str | None = None
    ) -> dict | str:
        try:
            models_fetcher = get_fetcher(ctx)
            return models_fetcher.fetch_model_details(model_name, unique_id)
        except Exception as e:
            return str(e)

    def get_model_parents(
        ctx: DbtMcpContext, model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict] | str:
        try:
            models_fetcher = get_fetcher(ctx)
            return models_fetcher.fetch_model_parents(model_name, unique_id)
        except Exception as e:
            return str(e)

    def get_model_children(
        ctx: DbtMcpContext, model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict] | str:
        try:
            models_fetcher = get_fetcher(ctx)
            return models_fetcher.fetch_model_children(model_name, unique_id)
        except Exception as e:
            return str(e)

    def get_model_health(
        ctx: DbtMcpContext, model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict] | str:
        try:
            models_fetcher = get_fetcher(ctx)
            return models_fetcher.fetch_model_health(model_name, unique_id)
        except Exception as e:
            return str(e)

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
    ]


def register_discovery_tools(
    dbt_mcp: FastMCP,
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    register_tools(
        dbt_mcp,
        create_discovery_tool_definitions(),
        exclude_tools,
    )
