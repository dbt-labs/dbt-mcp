import json
import logging
import os
import subprocess
from collections.abc import Sequence
from typing import Any

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import (
    ConfigProvider,
    DiscoveryConfig,
)
from dbt_mcp.discovery.client import (
    ExposuresFetcher,
    MetadataAPIClient,
    ModelsFetcher,
    SourcesFetcher,
)
from dbt_mcp.config.config import DbtCliConfig
from dbt_mcp.dbt_cli.binary_type import get_color_disable_flag
from dbt_mcp.discovery.models.lineage_types import ModelLineage
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)


def _get_manifest_from_cli(config: DbtCliConfig) -> dict[str, Any]:
    """Run `dbt parse` and load target/manifest.json from the dbt project dir."""
    try:
        cwd_path = config.project_dir if os.path.isabs(config.project_dir) else None
        color_flag = get_color_disable_flag(config.binary_type)
        args = [config.dbt_path, color_flag, "parse"]
        subprocess.run(args, cwd=cwd_path, check=False, stdout=subprocess.DEVNULL)
        manifest_path = os.path.join(cwd_path or ".", "target", "manifest.json")
        with open(manifest_path) as f:
            return json.loads(f.read())
    except FileNotFoundError:
        raise FileNotFoundError(
            f"manifest.json not found at {manifest_path}; run `dbt parse` to generate it"
        )


def create_discovery_tool_definitions(
    config_provider: ConfigProvider[DiscoveryConfig] | None,
    dbt_cli_config: DbtCliConfig | None = None,
) -> list[ToolDefinition]:
    """
    Create discovery tool definitions.

    If a platform discovery `config_provider` is present, use the platform
    implementations for all discovery tools. If not, but a `dbt_cli_config` is
    provided, register only the subset of discovery tools that can be backed by
    a dbt CLI manifest (parents/children lineage).
    """
    # If we have platform discovery, wire the API client based implementations
    if config_provider:
        api_client = MetadataAPIClient(config_provider=config_provider)
        models_fetcher = ModelsFetcher(api_client=api_client)
        exposures_fetcher = ExposuresFetcher(api_client=api_client)
        sources_fetcher = SourcesFetcher(api_client=api_client)

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

        async def get_all_sources(
            source_names: list[str] | None = None,
            unique_ids: list[str] | None = None,
        ) -> list[dict]:
            return await sources_fetcher.fetch_sources(source_names, unique_ids)

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
            return await exposures_fetcher.fetch_exposure_details(
                exposure_name, unique_ids
            )

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
        ]

    # If we don't have platform discovery but do have dbt CLI, only register
    # the subset of discovery tools we can back with the CLI (parents/children).
    if dbt_cli_config:

        async def get_model_parents_cli(
            model_name: str | None = None, unique_id: str | None = None
        ) -> list[dict]:
            manifest = _get_manifest_from_cli(dbt_cli_config)
            # Determine model_id from unique_id or model_name
            model_id = unique_id
            if not model_id and model_name:
                # find node with matching name
                for uid, node in (manifest.get("nodes", {}) | {}).items():
                    if node.get("name") == model_name:
                        model_id = uid
                        break
            if not model_id:
                raise ValueError(
                    "model not found in manifest: provide unique_id or model_name"
                )

            ml = ModelLineage.from_manifest(
                manifest, model_id, recursive=True, direction="parents"
            )
            return [a.model_dump() for a in ml.parents]

        async def get_model_children_cli(
            model_name: str | None = None, unique_id: str | None = None
        ) -> list[dict]:
            manifest = _get_manifest_from_cli(dbt_cli_config)
            model_id = unique_id
            if not model_id and model_name:
                for uid, node in (manifest.get("nodes", {}) | {}).items():
                    if node.get("name") == model_name:
                        model_id = uid
                        break
            if not model_id:
                raise ValueError(
                    "model not found in manifest: provide unique_id or model_name"
                )

            ml = ModelLineage.from_manifest(
                manifest, model_id, recursive=True, direction="children"
            )
            return [d.model_dump() for d in ml.children]

        return [
            ToolDefinition(
                description=get_prompt("discovery/get_model_parents"),
                fn=get_model_parents_cli,
                annotations=create_tool_annotations(
                    title="Get Model Parents (dbt CLI)",
                    read_only_hint=True,
                    destructive_hint=False,
                    idempotent_hint=True,
                ),
            ),
            ToolDefinition(
                description=get_prompt("discovery/get_model_children"),
                fn=get_model_children_cli,
                annotations=create_tool_annotations(
                    title="Get Model Children (dbt CLI)",
                    read_only_hint=True,
                    destructive_hint=False,
                    idempotent_hint=True,
                ),
            ),
        ]

    # No discovery provider and no dbt CLI available -> no discovery tools
    return []


def register_discovery_tools(
    dbt_mcp: FastMCP,
    config_provider: ConfigProvider[DiscoveryConfig] | None,
    dbt_cli_config: DbtCliConfig | None = None,
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    register_tools(
        dbt_mcp,
        create_discovery_tool_definitions(config_provider, dbt_cli_config),
        exclude_tools,
    )
