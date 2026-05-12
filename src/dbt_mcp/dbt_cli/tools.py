import json
import os
import subprocess
from collections.abc import Iterable
from typing import Any
import logging

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config import DbtCliConfig
from dbt_mcp.dbt_cli.binary_type import BinaryType, get_color_disable_flag
from dbt_mcp.dbt_cli.models.lineage_types import ModelLineage
from dbt_mcp.dbt_cli.models.manifest import Manifest
from dbt_mcp.errors.common import InvalidParameterError
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.fields import (
    DEPTH_FIELD,
    TYPES_FIELD,
    UNIQUE_ID_REQUIRED_FIELD,
)
from dbt_mcp.tools.parameters import LineageResourceType
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)

_VALID_RESOURCE_TYPES = frozenset(
    {
        "all",
        "analysis",
        "default",
        "exposure",
        "function",
        "metric",
        "model",
        "saved_query",
        "seed",
        "semantic_model",
        "snapshot",
        "source",
        "test",
        "unit_test",
    }
)


def create_dbt_cli_tool_definitions(config: DbtCliConfig) -> list[ToolDefinition]:
    def _run_dbt_command(
        command: list[str],
        node_selection: str | None = None,
        resource_type: list[str] | None = None,
        is_selectable: bool = False,
        is_full_refresh: bool | None = False,
        vars: str | None = None,
        sample: str | None = None,
        yml_selector: str | None = None,
        state_path: str | None = None,
    ) -> str:
        try:
            # Commands that should always be quiet to reduce output verbosity
            verbose_commands = [
                "build",
                "compile",
                "docs",
                "parse",
                "run",
                "test",
                "list",
                "clone",
            ]
            if (
                # dbt CLI (Cloud CLI) does not support --state
                config.binary_type != BinaryType.DBT_CLOUD_CLI
                and state_path
                and isinstance(state_path, str)
            ):
                command.extend(["--state", state_path])
            elif state_path and config.binary_type == BinaryType.DBT_CLOUD_CLI:
                raise InvalidParameterError(
                    "--state is not supported by dbt CLI (Cloud/Platform)."
                )

            if is_full_refresh is True:
                command.append("--full-refresh")

            if sample and isinstance(sample, str):
                command.extend(["--sample", sample])

            if vars and isinstance(vars, str):
                command.extend(["--vars", vars])

            if node_selection and isinstance(node_selection, str):
                selector_params = node_selection.split()
                if any(t.startswith("-") for t in selector_params):
                    raise InvalidParameterError(
                        "node_selection contains an invalid token. Tokens must not start with '-'."
                    )
                command.extend(["--select"] + selector_params)

            if yml_selector and isinstance(yml_selector, str):
                command.extend(["--selector", yml_selector])

            if isinstance(resource_type, Iterable) and not isinstance(
                resource_type, str
            ):
                rt_list = list(resource_type)
                if invalid := [v for v in rt_list if v not in _VALID_RESOURCE_TYPES]:
                    raise InvalidParameterError(
                        f"resource_type contains invalid values: {invalid}. "
                        f"Allowed values: {sorted(_VALID_RESOURCE_TYPES)}"
                    )
                command.extend(["--resource-type"] + rt_list)

            full_command = command.copy()
            # Add --quiet flag to specific commands to reduce context window usage
            if len(full_command) > 0 and full_command[0] in verbose_commands:
                main_command = full_command[0]
                command_args = full_command[1:] if len(full_command) > 1 else []
                full_command = [main_command, "--quiet", *command_args]

            # We change the path only if this is an absolute path, otherwise we can have
            # problems with relative paths applied multiple times as DBT_PROJECT_DIR
            # is applied to dbt Core and Fusion as well (but not the dbt Cloud CLI)
            cwd_path = config.project_dir if os.path.isabs(config.project_dir) else None

            # Add appropriate color disable flag based on binary type
            color_flag = get_color_disable_flag(config.binary_type)
            args = [config.dbt_path, color_flag, *full_command]

            process = subprocess.Popen(
                args=args,
                cwd=cwd_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                text=True,
            )
            stdout, stderr = process.communicate(timeout=config.dbt_cli_timeout)
            # Use returncode as the success signal so noise on stderr (e.g.
            # urllib3 deprecation warnings under externalbrowser auth) can't
            # masquerade as the result of a successful command. On failure
            # we surface stderr too, since some dbt errors only appear there.
            if process.returncode == 0:
                return stdout or "OK"
            parts = []
            if stdout:
                parts.append(f"--- stdout ---\n{stdout.rstrip()}")
            if stderr:
                parts.append(f"--- stderr ---\n{stderr.rstrip()}")
            if parts:
                return "\n".join(parts)
            return f"Command failed with exit code {process.returncode} (no output)"
        except subprocess.TimeoutExpired:
            return "Timeout: dbt command took too long to complete." + (
                " Try using a specific selector to narrow down the results."
                if is_selectable
                else ""
            )

    def build(
        node_selection: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/node_selection")
        ),
        yml_selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/yml_selector")
        ),
        is_full_refresh: bool | None = Field(
            default=None, description=get_prompt("dbt_cli/args/full_refresh")
        ),
        vars: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/vars")
        ),
        sample: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/sample")
        ),
    ) -> str:
        return _run_dbt_command(
            ["build"],
            node_selection,
            is_selectable=True,
            is_full_refresh=is_full_refresh,
            vars=vars,
            sample=sample,
            yml_selector=yml_selector,
        )

    def compile(
        node_selection: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/node_selection")
        ),
        yml_selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/yml_selector")
        ),
    ) -> str:
        return _run_dbt_command(
            ["compile"], node_selection, is_selectable=True, yml_selector=yml_selector
        )

    def docs() -> str:
        return _run_dbt_command(["docs", "generate"])

    def ls(
        node_selection: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/node_selection")
        ),
        yml_selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/yml_selector")
        ),
        resource_type: list[str] | None = Field(
            default=None,
            description=get_prompt("dbt_cli/args/resource_type"),
        ),
    ) -> str:
        return _run_dbt_command(
            ["list"],
            node_selection,
            resource_type=resource_type,
            is_selectable=True,
            yml_selector=yml_selector,
        )

    def parse() -> str:
        return _run_dbt_command(["parse"])

    def run(
        node_selection: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/node_selection")
        ),
        yml_selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/yml_selector")
        ),
        is_full_refresh: bool | None = Field(
            default=None, description=get_prompt("dbt_cli/args/full_refresh")
        ),
        vars: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/vars")
        ),
        sample: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/sample")
        ),
    ) -> str:
        return _run_dbt_command(
            ["run"],
            node_selection,
            is_selectable=True,
            is_full_refresh=is_full_refresh,
            vars=vars,
            sample=sample,
            yml_selector=yml_selector,
        )

    def test(
        node_selection: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/node_selection")
        ),
        yml_selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/yml_selector")
        ),
        vars: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/vars")
        ),
    ) -> str:
        return _run_dbt_command(
            ["test"],
            node_selection,
            is_selectable=True,
            vars=vars,
            yml_selector=yml_selector,
        )

    def show(
        sql_query: str = Field(description=get_prompt("dbt_cli/args/sql_query")),
        limit: int = Field(default=5, description=get_prompt("dbt_cli/args/limit")),
    ) -> str:
        args = ["show", "--inline", sql_query, "--favor-state"]
        # This is quite crude, but it should be okay for now
        # until we have a dbt Fusion integration.
        cli_limit = None
        if "limit" in sql_query.lower():
            # When --limit=-1, dbt won't apply a separate limit.
            cli_limit = -1
        elif limit:
            # This can be problematic if the LLM provides
            # a SQL limit and a `limit` argument. However, preferencing the limit
            # in the SQL query leads to a better experience when the LLM
            # makes that mistake.
            cli_limit = limit
        if cli_limit is not None:
            args.extend(["--limit", str(cli_limit)])
        args.extend(["--output", "json"])
        return _run_dbt_command(args)

    def clone(
        node_selection: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/node_selection")
        ),
        yml_selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/yml_selector")
        ),
        is_full_refresh: bool | None = Field(
            default=None, description=get_prompt("dbt_cli/args/full_refresh")
        ),
        vars: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/vars")
        ),
        # Only applies to Core / Fusion CLI
        state_path: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/state_path")
        ),
    ) -> str:
        return _run_dbt_command(
            ["clone"],
            node_selection,
            is_selectable=True,
            is_full_refresh=is_full_refresh,
            vars=vars,
            yml_selector=yml_selector,
            state_path=state_path,
        )

    def _get_manifest() -> Manifest:
        """Helper function to load the dbt manifest.json file."""
        _run_dbt_command(["parse"])  # Ensure manifest is generated
        cwd_path = config.project_dir if os.path.isabs(config.project_dir) else None
        manifest_path = os.path.join(cwd_path or ".", "target", "manifest.json")
        with open(manifest_path, encoding="utf-8") as f:
            manifest_data = json.load(f)
        return Manifest(**manifest_data)

    def get_lineage_dev(
        unique_id: str = UNIQUE_ID_REQUIRED_FIELD,
        types: list[LineageResourceType] | None = TYPES_FIELD,
        depth: int = DEPTH_FIELD,
    ) -> dict[str, Any]:
        manifest = _get_manifest()
        model_lineage = ModelLineage.from_manifest(
            manifest,
            unique_id=unique_id,
            types=types,
            depth=depth,
        )
        return model_lineage.model_dump()

    def get_node_details_dev(
        node_id: str = Field(
            description=get_prompt("dbt_cli/args/node_id"),
        ),
    ) -> dict[str, Any]:
        # Comprehensive list of output keys to include all available node metadata
        output_keys = [
            # This is the entire list. Keeping it for ref and commmenting fields that are not needed.
            # Because we get the path, we should not need the raw_code and compiled_code that can be very big and that the LLM can read
            "unique_id",
            "name",
            "resource_type",
            "package_name",
            "original_file_path",
            "path",
            "alias",
            "description",
            "columns",
            "meta",
            "tags",
            "config",
            "depends_on",
            "patch_path",
            "schema",
            "database",
            "relation_name",
            # "raw_code",
            # "compiled_code",
            # "checksum",
            "language",
            "docs",
            "group",
            "access",
            "version",
            "latest_version",
            "deprecation_date",
            "contract",
            "constraints",
            "primary_key",
            "fqn",
            "build_path",
            "refs",
            "sources",
            "metrics",
            "created_at",
            "unrendered_config",
        ]
        output = _run_dbt_command(
            ["list", "--output", "json", "--output-keys", *output_keys],
            node_selection=node_id,
            is_selectable=True,
        )

        node_result: dict[str, Any] | None = None
        test_ids: list[str] = []

        for line in output.strip().split("\n"):
            if not line.strip():
                continue
            try:
                node = json.loads(line)
                resource_type = node.get("resource_type")
                if node_result is None:
                    # First node is the primary result
                    node_result = node
                elif resource_type == "test":
                    # Collect additional tests (associated with the primary node)
                    fqn = node.get("fqn", [])
                    test_ids.append(".".join(fqn) if fqn else node.get("unique_id", ""))
            except json.JSONDecodeError:
                continue

        if node_result is None:
            raise ValueError(f"No node found for selector: {node_id}")

        # Add test unique_ids for models, seeds, sources, and snapshots
        if node_result.get("resource_type") in ("model", "seed", "source", "snapshot"):
            node_result["tests"] = test_ids

        return node_result

    return [
        ToolDefinition(
            fn=build,
            description=get_prompt("dbt_cli/build"),
            annotations=create_tool_annotations(
                title="dbt build",
                read_only_hint=False,
                destructive_hint=True,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            fn=compile,
            description=get_prompt("dbt_cli/compile"),
            annotations=create_tool_annotations(
                title="dbt compile",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            fn=docs,
            description=get_prompt("dbt_cli/docs"),
            annotations=create_tool_annotations(
                title="dbt docs",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            name="list",
            fn=ls,
            description=get_prompt("dbt_cli/list"),
            annotations=create_tool_annotations(
                title="dbt list",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            fn=parse,
            description=get_prompt("dbt_cli/parse"),
            annotations=create_tool_annotations(
                title="dbt parse",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            fn=run,
            description=get_prompt("dbt_cli/run"),
            annotations=create_tool_annotations(
                title="dbt run",
                read_only_hint=False,
                destructive_hint=True,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            fn=test,
            description=get_prompt("dbt_cli/test"),
            annotations=create_tool_annotations(
                title="dbt test",
                read_only_hint=False,
                destructive_hint=True,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            fn=show,
            description=get_prompt("dbt_cli/show"),
            annotations=create_tool_annotations(
                title="dbt show",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            fn=clone,
            description=get_prompt("dbt_cli/clone"),
            annotations=create_tool_annotations(
                title="dbt clone",
                read_only_hint=False,
                destructive_hint=True,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            name="get_lineage_dev",
            fn=get_lineage_dev,
            description=get_prompt("dbt_cli/get_lineage_dev"),
            annotations=create_tool_annotations(
                title="Get Model Lineage (Dev)",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            name="get_node_details_dev",
            fn=get_node_details_dev,
            description=get_prompt("dbt_cli/get_node_details_dev"),
            annotations=create_tool_annotations(
                title="Get Node Details (Dev)",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
    ]


def register_dbt_cli_tools(
    dbt_mcp: FastMCP,
    config: DbtCliConfig,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    register_tools(
        dbt_mcp,
        tool_definitions=create_dbt_cli_tool_definitions(config),
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
