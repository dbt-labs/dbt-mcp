import json
import os
import subprocess
from pathlib import Path

import yaml
from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config import DbtCliConfig
from dbt_mcp.prompts.prompts import get_prompt


def register_dbt_cli_tools(dbt_mcp: FastMCP, config: DbtCliConfig) -> None:
    def _find_profiles_yml() -> Path | None:
        """Find the profiles.yml file in the standard dbt locations."""
        # Check project directory first
        project_profiles = Path(config.project_dir) / "profiles.yml"
        if project_profiles.exists():
            return project_profiles

        # Check ~/.dbt/profiles.yml
        home_profiles = Path.home() / ".dbt" / "profiles.yml"
        if home_profiles.exists():
            return home_profiles

        # Check DBT_PROFILES_DIR environment variable
        profiles_dir = os.environ.get("DBT_PROFILES_DIR")
        if profiles_dir:
            env_profiles = Path(profiles_dir) / "profiles.yml"
            if env_profiles.exists():
                return env_profiles

        return None

    def _run_dbt_command(
        command: list[str],
        selector: str | None = None,
        target: str | None = None,
        timeout: int | None = None,
    ) -> str:
        # Commands that should always be quiet to reduce output verbosity
        verbose_commands = ["build", "compile", "docs", "parse", "run", "test"]

        if selector:
            selector_params = str(selector).split(" ")
            command = command + ["--select"] + selector_params

        if target:
            target_params = str(target).split(" ")
            command = command + ["--target"] + target_params

        full_command = command.copy()
        # Add --quiet flag to specific commands to reduce context window usage
        if len(full_command) > 0 and full_command[0] in verbose_commands:
            main_command = full_command[0]
            command_args = full_command[1:] if len(full_command) > 1 else []
            full_command = [main_command, "--quiet", *command_args]

        # Make the format json to make it easier to parse for the LLM
        full_command = full_command + ["--log-format", "json"]

        process = subprocess.Popen(
            args=[config.dbt_path, *full_command],
            cwd=config.project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output, _ = process.communicate(timeout=timeout)
        return output or "OK"

    @dbt_mcp.tool(description=get_prompt("dbt_cli/build"))
    def build(
        selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
        target: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/target")
        ),
    ) -> str:
        return _run_dbt_command(["build"], selector, target)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/compile"))
    def compile(
        selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
        target: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/target")
        ),
    ) -> str:
        return _run_dbt_command(["compile"], selector, target)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/docs"))
    def docs(
        target: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/target")
        ),
    ) -> str:
        return _run_dbt_command(["docs", "generate"], target)

    @dbt_mcp.tool(name="list", description=get_prompt("dbt_cli/list"))
    def ls(
        selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
        target: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/target")
        ),
    ) -> str:
        try:
            return _run_dbt_command(["list"], selector, target, timeout=10)
        except subprocess.TimeoutExpired:
            return (
                "Timeout: dbt list command took too long to complete. "
                + "Try using a more specific selector to narrow down the list of models."
            )

    @dbt_mcp.tool(description=get_prompt("dbt_cli/parse"))
    def parse(
        target: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/target")
        ),
    ) -> str:
        return _run_dbt_command(["parse"], target)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/run"))
    def run(
        selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
        target: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/target")
        ),
    ) -> str:
        return _run_dbt_command(["run"], selector, target)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/test"))
    def test(
        selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
        target: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/target")
        ),
    ) -> str:
        return _run_dbt_command(["test"], selector, target)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/show"))
    def show(
        sql_query: str = Field(description=get_prompt("dbt_cli/args/sql_query")),
        limit: int | None = Field(
            default=None, description=get_prompt("dbt_cli/args/limit")
        ),
        target: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/target")
        ),
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
        return _run_dbt_command(args, target)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/get_profiles"))
    def get_profiles() -> str:
        """Parse profiles.yml and return available target names."""
        profiles_path = _find_profiles_yml()
        if not profiles_path:
            return "Error: Could not find profiles.yml file in project directory, ~/.dbt/, or DBT_PROFILES_DIR"

        try:
            with open(profiles_path, "r") as f:
                profiles_data = yaml.safe_load(f)

            if not profiles_data:
                return "Error: profiles.yml file is empty or invalid"

            # Extract all profiles and their targets
            result = {"profiles_file": str(profiles_path), "profiles": {}}

            for profile_name, profile_config in profiles_data.items():
                if isinstance(profile_config, dict) and "outputs" in profile_config:
                    targets = {}
                    for target_name, target_config in profile_config["outputs"].items():
                        target_info = {"name": target_name}

                        # Extract database type (adapter type)
                        if "type" in target_config:
                            target_info["type"] = target_config["type"]

                        # Extract database name
                        # Different adapters use different keys for database name
                        db_name = None
                        if "database" in target_config:
                            db_name = target_config["database"]
                        elif "dbname" in target_config:  # Some PostgreSQL configs
                            db_name = target_config["dbname"]
                        elif "catalog" in target_config:  # Some adapters use catalog
                            db_name = target_config["catalog"]

                        if db_name:
                            target_info["database"] = db_name

                        # Add any additional connection details that might be useful
                        if "host" in target_config:
                            target_info["host"] = target_config["host"]
                        if "port" in target_config:
                            target_info["port"] = target_config["port"]
                        if "schema" in target_config:
                            target_info["schema"] = target_config["schema"]

                        targets[target_name] = target_info

                    default_target = profile_config.get(
                        "target", list(targets.keys())[0] if targets else None
                    )
                    result["profiles"][profile_name] = {
                        "targets": targets,
                        "default_target": default_target,
                    }

            return json.dumps(result, indent=2)

        except yaml.YAMLError as e:
            return f"Error: Failed to parse profiles.yml: {e}"
        except Exception as e:
            return f"Error: Failed to read profiles.yml: {e}"
