import subprocess
from typing import Optional

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config import DbtCliConfig
from dbt_mcp.prompts.prompts import get_prompt


def register_dbt_cli_tools(dbt_mcp: FastMCP, config: DbtCliConfig) -> None:
    def _run_dbt_command(command: list[str], selector: Optional[str] = None) -> str:
        # Commands that should always be quiet to reduce output verbosity
        verbose_commands = ["build", "compile", "docs", "parse", "run", "test"]

        if selector:
            selector_params = str(selector).split(" ")
            command = command + selector_params

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
        output, _ = process.communicate()
        return output or "OK"

    @dbt_mcp.tool(description=get_prompt("dbt_cli/build"))
    def build(
        selector: Optional[str] = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
    ) -> str:
        return _run_dbt_command(["build"], selector)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/compile"))
    def compile() -> str:
        return _run_dbt_command(["compile"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/docs"))
    def docs() -> str:
        return _run_dbt_command(["docs", "generate"])

    @dbt_mcp.tool(name="list", description=get_prompt("dbt_cli/list"))
    def ls(
        selector: Optional[str] = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
    ) -> str:
        return _run_dbt_command(["list"], selector)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/parse"))
    def parse() -> str:
        return _run_dbt_command(["parse"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/run"))
    def run(
        selector: Optional[str] = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
    ) -> str:
        return _run_dbt_command(["run"], selector)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/test"))
    def test(
        selector: Optional[str] = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
    ) -> str:
        return _run_dbt_command(["test"], selector)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/show"))
    def show(sql_query: str, limit: int | None = None) -> str:
        args = ["show", "--inline", sql_query, "--favor-state"]
        if limit:
            args.extend(["--limit", str(limit)])
        args.extend(["--output", "json"])
        return _run_dbt_command(args)
