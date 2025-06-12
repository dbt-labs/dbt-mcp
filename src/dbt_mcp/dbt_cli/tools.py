import subprocess

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config import DbtCliConfig
from dbt_mcp.prompts.prompts import get_prompt


def register_dbt_cli_tools(dbt_mcp: FastMCP, config: DbtCliConfig) -> None:
    def _run_dbt_command(command: list[str], selector: str | None = None) -> str:
        # Commands that should always be quiet to reduce output verbosity
        verbose_commands = ["build", "compile", "docs", "parse", "run", "test"]

        if selector:
            selector_params = str(selector).split(" ")
            command = command + ["--select"] + selector_params

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
        selector: str | None = Field(
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
        selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
    ) -> str:
        return _run_dbt_command(["list"], selector)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/parse"))
    def parse() -> str:
        return _run_dbt_command(["parse"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/run"))
    def run(
        selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
    ) -> str:
        return _run_dbt_command(["run"], selector)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/test"))
    def test(
        selector: str | None = Field(
            default=None, description=get_prompt("dbt_cli/args/selectors")
        ),
    ) -> str:
        return _run_dbt_command(["test"], selector)

    @dbt_mcp.tool(description=get_prompt("dbt_cli/show"))
    def show(
        sql_query: str = Field(description=get_prompt("dbt_cli/args/sql_query")),
        limit: int | None = Field(
            default=None, description=get_prompt("dbt_cli/args/limit")
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
        return _run_dbt_command(args)
