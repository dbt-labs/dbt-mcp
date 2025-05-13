import subprocess

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import Config
from dbt_mcp.prompts.prompts import get_prompt


def register_dbt_cli_tools(dbt_mcp: FastMCP, config: Config) -> None:
    def _run_dbt_command(command: list[str]) -> str:
        # Commands that should always be quiet to reduce output verbosity
        verbose_commands = ["build", "compile", "docs", "parse", "run", "test"]

        full_command = command.copy()
        # Add --quiet flag to specific commands to reduce context window usage
        if len(full_command) > 0 and full_command[0] in verbose_commands:
            main_command = full_command[0]
            command_args = full_command[1:] if len(full_command) > 1 else []
            full_command = [main_command, "--quiet", *command_args]

        process = subprocess.Popen(
            args=[config.dbt_command, *full_command],
            cwd=config.project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output, _ = process.communicate()
        return output

    @dbt_mcp.tool(description=get_prompt("dbt_cli/build"))
    def build() -> str:
        return _run_dbt_command(["build"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/compile"))
    def compile() -> str:
        return _run_dbt_command(["compile"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/docs"))
    def docs() -> str:
        return _run_dbt_command(["docs", "generate"])

    @dbt_mcp.tool(name="list", description=get_prompt("dbt_cli/list"))
    def ls() -> str:
        return _run_dbt_command(["list"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/parse"))
    def parse() -> str:
        return _run_dbt_command(["parse"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/run"))
    def run() -> str:
        return _run_dbt_command(["run"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/test"))
    def test() -> str:
        return _run_dbt_command(["test"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/show"))
    def show(sql_query: str, limit: int | None = None) -> str:
        args = ["show", "--inline", sql_query, "--favor-state"]
        if limit:
            args.extend(["--limit", str(limit)])
        args.extend(["--output", "json"])
        return _run_dbt_command(args)
