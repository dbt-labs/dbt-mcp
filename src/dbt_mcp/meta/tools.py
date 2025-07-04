import logging
from importlib.metadata import version

from mcp.server.fastmcp import FastMCP

from dbt_mcp.prompts.prompts import get_prompt


logger = logging.getLogger(__name__)


def register_meta_tools(dbt_mcp: FastMCP) -> None:
    @dbt_mcp.tool(description=get_prompt("meta/get_dbt_mcp_server_version"))
    def get_dbt_mcp_server_version() -> str:
        return version("dbt-mcp")
