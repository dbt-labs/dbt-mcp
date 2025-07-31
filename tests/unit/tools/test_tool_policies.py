from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp
from dbt_mcp.tools.policy import tool_policies
from dbt_mcp.tools.tool_names import ToolName
from tests.env_vars import default_env_vars_context


async def test_tool_policies_match_server_tools():
    """Test that the ToolPolicy enum matches the tools registered in the server."""
    sql_tool_names = {"text_to_sql", "execute_sql"}

    with default_env_vars_context():
        config = load_config()
        dbt_mcp = await create_dbt_mcp(config)

        # Get all tools from the server
        server_tools = await dbt_mcp.list_tools()
        # Manually adding SQL tools here because the server doesn't get them
        # in this unit test.
        server_tool_names = {tool.name for tool in server_tools} | sql_tool_names
        policy_names = {policy_name for policy_name in tool_policies}

        if server_tool_names != policy_names:
            raise ValueError(
                f"Tool name mismatch:\n"
                f"In server but not in enum: {server_tool_names - policy_names}\n"
                f"In enum but not in server: {policy_names - server_tool_names}"
            )


def test_tool_policies_match_tool_names():
    policy_names = {policy.upper() for policy in tool_policies}
    tool_names = {tool.name for tool in ToolName}
    if tool_names != policy_names:
        raise ValueError(
            f"Tool name mismatch:\n"
            f"In tool names but not in policy: {tool_names - policy_names}\n"
            f"In policy but not in tool names: {policy_names - tool_names}"
        )


def test_tool_policies_no_duplicates():
    """Test that there are no duplicate tool names in the policy."""
    assert len(tool_policies) == len(set(tool_policies.keys()))
