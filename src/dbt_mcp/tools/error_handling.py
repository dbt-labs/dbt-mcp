import mcp.types as types


def make_error_result(error_message: str) -> types.ServerResult:
    """Create a ServerResult with an error CallToolResult."""
    return types.ServerResult(
        types.CallToolResult(
            content=[types.TextContent(type="text", text=error_message)],
            isError=True,
        )
    )
