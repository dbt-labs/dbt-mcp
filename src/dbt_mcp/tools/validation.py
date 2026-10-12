from mcp.server.fastmcp.tools.base import Tool


def configure_argument_validation(tool: Tool) -> None:
    # FastMCP doesn't expose model_config in Tool.from_function. Configure its
    # generated model locally, and publish the schema from that same model.
    model = tool.fn_metadata.arg_model
    model.model_config = {**model.model_config, "extra": "forbid"}
    model.model_rebuild(force=True)
    tool.parameters = model.model_json_schema(by_alias=True)
