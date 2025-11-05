from dbt_mcp.errors import GraphQLError


def raise_gql_error(result: dict) -> None:
    if result.get("errors"):
        if len(result.get("errors", [])) > 0:
            error_messages = [
                error.get("message", "Unknown error")
                for error in result.get("errors", [])
                if isinstance(error, dict)
            ]
            if error_messages:
                raise GraphQLError(f"Errors calling API: {', '.join(error_messages)}")
        raise GraphQLError(
            "Unknown error calling API. "
            + "Check your configuration or contact support if this persists."
        )
