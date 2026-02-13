"""Base error class for all dbt-mcp tool calls."""


class ToolCallError(Exception):
    """Base exception for all tool call errors in dbt-mcp."""

    @property
    def is_client_error(self) -> bool:
        """Whether this error is a client-side error (True) or server-side error (False).

        Client errors indicate the caller made a mistake (bad input, invalid parameters).
        Server errors indicate something went wrong on the server side.
        Defaults to False (server-side). Subclasses override to True for client errors.
        """
        return False
