from mcp.types import ToolAnnotations


def create_tool_annotations(
    title: str | None = None,
    read_only_hint: bool = True,
    destructive_hint: bool = False,
    idempotent_hint: bool = True,
    open_world_hint: bool = True,
) -> ToolAnnotations:
    """
    Create tool annotations. Defaults to read-only, non-destructive,
    idempotent, and open-world hints. Forced to explicitly set hints
    to destructive and non-idempotent.
    Args:
        - title: Human-readable title for the tool
        - read_only_hint: Whether the tool only reads data
        - destructive_hint: Whether the tool makes destructive changes
        - idempotent_hint: Whether repeated calls have the same effect
        - open_world_hint: Whether the tool interacts with external systems
    """
    return ToolAnnotations(
        title=title,
        readOnlyHint=read_only_hint,
        destructiveHint=destructive_hint,
        idempotentHint=idempotent_hint,
        openWorldHint=open_world_hint,
    )
