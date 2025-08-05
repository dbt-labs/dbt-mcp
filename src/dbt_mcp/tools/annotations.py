from mcp.types import ToolAnnotations


def create_tool_annotations(
    title: str | None = None,
    read_only_hint: bool = False,
    destructive_hint: bool = True,
    idempotent_hint: bool = False,
    open_world_hint: bool = True,
) -> ToolAnnotations:
    """
    Creates tool annotations. Defaults to the most cautious option,
    i.e destructive, non-idempotent, and open-world.
    Args:
        - title: Human-readable title for the tool
        - read_only_hint: If true, the tool does not modify its environment.
        - destructive_hint:
          If true, the tool may perform destructive updates to its environment.
          If false, the tool performs only additive updates.
          This property is meaningful only when `readOnlyHint == false`.
        - idempotent_hint: Whether repeated calls have the same effect
          If true, calling the tool repeatedly with the same arguments will have no additional effect on the its environment.
          This property is meaningful only when `readOnlyHint == false`.
        - open_world_hint: Whether the tool interacts with external systems
          If true, this tool may interact with an "open world" of external entities.
          If false, the tool's domain of interaction is closed.
          For example, the world of a web search tool is open, whereas that of a memory tool is not.
    """
    return ToolAnnotations(
        title=title,
        readOnlyHint=read_only_hint,
        destructiveHint=destructive_hint,
        idempotentHint=idempotent_hint,
        openWorldHint=open_world_hint,
    )
