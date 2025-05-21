from dataclasses import dataclass
from typing import Any

from dbt_mcp.config.config import TrackingConfig


@dataclass
class ToolCalledEvent:
    tool_name: str
    arguments: dict[str, Any]
    error_message: str | None
    prod_environment_id: int | None
    dev_environment_id: int | None
    dbt_cloud_user_id: int | None
    local_user_id: str | None


class UsageTracker:
    def emit_tool_called_event(
        self,
        config: TrackingConfig,
        tool_name: str,
        arguments: dict[str, Any],
        error_message: str | None = None,
    ):
        pass  # TODO
