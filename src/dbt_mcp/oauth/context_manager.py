import logging
from pathlib import Path

import yaml

from dbt_mcp.oauth.dbt_platform import DbtPlatformContext

logger = logging.getLogger(__name__)


class DbtPlatformContextManager:
    """
    Manages dbt platform context files and context creation.
    """

    def __init__(self, config_location: Path):
        self.config_location = config_location

    def read_context(self) -> DbtPlatformContext | None:
        """Read the current context from file with proper locking."""
        if not self.config_location.exists():
            return None
        try:
            content = self.config_location.read_text()
            if not content.strip():
                return None
            parsed_content = yaml.safe_load(content)
            if parsed_content is None or not isinstance(parsed_content, dict):
                logger.warning("dbt Platform Context YAML file is invalid")
                return None
            return DbtPlatformContext(**parsed_content)
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML from {self.config_location}: {e}")
        except Exception as e:
            logger.error(f"Failed to read context from {self.config_location}: {e}")
        return None

    def update_context(
        self, new_dbt_platform_context: DbtPlatformContext
    ) -> DbtPlatformContext:
        """
        Update the existing context by merging with new context data.
        Reads existing context, merges with new data, and writes back to file.
        """
        existing_dbt_platform_context = self.read_context()
        if existing_dbt_platform_context is None:
            existing_dbt_platform_context = DbtPlatformContext()
        next_dbt_platform_context = existing_dbt_platform_context.override(
            new_dbt_platform_context
        )
        self.write_context_to_file(next_dbt_platform_context)
        return next_dbt_platform_context

    def _ensure_config_location_exists(self) -> None:
        """Ensure the config file location and its parent directories exist."""
        self.config_location.parent.mkdir(parents=True, exist_ok=True)
        if not self.config_location.exists():
            self.config_location.touch()

    def write_context_to_file(self, context: DbtPlatformContext) -> None:
        """Write context to file with proper locking."""
        self._ensure_config_location_exists()
        self.config_location.write_text(
            yaml.dump(context.model_dump(), default_flow_style=False)
        )
