import logging
from pathlib import Path

from dbt_mcp.resources.definitions import dbt_mcp_resource

logger = logging.getLogger(__name__)


@dbt_mcp_resource(
    uri="dbt://default-styleguide",
    name="dbt_default_styleguide",
    description="IMPORTANT: The default dbt styleguide and best practices from the dbt platform. Use it whenever you edit a dbt project!",
    mime_type="text/plain",
)
def get_default_styleguide() -> str:
    try:
        style_guide_path = Path(__file__).parent / "default_style_guide.md"
        return style_guide_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"Failed to read local styleguide: {e}")
        return "No styleguide found."
