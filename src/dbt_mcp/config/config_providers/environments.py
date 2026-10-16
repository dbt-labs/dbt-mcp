from __future__ import annotations

from typing import TYPE_CHECKING

from dbt_mcp.errors import NotFoundError

if TYPE_CHECKING:
    from dbt_mcp.config.settings import DbtMcpSettings
    from dbt_mcp.dbt_admin.client import DbtAdminAPIClient


async def resolve_production_environment_id(
    settings: DbtMcpSettings,
    *,
    project_id: int | None,
    admin_client: DbtAdminAPIClient | None,
) -> int:
    if project_id is None:
        if settings.actual_prod_environment_id is None:
            raise ValueError("Select a project or configure a production environment")
        return settings.actual_prod_environment_id
    # Must stay unconditional: explicit project selection requires an allowlist to
    # validate against, even outside multi-project configs.
    if not settings.dbt_project_ids:
        raise ValueError(
            "This server is not configured for project selection. Omit project_id, "
            "or configure DBT_PROJECT_IDS to enable selecting among multiple projects."
        )
    if project_id not in settings.dbt_project_ids:
        raise ValueError(
            f"Project {project_id} is not in the selected projects. "
            f"Available project IDs: {settings.dbt_project_ids}"
        )
    if admin_client is None:
        raise ValueError("Project selection requires an Admin API client")
    production, _ = await admin_client.get_environments_for_project(project_id)
    if production is None or production.id is None:
        raise NotFoundError(f"No production environment found for project {project_id}")
    return production.id
