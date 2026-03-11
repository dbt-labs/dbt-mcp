"""ProjectContext resolver: derives environment IDs from a project_id."""

import logging
import time
from dataclasses import dataclass
from typing import Any

from dbt_mcp.dbt_admin.client import DbtAdminAPIClient

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 300  # 5 minutes


@dataclass(frozen=True)
class ProjectContext:
    """Resolved environment context for a given project."""

    project_id: int
    prod_environment_id: int | None
    dev_environment_id: int | None


@dataclass
class _CacheEntry:
    value: ProjectContext
    expires_at: float


class ProjectContextResolver:
    """Resolves project_id to environment IDs via the Admin API, with TTL caching."""

    def __init__(
        self,
        admin_client: DbtAdminAPIClient,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
    ) -> None:
        self._admin_client = admin_client
        self._ttl_seconds = ttl_seconds
        self._cache: dict[tuple[int, int], _CacheEntry] = {}

    async def resolve(
        self,
        *,
        account_id: int,
        project_id: int,
    ) -> ProjectContext:
        """Resolve environment IDs for a project, using cache when available."""
        cache_key = (account_id, project_id)
        now = time.monotonic()

        entry = self._cache.get(cache_key)
        if entry is not None and entry.expires_at > now:
            return entry.value

        context = await self._fetch(account_id=account_id, project_id=project_id)
        self._cache[cache_key] = _CacheEntry(
            value=context,
            expires_at=now + self._ttl_seconds,
        )
        return context

    async def _fetch(
        self,
        *,
        account_id: int,
        project_id: int,
    ) -> ProjectContext:
        """Call the environments API and derive prod/dev environment IDs."""
        environments = await self._admin_client.list_environments(
            account_id, project_id
        )
        return _parse_environments(project_id, environments)


def _parse_environments(
    project_id: int, environments: list[dict[str, Any]]
) -> ProjectContext:
    """Extract prod and dev environment IDs from an environments API response."""
    prod_environment_id: int | None = None
    dev_environment_id: int | None = None

    for env in environments:
        deployment_type = env.get("deployment_type")
        env_type = env.get("type")

        if deployment_type == "production" and prod_environment_id is None:
            prod_environment_id = env["id"]
        elif env_type == "development" and dev_environment_id is None:
            dev_environment_id = env["id"]

    if prod_environment_id is None:
        logger.warning(
            f"No production environment found for project {project_id}"
        )

    return ProjectContext(
        project_id=project_id,
        prod_environment_id=prod_environment_id,
        dev_environment_id=dev_environment_id,
    )
