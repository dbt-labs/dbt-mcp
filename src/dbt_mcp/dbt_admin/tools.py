import logging
from collections.abc import Sequence
from enum import Enum
from typing import Any

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import (
    AdminApiConfig,
    ConfigProvider,
)
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)


class JobRunStatus(str, Enum):
    """Enum for job run status values."""

    QUEUED = "queued"
    STARTING = "starting"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    CANCELLED = "cancelled"


STATUS_MAP = {
    JobRunStatus.QUEUED: 1,
    JobRunStatus.STARTING: 2,
    JobRunStatus.RUNNING: 3,
    JobRunStatus.SUCCESS: 10,
    JobRunStatus.ERROR: 20,
    JobRunStatus.CANCELLED: 30,
}


def create_admin_api_tool_definitions(
    admin_client: DbtAdminAPIClient,
    admin_api_config_provider: ConfigProvider[AdminApiConfig],
) -> list[ToolDefinition]:
    async def list_jobs(
        # TODO: add support for project_id in the future
        # project_id: Optional[int] = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """List jobs in an account."""
        admin_api_config = await admin_api_config_provider.get_config()
        params = {}
        # if project_id:
        #     params["project_id"] = project_id
        if admin_api_config.prod_environment_id:
            params["environment_id"] = admin_api_config.prod_environment_id
        if limit:
            params["limit"] = limit
        if offset:
            params["offset"] = offset
        return await admin_client.list_jobs(admin_api_config.account_id, **params)

    async def get_job_details(job_id: int) -> dict[str, Any]:
        """Get details for a specific job."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.get_job_details(admin_api_config.account_id, job_id)

    async def trigger_job_run(
        job_id: int,
        cause: str = "Triggered by dbt MCP",
        git_branch: str | None = None,
        git_sha: str | None = None,
        schema_override: str | None = None,
    ) -> dict[str, Any]:
        """Trigger a job run."""
        admin_api_config = await admin_api_config_provider.get_config()
        kwargs = {}
        if git_branch:
            kwargs["git_branch"] = git_branch
        if git_sha:
            kwargs["git_sha"] = git_sha
        if schema_override:
            kwargs["schema_override"] = schema_override
        return await admin_client.trigger_job_run(
            admin_api_config.account_id, job_id, cause, **kwargs
        )

    async def list_jobs_runs(
        job_id: int | None = None,
        status: JobRunStatus | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """List runs in an account."""
        admin_api_config = await admin_api_config_provider.get_config()
        params: dict[str, Any] = {}
        if job_id:
            params["job_definition_id"] = job_id
        if status:
            status_id = STATUS_MAP[status]
            params["status"] = status_id
        if limit:
            params["limit"] = limit
        if offset:
            params["offset"] = offset
        if order_by:
            params["order_by"] = order_by
        return await admin_client.list_jobs_runs(admin_api_config.account_id, **params)

    async def get_job_run_details(
        run_id: int,
    ) -> dict[str, Any]:
        """Get details for a specific job run."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.get_job_run_details(
            admin_api_config.account_id, run_id
        )

    async def cancel_job_run(run_id: int) -> dict[str, Any]:
        """Cancel a job run."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.cancel_job_run(admin_api_config.account_id, run_id)

    async def retry_job_run(run_id: int) -> dict[str, Any]:
        """Retry a failed job run."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.retry_job_run(admin_api_config.account_id, run_id)

    async def list_job_run_artifacts(run_id: int) -> list[str]:
        """List artifacts for a job run."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.list_job_run_artifacts(
            admin_api_config.account_id, run_id
        )

    async def get_job_run_artifact(
        run_id: int, artifact_path: str, step: int | None = None
    ) -> Any:
        """Get a specific job run artifact."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.get_job_run_artifact(
            admin_api_config.account_id, run_id, artifact_path, step
        )

    return [
        ToolDefinition(
            description=get_prompt("admin_api/list_jobs"),
            fn=list_jobs,
            annotations=create_tool_annotations(
                title="List Jobs",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/get_job_details"),
            fn=get_job_details,
            annotations=create_tool_annotations(
                title="Get Job Details",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/trigger_job_run"),
            fn=trigger_job_run,
            annotations=create_tool_annotations(
                title="Trigger Job Run",
                read_only_hint=False,
                destructive_hint=False,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/list_jobs_runs"),
            fn=list_jobs_runs,
            annotations=create_tool_annotations(
                title="List Jobs Runs",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/get_job_run_details"),
            fn=get_job_run_details,
            annotations=create_tool_annotations(
                title="Get Job Run Details",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/cancel_job_run"),
            fn=cancel_job_run,
            annotations=create_tool_annotations(
                title="Cancel Job Run",
                read_only_hint=False,
                destructive_hint=False,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/retry_job_run"),
            fn=retry_job_run,
            annotations=create_tool_annotations(
                title="Retry Job Run",
                read_only_hint=False,
                destructive_hint=False,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/list_job_run_artifacts"),
            fn=list_job_run_artifacts,
            annotations=create_tool_annotations(
                title="List Job Run Artifacts",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/get_job_run_artifact"),
            fn=get_job_run_artifact,
            annotations=create_tool_annotations(
                title="Get Job Run Artifact",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
    ]


def register_admin_api_tools(
    dbt_mcp: FastMCP,
    admin_config_provider: ConfigProvider[AdminApiConfig],
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    """Register dbt Admin API tools."""
    admin_client = DbtAdminAPIClient(admin_config_provider)
    register_tools(
        dbt_mcp,
        create_admin_api_tool_definitions(admin_client, admin_config_provider),
        exclude_tools,
    )
