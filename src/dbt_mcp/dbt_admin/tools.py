import logging
from typing import Any, Dict, List, Optional
from collections.abc import Sequence

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config import Config
from dbt_mcp.dbt_admin.client import get_admin_api_client
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.annotations import create_tool_annotations

logger = logging.getLogger(__name__)


def create_admin_api_tool_definitions(
    admin_client: Any, admin_api_config: Any
) -> list[ToolDefinition]:
    def list_jobs(
        project_id: Optional[int] = None,
        environment_id: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]] | str:
        """List jobs in an account."""
        try:
            params = {}
            if project_id:
                params["project_id"] = project_id
            if environment_id:
                params["environment_id"] = environment_id
            if limit:
                params["limit"] = limit
            if offset:
                params["offset"] = offset
            return admin_client.list_jobs(admin_api_config.account_id, **params)
        except Exception as e:
            logger.error(
                f"Error listing jobs for account {admin_api_config.account_id}: {e}"
            )
            return str(e)

    def get_job(job_id: int) -> Dict[str, Any] | str:
        """Get details for a specific job."""
        try:
            return admin_client.get_job(admin_api_config.account_id, job_id)
        except Exception as e:
            logger.error(f"Error getting job {job_id}: {e}")
            return str(e)

    def trigger_job_run(
        job_id: int,
        cause: str,
        git_branch: Optional[str] = None,
        git_sha: Optional[str] = None,
        schema_override: Optional[str] = None,
    ) -> Dict[str, Any] | str:
        """Trigger a job run."""
        try:
            kwargs = {}
            if git_branch:
                kwargs["git_branch"] = git_branch
            if git_sha:
                kwargs["git_sha"] = git_sha
            if schema_override:
                kwargs["schema_override"] = schema_override
            return admin_client.trigger_job_run(
                admin_api_config.account_id, job_id, cause, **kwargs
            )
        except Exception as e:
            logger.error(f"Error triggering job {job_id}: {e}")
            return str(e)

    def list_runs(
        job_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[Dict[str, Any]] | str:
        """List runs in an account."""
        try:
            params: Dict[str, Any] = {}
            if job_id:
                params["job_definition_id"] = job_id
            if status:
                params["status"] = status
            if limit:
                params["limit"] = limit
            if offset:
                params["offset"] = offset
            if order_by:
                params["order_by"] = order_by
            return admin_client.list_runs(admin_api_config.account_id, **params)
        except Exception as e:
            logger.error(
                f"Error listing runs for account {admin_api_config.account_id}: {e}"
            )
            return str(e)

    def get_run(
        run_id: int,
        debug: bool = Field(
            default=False,
            description="Set to True only if the person is explicitely asking for debug level logs. Otherwise, do not set if just the logs are asked.",
        ),
    ) -> Dict[str, Any] | str:
        """Get details for a specific run."""
        try:
            return admin_client.get_run(
                admin_api_config.account_id, run_id, debug=debug
            )
        except Exception as e:
            logger.error(f"Error getting run {run_id}: {e}")
            return str(e)

    def cancel_run(run_id: int) -> Dict[str, Any] | str:
        """Cancel a run."""
        try:
            return admin_client.cancel_run(admin_api_config.account_id, run_id)
        except Exception as e:
            logger.error(f"Error cancelling run {run_id}: {e}")
            return str(e)

    def retry_run(run_id: int) -> Dict[str, Any] | str:
        """Retry a failed run."""
        try:
            return admin_client.retry_run(admin_api_config.account_id, run_id)
        except Exception as e:
            logger.error(f"Error retrying run {run_id}: {e}")
            return str(e)

    def list_run_artifacts(run_id: int) -> List[str] | str:
        """List artifacts for a run."""
        try:
            return admin_client.list_run_artifacts(admin_api_config.account_id, run_id)
        except Exception as e:
            logger.error(f"Error listing artifacts for run {run_id}: {e}")
            return str(e)

    def get_run_artifact(
        run_id: int, artifact_path: str, step: Optional[int] = None
    ) -> Any | str:
        """Get a specific run artifact."""
        try:
            return admin_client.get_run_artifact(
                admin_api_config.account_id, run_id, artifact_path, step
            )
        except Exception as e:
            logger.error(
                f"Error getting artifact {artifact_path} for run {run_id}: {e}"
            )
            return str(e)

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
            description=get_prompt("admin_api/get_job"),
            fn=get_job,
            annotations=create_tool_annotations(
                title="Get Job",
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
            description=get_prompt("admin_api/list_runs"),
            fn=list_runs,
            annotations=create_tool_annotations(
                title="List Runs",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/get_run"),
            fn=get_run,
            annotations=create_tool_annotations(
                title="Get Run",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/cancel_run"),
            fn=cancel_run,
            annotations=create_tool_annotations(
                title="Cancel Run",
                read_only_hint=False,
                destructive_hint=False,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/retry_run"),
            fn=retry_run,
            annotations=create_tool_annotations(
                title="Retry Run",
                read_only_hint=False,
                destructive_hint=False,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/list_run_artifacts"),
            fn=list_run_artifacts,
            annotations=create_tool_annotations(
                title="List Run Artifacts",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/get_run_artifact"),
            fn=get_run_artifact,
            annotations=create_tool_annotations(
                title="Get Run Artifact",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
    ]


def register_admin_api_tools(
    dbt_mcp: FastMCP,
    config: Config,
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    """Register dbt Admin API v2 tools."""
    if not config.admin_api_config:
        return

    admin_client = get_admin_api_client(config.admin_api_config)
    register_tools(
        dbt_mcp,
        create_admin_api_tool_definitions(admin_client, config.admin_api_config),
        exclude_tools,
    )
