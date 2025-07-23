import logging
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import RemoteConfig
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.dbt_admin.client import get_admin_api_client

from pydantic import Field

logger = logging.getLogger(__name__)


def register_admin_api_tools(dbt_mcp: FastMCP, config: RemoteConfig, disable_tools: list[str]) -> None:
    """Register dbt Admin API v2 tools."""
    admin_client = get_admin_api_client(config)

    if "list_jobs" not in disable_tools:
        @dbt_mcp.tool(description=get_prompt("admin_api/list_jobs"))
        def list_jobs(project_id: Optional[int] = None, environment_id: Optional[int] = None, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]] | str:
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
                return admin_client.list_jobs(config.account_id, **params)
            except Exception as e:
                logger.error(f"Error listing jobs for account {config.account_id}: {e}")
                return str(e)

    if "get_job" not in disable_tools:
        @dbt_mcp.tool(description=get_prompt("admin_api/get_job"))
        def get_job(job_id: int) -> Dict[str, Any] | str:
            """Get details for a specific job."""
            try:
                return admin_client.get_job(config.account_id, job_id)
            except Exception as e:
                logger.error(f"Error getting job {job_id}: {e}")
                return str(e)

    if "trigger_job_run" not in disable_tools:
        @dbt_mcp.tool(description=get_prompt("admin_api/trigger_job_run"))
        def trigger_job_run(job_id: int, cause: str, git_branch: Optional[str] = None, git_sha: Optional[str] = None, schema_override: Optional[str] = None) -> Dict[str, Any] | str:
            """Trigger a job run."""
            try:
                kwargs = {}
                if git_branch:
                    kwargs["git_branch"] = git_branch
                if git_sha:
                    kwargs["git_sha"] = git_sha
                if schema_override:
                    kwargs["schema_override"] = schema_override
                return admin_client.trigger_job_run(config.account_id, job_id, cause, **kwargs)
            except Exception as e:
                logger.error(f"Error triggering job {job_id}: {e}")
                return str(e)

    if "list_runs" not in disable_tools:
        @dbt_mcp.tool(description=get_prompt("admin_api/list_runs"))
        def list_runs(job_id: Optional[int] = None, project_id: Optional[int] = None, environment_id: Optional[int] = None, status: Optional[str] = None, limit: Optional[int] = None, offset: Optional[int] = None, order_by: Optional[str] = None) -> List[Dict[str, Any]] | str:
            """List runs in an account."""
            try:
                params = {}
                if job_id:
                    params["job_definition_id"] = job_id
                if project_id:
                    params["project_id"] = project_id
                if environment_id:
                    params["environment_id"] = environment_id
                if status:
                    params["status"] = status
                if limit:
                    params["limit"] = limit
                if offset:
                    params["offset"] = offset
                if order_by:
                    params["order_by"] = order_by
                return admin_client.list_runs(config.account_id, **params)
            except Exception as e:
                logger.error(f"Error listing runs for account {config.account_id}: {e}")
                return str(e)

    if "get_run" not in disable_tools:
        @dbt_mcp.tool(description=get_prompt("admin_api/get_run"))
        def get_run(
            run_id: int, 
            debug: bool = Field(default=False, description="Set to True only if the person is explicitely asking for debug level logs. Otherwise, do not set if just the logs are asked.")
        ) -> Dict[str, Any] | str:
            """Get details for a specific run."""
            try:
                return admin_client.get_run(config.account_id, run_id, debug=debug)
            except Exception as e:
                logger.error(f"Error getting run {run_id}: {e}")
                return str(e)

    if "cancel_run" not in disable_tools:
        @dbt_mcp.tool(description=get_prompt("admin_api/cancel_run"))
        def cancel_run(run_id: int) -> Dict[str, Any] | str:
            """Cancel a run."""
            try:
                return admin_client.cancel_run(config.account_id, run_id)
            except Exception as e:
                logger.error(f"Error cancelling run {run_id}: {e}")
                return str(e)

    if "retry_run" not in disable_tools:
        @dbt_mcp.tool(description=get_prompt("admin_api/retry_run"))
        def retry_run(run_id: int) -> Dict[str, Any] | str:
            """Retry a failed run."""
            try:
                return admin_client.retry_run(config.account_id, run_id)
            except Exception as e:
                logger.error(f"Error retrying run {run_id}: {e}")
                return str(e)

    if "list_run_artifacts" not in disable_tools:
        @dbt_mcp.tool(description=get_prompt("admin_api/list_run_artifacts"))
        def list_run_artifacts(run_id: int) -> List[str] | str:
            """List artifacts for a run."""
            try:
                return admin_client.list_run_artifacts(config.account_id, run_id)
            except Exception as e:
                logger.error(f"Error listing artifacts for run {run_id}: {e}")
                return str(e)

    if "get_run_artifact" not in disable_tools:
        @dbt_mcp.tool(description=get_prompt("admin_api/get_run_artifact"))
        def get_run_artifact(run_id: int, artifact_path: str, step: Optional[int] = None) -> Any | str:
            """Get a specific run artifact."""
            try:
                return admin_client.get_run_artifact(config.account_id, run_id, artifact_path, step)
            except Exception as e:
                logger.error(f"Error getting artifact {artifact_path} for run {run_id}: {e}")
                return str(e)
