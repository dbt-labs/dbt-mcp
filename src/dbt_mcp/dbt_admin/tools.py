import logging
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import RemoteConfig
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.dbt_admin.client import get_admin_api_client

logger = logging.getLogger(__name__)


def register_admin_api_tools(dbt_mcp: FastMCP, config: RemoteConfig) -> None:
    """Register dbt Cloud Admin API v2 tools."""
    admin_client = get_admin_api_client(config)
    
    # Helper to get account ID from config or user input
    def get_account_id() -> int:
        # If config has account info, we could derive it, but for now we'll need it passed
        # This could be enhanced to auto-detect from user permissions
        return config.account_id  # This might need adjustment based on actual config structure

    @dbt_mcp.tool(description=get_prompt("admin_api/list_accounts"))
    def list_accounts() -> List[Dict[str, Any]] | str:
        """List all dbt Cloud accounts."""
        try:
            return admin_client.list_accounts()
        except Exception as e:
            logger.error(f"Error listing accounts: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/get_account"))
    def get_account(account_id: int) -> Dict[str, Any] | str:
        """Get details for a specific account."""
        try:
            return admin_client.get_account(account_id)
        except Exception as e:
            logger.error(f"Error getting account {account_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/list_projects"))
    def list_projects(account_id: int, project_id: Optional[int] = None, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]] | str:
        """List projects in an account."""
        try:
            params = {}
            if project_id:
                params["pk"] = project_id
            if limit:
                params["limit"] = limit
            if offset:
                params["offset"] = offset
            return admin_client.list_projects(account_id, **params)
        except Exception as e:
            logger.error(f"Error listing projects for account {account_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/get_project"))
    def get_project(account_id: int, project_id: int) -> Dict[str, Any] | str:
        """Get details for a specific project."""
        try:
            return admin_client.get_project(account_id, project_id)
        except Exception as e:
            logger.error(f"Error getting project {project_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/list_environments"))
    def list_environments(account_id: int, project_id: Optional[int] = None, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]] | str:
        """List environments in an account."""
        try:
            params = {}
            if project_id:
                params["project_id"] = project_id
            if limit:
                params["limit"] = limit
            if offset:
                params["offset"] = offset
            return admin_client.list_environments(account_id, **params)
        except Exception as e:
            logger.error(f"Error listing environments for account {account_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/get_environment"))
    def get_environment(account_id: int, environment_id: int) -> Dict[str, Any] | str:
        """Get details for a specific environment."""
        try:
            return admin_client.get_environment(account_id, environment_id)
        except Exception as e:
            logger.error(f"Error getting environment {environment_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/list_jobs"))
    def list_jobs(account_id: int, project_id: Optional[int] = None, environment_id: Optional[int] = None, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]] | str:
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
            return admin_client.list_jobs(account_id, **params)
        except Exception as e:
            logger.error(f"Error listing jobs for account {account_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/get_job"))
    def get_job(account_id: int, job_id: int, include_related: Optional[str] = None) -> Dict[str, Any] | str:
        """Get details for a specific job."""
        try:
            return admin_client.get_job(account_id, job_id, include_related)
        except Exception as e:
            logger.error(f"Error getting job {job_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/trigger_job_run"))
    def trigger_job_run(account_id: int, job_id: int, cause: str, git_branch: Optional[str] = None, git_sha: Optional[str] = None, schema_override: Optional[str] = None) -> Dict[str, Any] | str:
        """Trigger a job run."""
        try:
            kwargs = {}
            if git_branch:
                kwargs["git_branch"] = git_branch
            if git_sha:
                kwargs["git_sha"] = git_sha
            if schema_override:
                kwargs["schema_override"] = schema_override
            return admin_client.trigger_job_run(account_id, job_id, cause, **kwargs)
        except Exception as e:
            logger.error(f"Error triggering job {job_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/list_runs"))
    def list_runs(account_id: int, job_id: Optional[int] = None, project_id: Optional[int] = None, environment_id: Optional[int] = None, status: Optional[str] = None, limit: Optional[int] = None, offset: Optional[int] = None, order_by: Optional[str] = None) -> List[Dict[str, Any]] | str:
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
            return admin_client.list_runs(account_id, **params)
        except Exception as e:
            logger.error(f"Error listing runs for account {account_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/get_run"))
    def get_run(account_id: int, run_id: int, include_related: Optional[str] = None) -> Dict[str, Any] | str:
        """Get details for a specific run."""
        try:
            return admin_client.get_run(account_id, run_id, include_related)
        except Exception as e:
            logger.error(f"Error getting run {run_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/cancel_run"))
    def cancel_run(account_id: int, run_id: int) -> Dict[str, Any] | str:
        """Cancel a run."""
        try:
            return admin_client.cancel_run(account_id, run_id)
        except Exception as e:
            logger.error(f"Error cancelling run {run_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/retry_run"))
    def retry_run(account_id: int, run_id: int) -> Dict[str, Any] | str:
        """Retry a failed run."""
        try:
            return admin_client.retry_run(account_id, run_id)
        except Exception as e:
            logger.error(f"Error retrying run {run_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/list_run_artifacts"))
    def list_run_artifacts(account_id: int, run_id: int) -> List[str] | str:
        """List artifacts for a run."""
        try:
            return admin_client.list_run_artifacts(account_id, run_id)
        except Exception as e:
            logger.error(f"Error listing artifacts for run {run_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/get_run_artifact"))
    def get_run_artifact(account_id: int, run_id: int, artifact_path: str, step: Optional[int] = None) -> Any | str:
        """Get a specific run artifact."""
        try:
            return admin_client.get_run_artifact(account_id, run_id, artifact_path, step)
        except Exception as e:
            logger.error(f"Error getting artifact {artifact_path} for run {run_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/list_users"))
    def list_users(account_id: int) -> List[Dict[str, Any]] | str:
        """List users in an account."""
        try:
            return admin_client.list_users(account_id)
        except Exception as e:
            logger.error(f"Error listing users for account {account_id}: {e}")
            return str(e)

    @dbt_mcp.tool(description=get_prompt("admin_api/get_user"))
    def get_user(user_id: int) -> Dict[str, Any] | str:
        """Get details for a specific user."""
        try:
            return admin_client.get_user(user_id)
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            return str(e)