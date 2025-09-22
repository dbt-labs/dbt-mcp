import logging
from functools import cache
from typing import Any

import requests

from dbt_mcp.config.config_providers import (
    AdminApiConfig,
    ConfigProvider,
)

logger = logging.getLogger(__name__)


class AdminAPIError(Exception):
    """Exception raised for Admin API errors."""

    pass


class DbtAdminAPIClient:
    """Client for interacting with the dbt Admin API."""

    def __init__(self, config_provider: ConfigProvider[AdminApiConfig]):
        self.config_provider = config_provider

    async def get_config(self) -> AdminApiConfig:
        return await self.config_provider.get_config()

    async def get_headers(self) -> dict[str, str]:
        config = await self.get_config()
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
        } | config.headers_provider.get_headers()

    async def _make_request(
        self, method: str, endpoint: str, **kwargs
    ) -> dict[str, Any]:
        """Make a request to the dbt API."""
        config = await self.get_config()
        url = f"{config.url}{endpoint}"
        headers = await self.get_headers()

        try:
            response = requests.request(method, url, headers=headers, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise AdminAPIError(f"API request failed: {e}")

    @cache
    async def list_jobs(self, account_id: int, **params) -> list[dict[str, Any]]:
        """List jobs for an account."""
        result = await self._make_request(
            "GET",
            f"/api/v2/accounts/{account_id}/jobs/?include_related=['most_recent_run','most_recent_completed_run']",
            params=params,
        )
        data = result.get("data", [])

        # we filter the data to the most relevant fields
        # the rest of the fields can be retrieved with the get_job tool
        filtered_data = [
            {
                "id": job.get("id"),
                "name": job.get("name"),
                "description": job.get("description"),
                "dbt_version": job.get("dbt_version"),
                "job_type": job.get("job_type"),
                "triggers": job.get("triggers"),
                "most_recent_run_id": job.get("most_recent_run").get("id")
                if job.get("most_recent_run")
                else None,
                "most_recent_run_status": job.get("most_recent_run").get(
                    "status_humanized"
                )
                if job.get("most_recent_run")
                else None,
                "most_recent_run_started_at": job.get("most_recent_run").get(
                    "started_at"
                )
                if job.get("most_recent_run")
                else None,
                "most_recent_run_finished_at": job.get("most_recent_run").get(
                    "finished_at"
                )
                if job.get("most_recent_run")
                else None,
                "most_recent_completed_run_id": job.get(
                    "most_recent_completed_run"
                ).get("id")
                if job.get("most_recent_completed_run")
                else None,
                "most_recent_completed_run_status": job.get(
                    "most_recent_completed_run"
                ).get("status_humanized")
                if job.get("most_recent_completed_run")
                else None,
                "most_recent_completed_run_started_at": job.get(
                    "most_recent_completed_run"
                ).get("started_at")
                if job.get("most_recent_completed_run")
                else None,
                "most_recent_completed_run_finished_at": job.get(
                    "most_recent_completed_run"
                ).get("finished_at")
                if job.get("most_recent_completed_run")
                else None,
                "schedule": job.get("schedule").get("cron")
                if job.get("schedule")
                else None,
                "next_run": job.get("next_run"),
            }
            for job in data
        ]

        return filtered_data

    async def get_job_details(self, account_id: int, job_id: int) -> dict[str, Any]:
        """Get details for a specific job."""
        result = await self._make_request(
            "GET",
            f"/api/v2/accounts/{account_id}/jobs/{job_id}/?include_related=['most_recent_run','most_recent_completed_run']",
        )
        return result.get("data", {})

    async def trigger_job_run(
        self, account_id: int, job_id: int, cause: str, **kwargs
    ) -> dict[str, Any]:
        """Trigger a job run."""
        data = {"cause": cause, **kwargs}
        result = await self._make_request(
            "POST", f"/api/v2/accounts/{account_id}/jobs/{job_id}/run/", json=data
        )
        return result.get("data", {})

    async def list_jobs_runs(self, account_id: int, **params) -> list[dict[str, Any]]:
        """List runs for an account."""
        extra_info = "?include_related=['job']"
        result = await self._make_request(
            "GET", f"/api/v2/accounts/{account_id}/runs/{extra_info}", params=params
        )

        data = result.get("data", [])

        # we remove less relevant fields from the data we get to avoid filling the context with too much data
        for run in data:
            run["job_name"] = run.get("job", {}).get("name", "")
            run["job_steps"] = run.get("job", {}).get("execute_step", "")
            run.pop("job", None)
            run.pop("account_id", None)
            run.pop("environment_id", None)
            run.pop("blocked_by", None)
            run.pop("used_repo_cache", None)
            run.pop("audit", None)
            run.pop("created_at_humanized", None)
            run.pop("duration_humanized", None)
            run.pop("finished_at_humanized", None)
            run.pop("queued_duration_humanized", None)
            run.pop("run_duration_humanized", None)
            run.pop("artifacts_saved", None)
            run.pop("artifact_s3_path", None)
            run.pop("has_docs_generated", None)
            run.pop("has_sources_generated", None)
            run.pop("notifications_sent", None)
            run.pop("executed_by_thread_id", None)
            run.pop("updated_at", None)
            run.pop("dequeued_at", None)
            run.pop("last_checked_at", None)
            run.pop("last_heartbeat_at", None)
            run.pop("trigger", None)
            run.pop("run_steps", None)
            run.pop("deprecation", None)
            run.pop("environment", None)

        return data

    async def get_job_run_details(self, account_id: int, run_id: int) -> dict[str, Any]:
        """Get details for a specific job run."""

        incl = "?include_related=['run_steps']"
        result = await self._make_request(
            "GET", f"/api/v2/accounts/{account_id}/runs/{run_id}/{incl}"
        )
        data = result.get("data", {})

        # we remove the truncated debug logs and logs, they are not very relevant
        for step in data.get("run_steps", []):
            step.pop("truncated_debug_logs", None)
            step.pop("logs", None)

        return data

    async def cancel_job_run(self, account_id: int, run_id: int) -> dict[str, Any]:
        """Cancel a job run."""
        result = await self._make_request(
            "POST", f"/api/v2/accounts/{account_id}/runs/{run_id}/cancel/"
        )
        return result.get("data", {})

    async def retry_job_run(self, account_id: int, run_id: int) -> dict[str, Any]:
        """Retry a failed job run."""
        result = await self._make_request(
            "POST", f"/api/v2/accounts/{account_id}/runs/{run_id}/retry/"
        )
        return result.get("data", {})

    async def list_job_run_artifacts(self, account_id: int, run_id: int) -> list[str]:
        """List artifacts for a job run."""
        result = await self._make_request(
            "GET", f"/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/"
        )
        data = result.get("data", [])

        # we remove the compiled and run artifacts, they are not very relevant and there are thousands of them, filling the context
        filtered_data = [
            artifact
            for artifact in data
            if (
                not artifact.startswith("compiled/") and not artifact.startswith("run/")
            )
        ]
        return filtered_data

    async def get_job_run_artifact(
        self,
        account_id: int,
        run_id: int,
        artifact_path: str,
        step: int | None = None,
    ) -> Any:
        """Get a specific job run artifact."""
        params = {}
        if step:
            params["step"] = step

        config = await self.get_config()
        get_artifact_header = {
            "Accept": "*/*",
        } | config.headers_provider.get_headers()

        response = requests.get(
            f"{config.url}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/{artifact_path}",
            headers=get_artifact_header,
            params=params,
        )
        response.raise_for_status()
        return response.text
