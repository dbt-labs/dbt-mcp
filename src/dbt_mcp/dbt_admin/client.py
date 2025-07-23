import logging
from functools import cache
from typing import Any, Dict, List, Optional
import requests

from dbt_mcp.config.config import RemoteConfig

logger = logging.getLogger(__name__)


class AdminAPIError(Exception):
    """Exception raised for Admin API errors."""
    pass


class DbtAdminAPIClient:
    """Client for interacting with dbt Cloud Admin API v2."""
    
    def __init__(self, config: RemoteConfig):
        self.config = config
        self.base_url = self._get_base_url()
        self.headers = {
            "Authorization": f"Bearer {config.token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
    def _get_base_url(self) -> str:
        """Get the base URL for the dbt API."""
        if self.config.multicell_account_prefix:
            return f"https://{self.config.multicell_account_prefix}.{self.config.host}"
        else:
            return f"https://{self.config.host}"
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make a request to the dbt API."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.request(method, url, headers=self.headers, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise AdminAPIError(f"API request failed: {e}")
    
    @cache
    def list_jobs(self, account_id: int, **params) -> List[Dict[str, Any]]:
        """List jobs for an account."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/jobs/?include_related=['most_recent_run','most_recent_completed_run']", params=params)
        return result.get("data", [])
    
    def get_job(self, account_id: int, job_id: int) -> Dict[str, Any]:
        """Get details for a specific job."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/jobs/{job_id}/?include_related=['most_recent_run','most_recent_completed_run']")
        return result.get("data", {})
    
    def trigger_job_run(self, account_id: int, job_id: int, cause: str, **kwargs) -> Dict[str, Any]:
        """Trigger a job run."""
        data = {"cause": cause, **kwargs}
        result = self._make_request("POST", f"/api/v2/accounts/{account_id}/jobs/{job_id}/run/", json=data)
        return result.get("data", {})
    
    def list_runs(self, account_id: int, **params) -> List[Dict[str, Any]]:
        """List runs for an account."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/runs/", params=params)
        return result.get("data", [])
    
    def get_run(self, account_id: int, run_id: int, debug: bool = False) -> Dict[str, Any]:
        """Get details for a specific run."""
        incl = "?include_related=['run_steps']"
        if debug:
            incl = "?include_related=['run_steps','debug_logs']"
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/runs/{run_id}/{incl}")
        data = result.get("data", {})

        # we remove the truncated debug logs that can be very big
        if not debug:
            for step in data.get("run_steps", []):
                step.pop("truncated_debug_logs", None)
        return data
    
    def cancel_run(self, account_id: int, run_id: int) -> Dict[str, Any]:
        """Cancel a run."""
        result = self._make_request("POST", f"/api/v2/accounts/{account_id}/runs/{run_id}/cancel/")
        return result.get("data", {})
    
    def retry_run(self, account_id: int, run_id: int) -> Dict[str, Any]:
        """Retry a failed run."""
        result = self._make_request("POST", f"/api/v2/accounts/{account_id}/runs/{run_id}/retry/")
        return result.get("data", {})
    
    def list_run_artifacts(self, account_id: int, run_id: int) -> List[str]:
        """List artifacts for a run."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/")
        return result.get("data", [])
    
    def get_run_artifact(self, account_id: int, run_id: int, artifact_path: str, step: Optional[int] = None) -> Any:
        """Get a specific run artifact."""
        params = {}
        if step:
            params["step"] = step
        response = requests.get(
            f"{self.base_url}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/{artifact_path}",
            headers=self.headers,
            params=params
        )
        response.raise_for_status()
        
        # Return raw content for artifacts
        if response.headers.get("content-type", "").startswith("application/json"):
            return response.json()
        else:
            return response.text


def get_admin_api_client(config: RemoteConfig) -> DbtAdminAPIClient:
    """Factory function to create a DbtAdminAPIClient instance."""
    return DbtAdminAPIClient(config)
