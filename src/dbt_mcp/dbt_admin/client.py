import logging
from functools import cache
from typing import Any, Dict, List, Optional
import requests
from datetime import datetime

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
        """Get the base URL for the dbt Cloud API."""
        if self.config.multicell_account_prefix:
            return f"https://{self.config.multicell_account_prefix}.{self.config.host}"
        else:
            return f"https://{self.config.host}"
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make a request to the dbt Cloud API."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.request(method, url, headers=self.headers, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise AdminAPIError(f"API request failed: {e}")
    
    @cache
    def list_accounts(self) -> List[Dict[str, Any]]:
        """List all accounts accessible to the user."""
        result = self._make_request("GET", "/api/v2/accounts/")
        return result.get("data", [])
    
    def get_account(self, account_id: int) -> Dict[str, Any]:
        """Get details for a specific account."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/")
        return result.get("data", {})
    
    def list_projects(self, account_id: int, **params) -> List[Dict[str, Any]]:
        """List projects for an account."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/projects/", params=params)
        return result.get("data", [])
    
    def get_project(self, account_id: int, project_id: int) -> Dict[str, Any]:
        """Get details for a specific project."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/projects/{project_id}/")
        return result.get("data", {})
    
    def list_environments(self, account_id: int, **params) -> List[Dict[str, Any]]:
        """List environments for an account."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/environments/", params=params)
        return result.get("data", [])
    
    def get_environment(self, account_id: int, environment_id: int) -> Dict[str, Any]:
        """Get details for a specific environment."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/environments/{environment_id}/")
        return result.get("data", {})
    
    def list_jobs(self, account_id: int, **params) -> List[Dict[str, Any]]:
        """List jobs for an account."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/jobs/", params=params)
        return result.get("data", [])
    
    def get_job(self, account_id: int, job_id: int, include_related: Optional[str] = None) -> Dict[str, Any]:
        """Get details for a specific job."""
        params = {}
        if include_related:
            params["include_related"] = include_related
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/jobs/{job_id}/", params=params)
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
    
    def get_run(self, account_id: int, run_id: int, include_related: Optional[str] = None) -> Dict[str, Any]:
        """Get details for a specific run."""
        params = {}
        if include_related:
            params["include_related"] = include_related
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/runs/{run_id}/", params=params)
        return result.get("data", {})
    
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
    
    def get_run_step(self, account_id: int, step_id: int, include_related: Optional[str] = None) -> Dict[str, Any]:
        """Get details for a specific run step."""
        params = {}
        if include_related:
            params["include_related"] = include_related
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/steps/{step_id}/", params=params)
        return result.get("data", {})
    
    def list_users(self, account_id: int) -> List[Dict[str, Any]]:
        """List users in an account."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/users/")
        return result.get("data", [])
    
    def get_user(self, user_id: int) -> Dict[str, Any]:
        """Get details for a specific user."""
        result = self._make_request("GET", f"/api/v2/users/{user_id}/")
        return result.get("data", {})
    
    def list_notifications(self, account_id: int, **params) -> List[Dict[str, Any]]:
        """List notification configurations."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/notifications/", params=params)
        return result.get("data", [])
    
    def create_notification(self, account_id: int, notification_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new notification configuration."""
        result = self._make_request("POST", f"/api/v2/accounts/{account_id}/notifications/", json=notification_data)
        return result.get("data", {})
    
    def list_licenses(self, account_id: int) -> List[Dict[str, Any]]:
        """List license allocations for an account."""
        result = self._make_request("GET", f"/api/v2/accounts/{account_id}/licenses/")
        return result.get("data", [])


def get_admin_api_client(config: RemoteConfig) -> DbtAdminAPIClient:
    """Factory function to create a DbtAdminAPIClient instance."""
    return DbtAdminAPIClient(config)
