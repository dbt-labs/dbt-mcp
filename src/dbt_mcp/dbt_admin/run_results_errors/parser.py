import json
import logging
from typing import Any, Dict, List, Optional
from pydantic import ValidationError

from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.dbt_admin.run_results_errors.config import (
    RunDetailsSchema,
    RunResultsArtifactSchema,
    ErrorResultSchema,
    MultiErrorResultSchema,
)

logger = logging.getLogger(__name__)

# Should re-use the JobRunStatus enum / STATUS_MAP in dbt_admin/tools.py somehow ?
# Will lead to circular import so keeping separate for now
ERROR_STATUS_CODES = [20]
ERROR_STATUS_HUMANIZED = ["error", "fail"]
SOURCE_FRESHNESS_STEP_NAME = "source freshness"


class ErrorFetcher:
    """Parses dbt Cloud job run data to extract focused error information."""

    def __init__(
        self,
        run_id: int,
        run_details: Dict[str, Any],
        client: DbtAdminAPIClient,
    ):
        """
        Initialize parser with run data.

        Args:
            run_id: dbt Cloud job run ID
            run_details: Raw run details from get_job_run_details()
            client: DbtAdminAPIClient instance for fetching artifacts
        """
        self.run_id = run_id
        self.run_details = run_details
        self.client = client

    def analyze_run_errors(self) -> Dict[str, Any]:
        """
        Parse the run data and return simplified failure details with validation.

        Returns:
            Structured error information
        """
        try:
            run_details = RunDetailsSchema.model_validate(self.run_details)

            failed_step = self._find_failed_step(run_details.model_dump())

            if not failed_step:
                return self._create_error_result("No failed step found")

            result = self._get_failure_details(failed_step)

            return MultiErrorResultSchema.model_validate(result).model_dump()

        except ValidationError as e:
            logger.error(f"Schema validation failed for run {self.run_id}: {e}")
            return self._create_error_result(f"Validation failed: {str(e)}")
        except Exception as e:
            logger.error(f"Error analyzing run {self.run_id}: {e}")
            return self._create_error_result(str(e))

    def _find_failed_step(self, run_details: Dict) -> Optional[Dict[str, Any]]:
        """Find the first failed step in the run."""
        for step in run_details.get("run_steps", []):
            if step.get("status") in ERROR_STATUS_CODES:
                return {
                    "name": step.get("name"),
                    "finished_at": step.get("finished_at"),
                    "status_humanized": step.get("status_humanized"),
                    "index": step.get("index"),
                }
        return None

    def _get_failure_details(self, failed_step: Dict) -> Dict[str, Any]:
        """Get simplified failure information from failed step."""
        run_results_content = self._fetch_run_results_artifact(failed_step)

        if not run_results_content:
            return self._handle_artifact_error(failed_step)

        return self._parse_run_results(run_results_content, failed_step)

    def _fetch_run_results_artifact(self, failed_step: Dict[str, Any]) -> Optional[str]:
        """Fetch run_results.json artifact for the failed step."""
        step_index = failed_step.get("index")

        try:
            if step_index is not None:
                run_results_content = self.client.get_job_run_artifact(
                    self.client.config.account_id,
                    self.run_id,
                    "run_results.json",
                    step=step_index,
                )
                logger.info(f"Got run_results.json from failed step {step_index}")
                return run_results_content
            else:
                raise ValueError("No step index available for artifact retrieval")

        except Exception as e:
            logger.error(f"Failed to get run_results.json from step {step_index}: {e}")
            return None

    def _parse_run_results(
        self, run_results_content: str, failed_step: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Parse run_results.json content and extract errors."""
        try:
            run_results_data = json.loads(run_results_content)
            run_results = RunResultsArtifactSchema.model_validate(run_results_data)
            target = run_results_data.get("args", {}).get("target")
            errors = self._extract_errors_from_results(run_results.results)

            return self._build_error_response(errors, failed_step, target)

        except ValidationError as e:
            logger.warning(f"run_results.json validation failed: {e}")
            return self._handle_artifact_error(failed_step, e)
        except Exception as e:
            return self._handle_artifact_error(failed_step, e)

    def _extract_errors_from_results(
        self, results: List[Any]
    ) -> List[ErrorResultSchema]:
        """Extract error results from run results."""
        errors = []
        for result in results:
            if result.status in ERROR_STATUS_HUMANIZED:
                error = ErrorResultSchema(
                    unique_id=result.unique_id,
                    relation_name=result.relation_name,
                    message=result.message or "",
                )
                errors.append(error)
        return errors

    def _build_error_response(
        self,
        errors: List[ErrorResultSchema],
        failed_step: Dict[str, Any],
        target: Optional[str],
    ) -> Dict[str, Any]:
        """Build the final error response structure."""
        if errors:
            return {
                "errors": [error.model_dump() for error in errors],
                "step_name": failed_step["name"],
                "finished_at": failed_step["finished_at"],
                "target": target,
            }

        return self._create_error_result(
            message="No failures found in run_results",
            target=target,
            step_name=failed_step["name"],
            finished_at=failed_step["finished_at"],
        )

    def _create_error_result(
        self,
        message: str,
        unique_id: Optional[str] = None,
        relation_name: Optional[str] = None,
        target: Optional[str] = None,
        step_name: Optional[str] = None,
        finished_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a standardized error results using MultiErrorResultSchema."""
        error = ErrorResultSchema(
            unique_id=unique_id,
            relation_name=relation_name,
            message=message,
        )
        result = MultiErrorResultSchema(
            errors=[error],
            step_name=step_name,
            finished_at=finished_at,
            target=target,
        )
        return result.model_dump()

    def _handle_artifact_error(
        self, failed_step: Dict, error: Optional[Exception] = None
    ) -> Dict[str, Any]:
        """Handle cases where run_results.json is not available."""
        step_name = failed_step.get("name", "")

        # Special handling for source freshness steps
        if SOURCE_FRESHNESS_STEP_NAME in step_name.lower():
            message = "Source freshness error: run_results.json not available"
        else:
            error_detail = str(error) if error else "not available"
            message = f"run_results.json not available: {error_detail}"

        return self._create_error_result(
            message=message,
            step_name=failed_step.get("name"),
            finished_at=failed_step.get("finished_at"),
        )
