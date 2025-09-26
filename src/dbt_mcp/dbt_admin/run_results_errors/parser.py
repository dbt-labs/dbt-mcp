import logging
from typing import Any, Optional
from pydantic import ValidationError

from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.config.config_providers import AdminApiConfig
from dbt_mcp.dbt_admin.constants import (
    JobRunStatus,
    RunResultsStatus,
    STATUS_MAP,
    SOURCE_FRESHNESS_STEP_NAME,
)
from dbt_mcp.dbt_admin.run_results_errors.config import (
    RunDetailsSchema,
    RunStepSchema,
    RunResultSchema,
    RunResultsArtifactSchema,
    ErrorResultSchema,
    ErrorStepSchema,
)


logger = logging.getLogger(__name__)


class ErrorFetcher:
    """Parses dbt Cloud job run data to extract focused error information."""

    def __init__(
        self,
        run_id: int,
        run_details: dict[str, Any],
        client: DbtAdminAPIClient,
        admin_api_config: AdminApiConfig,
    ):
        """
        Initialize parser with run data.
        Args:
            run_id: dbt Cloud job run ID
            run_details: Raw run details from get_job_run_details()
            client: DbtAdminAPIClient instance for fetching artifacts
            admin_api_config: Admin API configuration
        """
        self.run_id = run_id
        self.run_details = run_details
        self.client = client
        self.admin_api_config = admin_api_config
        self._run_details_with_logs = None  # Cache for run details with logs

    async def analyze_run_errors(self) -> dict[str, Any]:
        """Parse the run data and return all failed steps with their details."""
        try:
            run_details = RunDetailsSchema.model_validate(self.run_details)
            failed_steps = self._find_all_failed_steps(run_details)

            if run_details.is_cancelled:
                error_result = self._create_error_result(
                    message="Job run was cancelled",
                    finished_at=run_details.finished_at,
                )
                return {"failed_steps": [error_result]}

            if not failed_steps:
                error_result = self._create_error_result("No failed step found")
                return {"failed_steps": [error_result]}

            processed_steps = []
            for step in failed_steps:
                step_result = await self._get_failure_details(step)
                processed_steps.append(step_result)

            return {"failed_steps": processed_steps}

        except ValidationError as e:
            logger.error(f"Schema validation failed for run {self.run_id}: {e}")
            error_result = self._create_error_result(f"Validation failed: {str(e)}")
            return {"failed_steps": [error_result]}
        except Exception as e:
            logger.error(f"Error analyzing run {self.run_id}: {e}")
            error_result = self._create_error_result(str(e))
            return {"failed_steps": [error_result]}

    def _find_all_failed_steps(
        self, run_details: RunDetailsSchema
    ) -> list[RunStepSchema]:
        """Find all failed steps in the run."""
        failed_steps = []
        for step in run_details.run_steps:
            if step.status == STATUS_MAP[JobRunStatus.ERROR]:
                failed_steps.append(step)
        return failed_steps

    async def _get_failure_details(self, failed_step: RunStepSchema) -> dict[str, Any]:
        """Get simplified failure information from failed step."""
        run_results_content = await self._fetch_run_results_artifact(failed_step)

        if not run_results_content:
            return await self._handle_artifact_error(failed_step)

        return await self._parse_run_results(run_results_content, failed_step)

    async def _fetch_run_results_artifact(
        self, failed_step: RunStepSchema
    ) -> Optional[str]:
        """Fetch run_results.json artifact for the failed step."""
        step_index = failed_step.index

        try:
            if step_index is not None:
                run_results_content = await self.client.get_job_run_artifact(
                    self.admin_api_config.account_id,
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

    async def _parse_run_results(
        self, run_results_content: str, failed_step: RunStepSchema
    ) -> dict[str, Any]:
        """Parse run_results.json content and extract errors."""
        try:
            run_results = RunResultsArtifactSchema.model_validate_json(
                run_results_content
            )
            errors = self._extract_errors_from_results(run_results.results)

            return self._build_error_response(errors, failed_step, run_results.args)

        except ValidationError as e:
            logger.warning(f"run_results.json validation failed: {e}")
            return await self._handle_artifact_error(failed_step, e)
        except Exception as e:
            return await self._handle_artifact_error(failed_step, e)

    def _extract_errors_from_results(
        self, results: list[RunResultSchema]
    ) -> list[ErrorResultSchema]:
        """Extract error results from run results."""
        errors = []
        for result in results:
            if result.status in [
                RunResultsStatus.ERROR.value,
                RunResultsStatus.FAIL.value,
            ]:
                relation_name = (
                    result.relation_name
                    if result.relation_name is not None
                    else "No database relation"
                )
                error = ErrorResultSchema(
                    unique_id=result.unique_id,
                    relation_name=relation_name,
                    message=result.message or "",
                    compiled_code=result.compiled_code,
                )
                errors.append(error)
        return errors

    def _build_error_response(
        self,
        errors: list[ErrorResultSchema],
        failed_step: RunStepSchema,
        args: Optional[Any],
    ) -> dict[str, Any]:
        """Build the final error response structure."""
        target = args.target if args else None

        if errors:
            return ErrorStepSchema(
                errors=errors,
                step_name=failed_step.name,
                finished_at=failed_step.finished_at,
                target=target,
            ).model_dump()

        if (
            failed_step.status == STATUS_MAP[JobRunStatus.CANCELLED]
        ):  # Cancelled run with steps triggered
            message = "Job run was cancelled: run_results.json not available"
        else:
            message = "No failures found in run_results"

        return self._create_error_result(
            message=message,
            target=target,
            step_name=failed_step.name,
            finished_at=failed_step.finished_at,
        )

    def _create_error_result(
        self,
        message: str,
        unique_id: Optional[str] = None,
        relation_name: Optional[str] = None,
        target: Optional[str] = None,
        step_name: Optional[str] = None,
        finished_at: Optional[str] = None,
        compiled_code: Optional[str] = None,
    ) -> dict[str, Any]:
        """Create a standardized error results using ErrorStepSchema."""
        error = ErrorResultSchema(
            unique_id=unique_id,
            relation_name=relation_name,
            message=message,
            compiled_code=compiled_code,
        )
        return ErrorStepSchema(
            errors=[error],
            step_name=step_name,
            finished_at=finished_at,
            target=target,
        ).model_dump()

    async def _handle_artifact_error(
        self, failed_step: RunStepSchema, error: Optional[Exception] = None
    ) -> dict[str, Any]:
        """Handle cases where run_results.json is not available by fetching logs."""
        step_name = failed_step.name

        # Try to get logs for the failed step
        step_logs = await self._get_step_logs(failed_step)

        if step_logs:
            # Special handling for source freshness steps
            if SOURCE_FRESHNESS_STEP_NAME.lower() in step_name.lower():
                message = "Source freshness error detected from logs"
                compiled_code = ""
            else:
                message = "Error details from step logs"
                logs = step_logs.splitlines()
                if len(logs) > 50:
                    step_logs = "Logs truncated to last 50 lines\n" + "\n".join(logs[-50:])
                compiled_code = step_logs

            return self._create_error_result(
                message=message,
                step_name=step_name,
                finished_at=failed_step.finished_at,
                compiled_code=compiled_code,
            )
        else:
            # Fallback to original behavior when logs are also not available
            if SOURCE_FRESHNESS_STEP_NAME.lower() in step_name.lower():
                message = "Source freshness error: run_results.json not available"
            else:
                error_detail = str(error) if error else "not available"
                message = f"run_results.json not available: {error_detail}"

            return self._create_error_result(
                message=message,
                step_name=failed_step.name,
                finished_at=failed_step.finished_at,
            )

    async def _get_step_logs(self, failed_step: RunStepSchema) -> Optional[str]:
        """Get logs for a failed step by fetching run details with logs (cached)."""
        try:
            # Fetch run details with logs only once and cache it
            if self._run_details_with_logs is None:
                logger.info(f"Fetching run details with logs for run {self.run_id}")
                self._run_details_with_logs = await self.client.get_job_run_details(
                    self.admin_api_config.account_id, self.run_id, include_logs=True
                )

            # Find the matching step by index
            run_steps = self._run_details_with_logs.get("run_steps", [])
            for step in run_steps:
                if step.get("index") == failed_step.index:
                    logs = step.get("logs")
                    if logs:
                        logger.info(f"Retrieved logs for failed step {failed_step.index}")
                        return logs
                    break

            logger.warning(f"No logs found for failed step {failed_step.index}")
            return None

        except Exception as e:
            logger.error(f"Failed to get logs for step {failed_step.index}: {e}")
            return None
