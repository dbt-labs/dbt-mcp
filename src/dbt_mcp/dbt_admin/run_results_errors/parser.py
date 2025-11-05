import logging
from typing import Any

from pydantic import ValidationError

from dbt_mcp.config.config_providers import AdminApiConfig
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.dbt_admin.constants import (
    SOURCE_FRESHNESS_STEP_NAME,
    STATUS_MAP,
    JobRunStatus,
    RunResultsStatus,
)
from dbt_mcp.dbt_admin.run_results_errors.config import (
    ErrorResultSchema,
    ErrorStepSchema,
    RunDetailsSchema,
    RunResultsArtifactSchema,
    RunResultSchema,
    RunStepSchema,
)
from dbt_mcp.errors import ArtifactRetrievalError

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
            error_result = self._create_error_result(f"Validation failed: {e!s}")
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
            return self._handle_artifact_error(failed_step)

        return self._parse_run_results(run_results_content, failed_step)

    async def _fetch_run_results_artifact(
        self, failed_step: RunStepSchema
    ) -> str | None:
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
                raise ArtifactRetrievalError(
                    "No step index available for artifact retrieval"
                )

        except Exception as e:
            logger.error(f"Failed to get run_results.json from step {step_index}: {e}")
            return None

    def _parse_run_results(
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
            return self._handle_artifact_error(failed_step, e)
        except Exception as e:
            return self._handle_artifact_error(failed_step, e)

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
        args: Any | None,
    ) -> dict[str, Any]:
        """Build the final error response structure."""
        target = args.target if args else None
        step_name = failed_step.name
        finished_at = failed_step.finished_at
        truncated_logs = self._truncated_logs(failed_step)

        if errors:
            return ErrorStepSchema(
                errors=errors,
                step_name=step_name,
                finished_at=finished_at,
                target=target,
            ).model_dump()

        message = "No failures found in run_results.json"

        return self._create_error_result(
            message=message,
            target=target,
            step_name=step_name,
            finished_at=finished_at,
            truncated_logs=truncated_logs,
        )

    def _create_error_result(
        self,
        message: str,
        unique_id: str | None = None,
        relation_name: str | None = None,
        target: str | None = None,
        step_name: str | None = None,
        finished_at: str | None = None,
        compiled_code: str | None = None,
        truncated_logs: str | None = None,
    ) -> dict[str, Any]:
        """Create a standardized error results using ErrorStepSchema."""
        error = ErrorResultSchema(
            unique_id=unique_id,
            relation_name=relation_name,
            message=message,
            compiled_code=compiled_code,
            truncated_logs=truncated_logs,
        )
        return ErrorStepSchema(
            errors=[error],
            step_name=step_name,
            finished_at=finished_at,
            target=target,
        ).model_dump()

    def _handle_artifact_error(
        self, failed_step: RunStepSchema, error: Exception | None = None
    ) -> dict[str, Any]:
        """Handle cases where run_results.json is not available."""
        relation_name = "No database relation"
        step_name = failed_step.name
        finished_at = failed_step.finished_at
        truncated_logs = self._truncated_logs(failed_step)

        # Special handling for source freshness steps
        if SOURCE_FRESHNESS_STEP_NAME.lower() in step_name.lower():
            message = "Source freshness error - returning logs"
        else:
            message = "run_results.json not available - returning logs"

        return self._create_error_result(
            message=message,
            relation_name=relation_name,
            step_name=step_name,
            finished_at=finished_at,
            truncated_logs=truncated_logs,
        )

    def _truncated_logs(self, failed_step: RunStepSchema) -> str | None:
        """Truncate logs to the last 50 lines."""
        TRUNCATED_LOGS_LENGTH = 50

        split_logs = failed_step.logs.splitlines() if failed_step.logs else []
        if len(split_logs) > TRUNCATED_LOGS_LENGTH:
            split_logs = [
                f"Logs truncated to last {TRUNCATED_LOGS_LENGTH} lines..."
            ] + split_logs[-TRUNCATED_LOGS_LENGTH:]
        return "\n".join(split_logs)
