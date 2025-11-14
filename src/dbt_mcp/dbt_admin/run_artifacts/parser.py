import asyncio
import logging
import re
from typing import Any

from dbt_mcp.config.config_providers import AdminApiConfig
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient

from dbt_mcp.dbt_admin.constants import (
    STATUS_MAP,
    TRUNCATED_LOGS_LENGTH,
    JobRunStatus,
    RunResultsStatus,
)
from dbt_mcp.dbt_admin.run_artifacts.config import (
    OutputResultSchema,
    OutputStepSchema,
    RunDetailsSchema,
    RunResultsArtifactSchema,
    RunResultSchema,
    RunStepSchema,
    SourcesArtifactSchema,
)
from dbt_mcp.errors import ArtifactRetrievalError
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class JobRunFetcher:
    """Base class for fetching and parsing dbt Cloud job run artifacts.

    Provides shared functionality for ErrorFetcher and WarningFetcher to:
    - Fetch run_results.json artifacts from specific steps
    - Fetch sources.json artifacts for source freshness checks
    - Parse source freshness results (errors/warnings)

    Both ErrorFetcher and WarningFetcher inherit from this class but process
    different sets of steps:
    - ErrorFetcher: processes failed steps (status == ERROR)
    - WarningFetcher: processes successful steps (status == SUCCESS)

    Each fetcher instance is single-use and processes one job run analysis.
    """

    def __init__(
        self,
        run_id: int,
        run_details: dict[str, Any],
        client: DbtAdminAPIClient,
        admin_api_config: AdminApiConfig,
    ):
        """Initialize the job run fetcher.

        Args:
            run_id: The dbt Cloud job run ID
            run_details: Raw job run details from the dbt Admin API
            client: dbt Admin API client for making artifact requests
            admin_api_config: Configuration containing account_id and credentials
        """
        self.run_id = run_id
        self.run_details = run_details
        self.client = client
        self.admin_api_config = admin_api_config

    async def _fetch_run_results_artifact(self, step: RunStepSchema) -> str | None:
        """Fetch run_results.json artifact for the step."""
        step_index = step.index

        try:
            if step_index is not None:
                run_results_content = await self.client.get_job_run_artifact(
                    self.admin_api_config.account_id,
                    self.run_id,
                    "run_results.json",
                    step=step_index,
                )
                logger.info(f"Got run_results.json from step {step_index}")
                return run_results_content
            return None
        except ArtifactRetrievalError as e:
            logger.debug(f"No run_results.json for step {step_index}: {e}")
            return None

    async def _fetch_sources_artifact(self, step: RunStepSchema) -> str | None:
        """Fetch sources.json artifact for the step."""
        step_index = step.index

        try:
            if step_index is not None:
                sources_content = await self.client.get_job_run_artifact(
                    self.admin_api_config.account_id,
                    self.run_id,
                    "sources.json",
                    step=step_index,
                )
                logger.info(f"Got sources.json from step {step_index}")
                return sources_content
            return None
        except ArtifactRetrievalError as e:
            logger.debug(f"No sources.json for step {step_index}: {e}")
            return None

    async def _check_source_freshness_results(
        self,
        step: RunStepSchema,
        status_filter: list[str],
        message_prefix: str,
        result_status: str | None = None,
    ) -> OutputStepSchema | None:
        """
        Check for source freshness results (errors/warnings) in sources.json artifact.
        Note: Only method used by both ErrorFetcher and WarningFetcher that differ in usage
        with params so including a complete docstring.

        Args:
            step: The run step to check for source freshness results
            status_filter: List of status values to filter for (["error", "fail"] or ["warn"])
            message_prefix: Prefix for the result message ("Source freshness error")
            result_status: Optional status value to set in OutputResultSchema ("warn")
        """
        sources_content = await self._fetch_sources_artifact(step)

        if not sources_content:
            return None

        try:
            sources = SourcesArtifactSchema.model_validate_json(sources_content)

            message_suffix = (
                " (max allowed exceeded)" if "error" in message_prefix.lower() else ""
            )

            results = [
                OutputResultSchema(
                    unique_id=result.unique_id,
                    relation_name=result.unique_id.split(".")[-1]
                    if result.unique_id
                    else "Unknown",
                    message=f"{message_prefix}: {result.max_loaded_at_time_ago_in_s or 0:.0f}s since last load{message_suffix}",
                    status=result_status,
                )
                for result in sources.results
                if result.status in status_filter
            ]

            if results:
                return OutputStepSchema(
                    target=None,
                    step_name=step.name,
                    finished_at=step.finished_at,
                    results=results,
                )
            return None
        except ValidationError as e:
            logger.debug(f"sources.json validation failed: {e}")
            return None
        except Exception as e:
            logger.debug(f"Error parsing sources.json: {e}")
            return None


class ErrorFetcher(JobRunFetcher):
    """Parses dbt Cloud job run data to extract focused error information."""

    async def analyze_run_errors(self) -> dict[str, Any]:
        """Parse the run data and return all failed steps with their details."""
        try:
            run_details = RunDetailsSchema.model_validate(self.run_details)
            failed_steps = self._find_failed_steps(run_details)

            if run_details.is_cancelled:
                error_result = self._create_error_result(
                    message="Job run was cancelled",
                    finished_at=run_details.finished_at,
                )
                return {"failed_steps": [error_result.model_dump()]}

            if not failed_steps:
                error_result = self._create_error_result("No failed step found")
                return {"failed_steps": [error_result.model_dump()]}

            # asyncio.gather to process steps in parallel
            step_results = await asyncio.gather(
                *[self._get_step_errors(step) for step in failed_steps],
                return_exceptions=True,
            )

            processed_steps: list[OutputStepSchema] = []
            for step_result in step_results:
                if isinstance(step_result, BaseException):
                    logger.warning(f"Error processing failed step: {step_result}")
                    continue
                processed_steps.append(step_result)

            return {"failed_steps": [step.model_dump() for step in processed_steps]}

        except ValidationError as e:
            logger.error(f"Schema validation failed for run {self.run_id}: {e}")
            error_result = self._create_error_result(f"Validation failed: {e!s}")
            return {"failed_steps": [error_result.model_dump()]}
        except Exception as e:
            logger.error(f"Error analyzing run {self.run_id}: {e}")
            error_result = self._create_error_result(str(e))
            return {"failed_steps": [error_result.model_dump()]}

    def _find_failed_steps(self, run_details: RunDetailsSchema) -> list[RunStepSchema]:
        """Find all failed steps in the run."""
        failed_steps = []
        for step in run_details.run_steps:
            if step.status == STATUS_MAP[JobRunStatus.ERROR]:
                failed_steps.append(step)
        return failed_steps

    async def _get_step_errors(self, failed_step: RunStepSchema) -> OutputStepSchema:
        """Get simplified error information from failed step."""
        run_results_content = await self._fetch_run_results_artifact(failed_step)

        if not run_results_content:
            # Try to get structured errors from sources.json for source freshness checks
            source_errors = await self._check_source_freshness_errors(failed_step)
            if source_errors:
                return source_errors
            # Fall back to truncated logs if no structured errors available
            return self._handle_artifact_error(failed_step)

        return self._parse_run_results(run_results_content, failed_step)

    def _parse_run_results(
        self, run_results_content: str, failed_step: RunStepSchema
    ) -> OutputStepSchema:
        """Parse run_results.json content and extract errors."""
        try:
            run_results = RunResultsArtifactSchema.model_validate_json(
                run_results_content
            )
            errors = self._extract_errors_from_results(run_results.results)

            return self._build_error_response(errors, failed_step, run_results.args)

        except ValidationError as e:
            logger.warning(f"run_results.json validation failed: {e}")
            return self._handle_artifact_error(failed_step)
        except Exception as e:
            logger.warning(f"run_results.json parsing failed: {e}")
            return self._handle_artifact_error(failed_step)

    def _extract_errors_from_results(
        self, results: list[RunResultSchema]
    ) -> list[OutputResultSchema]:
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
                error = OutputResultSchema(
                    unique_id=result.unique_id,
                    relation_name=relation_name,
                    message=result.message or "",
                    compiled_code=result.compiled_code,
                )
                errors.append(error)
        return errors

    def _build_error_response(
        self,
        errors: list[OutputResultSchema],
        failed_step: RunStepSchema,
        args: Any | None,
    ) -> OutputStepSchema:
        """Build the final error response structure."""
        target = args.target if args else None
        step_name = failed_step.name
        finished_at = failed_step.finished_at
        truncated_logs = self._get_truncated_logs(failed_step)

        if errors:
            return OutputStepSchema(
                results=errors,
                step_name=step_name,
                finished_at=finished_at,
                target=target,
            )

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
    ) -> OutputStepSchema:
        """Create a standardized error results using OutputStepSchema."""
        error = OutputResultSchema(
            unique_id=unique_id,
            relation_name=relation_name,
            message=message,
            compiled_code=compiled_code,
            truncated_logs=truncated_logs,
        )
        return OutputStepSchema(
            results=[error],
            step_name=step_name,
            finished_at=finished_at,
            target=target,
        )

    async def _check_source_freshness_errors(
        self, step: RunStepSchema
    ) -> OutputStepSchema | None:
        """Check for source freshness errors in sources.json artifact."""
        return await self._check_source_freshness_results(
            step=step,
            status_filter=["error", "fail"],  # sources.json uses "fail" for errors
            message_prefix="Source freshness error",
            result_status=None,
        )

    def _handle_artifact_error(self, failed_step: RunStepSchema) -> OutputStepSchema:
        """Handle cases where run_results.json is not available."""
        relation_name = "No database relation"
        step_name = failed_step.name
        finished_at = failed_step.finished_at
        truncated_logs = self._get_truncated_logs(failed_step)

        message = "run_results.json not available - returning logs"

        return self._create_error_result(
            message=message,
            relation_name=relation_name,
            step_name=step_name,
            finished_at=finished_at,
            truncated_logs=truncated_logs,
        )

    def _get_truncated_logs(self, step: RunStepSchema) -> str | None:
        """Truncate logs to the last N lines."""
        split_logs = step.logs.splitlines() if step.logs else []
        if len(split_logs) > TRUNCATED_LOGS_LENGTH:  # N = 50
            split_logs = [
                f"Logs truncated to last {TRUNCATED_LOGS_LENGTH} lines..."
            ] + split_logs[-TRUNCATED_LOGS_LENGTH:]
        return "\n".join(split_logs)


class WarningFetcher(JobRunFetcher):
    """Parser for extracting warning information from successful runs."""

    async def analyze_run_warnings(self) -> dict[str, Any]:
        """Parse the run data and return all structured / log warnings with their details."""
        try:
            run_details = RunDetailsSchema.model_validate(self.run_details)

            if run_details.is_cancelled:
                return self._empty_response("Run was cancelled")

            successful_steps = self._find_successful_steps(run_details)
            if not successful_steps:
                return self._empty_response("No successful steps found")

            warning_step_models, all_log_warning_results = await self._collect_warnings(
                successful_steps
            )
            deduplicated_log_warnings = self._deduplicate_log_warnings(
                all_log_warning_results
            )
            summary = self._create_summary(
                warning_step_models, deduplicated_log_warnings
            )

            return {
                "has_warnings": summary["total_warnings"] > 0,
                "warning_steps": [step.model_dump() for step in warning_step_models],
                "log_warnings": [
                    warning.model_dump() for warning in deduplicated_log_warnings
                ],
                "summary": summary,
            }

        except ValidationError as e:
            logger.error(f"Schema validation failed for run {self.run_id}: {e}")
            return self._empty_response(f"Validation failed: {e!s}")
        except Exception as e:
            logger.error(f"Error analyzing warnings for run {self.run_id}: {e}")
            return self._empty_response(str(e))

    def _find_successful_steps(
        self, run_details: RunDetailsSchema
    ) -> list[RunStepSchema]:
        """Find all successful steps in the run."""
        return [
            step
            for step in run_details.run_steps
            if step.status == STATUS_MAP[JobRunStatus.SUCCESS]
        ]

    async def _collect_warnings(
        self, successful_steps: list[RunStepSchema]
    ) -> tuple[list[OutputStepSchema], list[OutputResultSchema]]:
        """Collect warnings from all successful steps."""
        # asyncio.gather to process steps in parallel
        step_results = await asyncio.gather(
            *[self._get_step_warnings(step) for step in successful_steps],
            return_exceptions=True,
        )

        warning_steps: list[OutputStepSchema] = []
        all_log_warnings: list[OutputResultSchema] = []

        for step_result in step_results:
            if isinstance(step_result, BaseException):
                logger.warning(f"Error processing step: {step_result}")
                continue

            if step_result is not None:
                structured_warnings = [
                    warning for warning in step_result.results if warning.unique_id
                ]
                step_log_warnings = [
                    warning for warning in step_result.results if not warning.unique_id
                ]

                if structured_warnings:
                    warning_steps.append(
                        OutputStepSchema(
                            target=step_result.target,
                            step_name=step_result.step_name,
                            finished_at=step_result.finished_at,
                            results=structured_warnings,
                        )
                    )

                if step_log_warnings:
                    all_log_warnings.extend(step_log_warnings)

        return warning_steps, all_log_warnings

    async def _get_step_warnings(self, step: RunStepSchema) -> OutputStepSchema | None:
        """Get warnings from a single successful step."""
        run_results_content = await self._fetch_run_results_artifact(step)

        result: OutputStepSchema | None = None
        if run_results_content:
            result = self._parse_run_results_for_warnings(run_results_content, step)
        else:
            result = await self._check_source_freshness_warnings(step)

        log_warnings = self._extract_log_warnings(step)

        if log_warnings:
            if result is not None:
                combined = result.results + log_warnings
                deduplicated = self._deduplicate_warning_results(combined)
                result = result.model_copy(update={"results": deduplicated})
            else:
                deduplicated = self._deduplicate_warning_results(log_warnings)
                result = OutputStepSchema(
                    target=None,
                    step_name=step.name,
                    finished_at=step.finished_at,
                    results=deduplicated,
                )
        elif result is not None:
            deduplicated = self._deduplicate_warning_results(result.results)
            result = result.model_copy(update={"results": deduplicated})

        return result

    def _parse_run_results_for_warnings(
        self, run_results_content: str, step: RunStepSchema
    ) -> OutputStepSchema | None:
        """Parse run_results.json content and extract structured warnings."""
        try:
            run_results = RunResultsArtifactSchema.model_validate_json(
                run_results_content
            )
            warnings = self._extract_warnings_from_results(run_results.results)

            if warnings:
                return OutputStepSchema(
                    target=run_results.args.target if run_results.args else None,
                    step_name=step.name,
                    finished_at=step.finished_at,
                    results=warnings,
                )
            return None
        except (ValidationError, Exception) as e:
            logger.warning(f"run_results.json parsing failed: {e}")
            return None

    def _extract_warnings_from_results(
        self, results: list[RunResultSchema]
    ) -> list[OutputResultSchema]:
        """Extract warning results from run results."""
        warnings = []
        for result in results:
            if result.status == RunResultsStatus.WARN.value:
                warnings.append(
                    OutputResultSchema(
                        unique_id=result.unique_id,
                        relation_name=result.relation_name or "No database relation",
                        message=result.message or "Warning detected",
                        status="warn",
                        compiled_code=result.compiled_code,
                    )
                )
        return warnings

    async def _check_source_freshness_warnings(
        self, step: RunStepSchema
    ) -> OutputStepSchema | None:
        """Check for source freshness warnings in sources.json artifact."""
        return await self._check_source_freshness_results(
            step=step,
            status_filter=["warn"],
            message_prefix="Source freshness warning",
            result_status="warn",
        )

    def _extract_log_warnings(self, step: RunStepSchema) -> list[OutputResultSchema]:
        """Extract warnings from step logs, matching on [WARNING] log entries."""
        if not step.logs:
            return []

        if "[WARNING]" not in step.logs:
            return []

        # TODO: Fusion logs differ from core (currently core only)
        # Need to explore and implement a solution for this

        # Remove ANSI color codes to focus on text
        ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
        clean_logs = ansi_escape.sub("", step.logs)
        lines = clean_logs.split("\n")
        warnings = []
        warning_pattern = r"\[WARNING\]"  # Standard warning log pattern in dbt Platform

        i = 0
        while i < len(lines):
            line = lines[i]

            if re.search(warning_pattern, line, re.IGNORECASE):
                message_lines = [line]
                j = i + 1

                # Capture continuation lines until next log entry or 5-line limit
                while j < len(lines):
                    next_line = lines[j]
                    # Timestamp pattern (HH:MM:SS) marks start of new log entry
                    if (
                        re.match(r"^\d{2}:\d{2}:\d{2}\s", next_line)
                        or len(message_lines) >= 5
                    ):
                        break
                    message_lines.append(next_line)
                    j += 1

                warnings.append(
                    OutputResultSchema(
                        unique_id=None,
                        relation_name=None,
                        message="\n".join(message_lines).strip(),
                        status="warn",
                    )
                )

                i = j
            else:
                i += 1

        return warnings

    def _deduplicate_warning_results(
        self, warnings: list[OutputResultSchema]
    ) -> list[OutputResultSchema]:
        """Deduplicate warnings based on unique_id or message content from run_results.json."""
        seen_unique_ids = set()
        seen_messages = set()
        deduplicated = []

        for warning in warnings:
            unique_id = warning.unique_id
            message = warning.message or ""

            if unique_id:
                if unique_id not in seen_unique_ids:
                    seen_unique_ids.add(unique_id)
                    deduplicated.append(warning)
            else:
                normalized_message = " ".join(message.split()).lower()
                if normalized_message and normalized_message not in seen_messages:
                    seen_messages.add(normalized_message)
                    deduplicated.append(warning)

        return deduplicated

    def _deduplicate_log_warnings(
        self, log_warnings: list[OutputResultSchema]
    ) -> list[OutputResultSchema]:
        """Deduplicate log warnings based on first line of text message content."""

        # Trade-off w/ current approach:
        # There is a chance that two different warnings with same first line get deduplicated
        # For example, if two deprecation warnings have an identical first line
        # but differ later in the nodes they refer to
        # However, so far in testing, this has not been an issue and greatly reduces noise
        # between repeated warnings in logs in multi-step runs

        seen_warning_keys = set()
        deduplicated = []

        for warning in log_warnings:
            message = warning.message or ""
            # Use only first line as the "core" identifier for deduplication
            first_line = message.split("\n")[0] if message else ""
            # Strip timestamps so same warning at different times matches
            first_line_clean = re.sub(r"^\d{2}:\d{2}:\d{2}\s+", "", first_line)
            # Normalize whitespace and case for matching
            normalized_key = " ".join(first_line_clean.split()).lower()

            if normalized_key and normalized_key not in seen_warning_keys:
                seen_warning_keys.add(normalized_key)
                deduplicated.append(warning)

        return deduplicated

    def _create_summary(
        self,
        warning_steps: list[OutputStepSchema],
        log_warning_steps: list[OutputResultSchema],
    ) -> dict[str, int]:
        """Create a summary of warning counts."""
        test_warnings = 0
        freshness_warnings = 0

        for step_result in warning_steps:
            for warning in step_result.results:
                unique_id = warning.unique_id or ""
                if "test." in unique_id:
                    test_warnings += 1
                elif "source." in unique_id:
                    freshness_warnings += 1

        log_warnings = len(log_warning_steps)
        total_warnings = test_warnings + freshness_warnings + log_warnings

        return {
            "total_warnings": total_warnings,
            "test_warnings": test_warnings,
            "freshness_warnings": freshness_warnings,
            "log_warnings": log_warnings,
        }

    def _empty_response(self, reason: str = "No warnings found") -> dict[str, Any]:
        """Return an empty warning response with a reason."""
        return {
            "has_warnings": False,
            "warning_steps": [],
            "log_warnings": [],
            "summary": {
                "total_warnings": 0,
                "test_warnings": 0,
                "freshness_warnings": 0,
                "log_warnings": 0,
            },
            "message": reason,
        }
