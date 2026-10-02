import logging
from dataclasses import dataclass
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config_providers import (
    AdminApiConfig,
    ConfigProvider,
)
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.dbt_admin.artifacts import INLINE_CONTENT_LIMIT, InlineArtifactLimitError
from dbt_mcp.dbt_admin.constants import STATUS_MAP, JobRunStatus
from dbt_mcp.dbt_admin.param_descriptions import (
    ARTIFACT_JQ_FILTER,
    ARTIFACT_PATH,
    ARTIFACT_STEP,
    INCLUDE_WARNINGS_WITH_ERRORS,
    JOB_DEFINITION_ID,
    JOBS_PROJECT_ID_FILTER,
    JOB_RUN_ID,
    JOB_RUNS_JOB_DEFINITION_ID_FILTER,
    JOB_RUNS_ORDER_BY,
    JOB_RUN_STATUS,
    TRIGGER_CAUSE,
    TRIGGER_DBT_VERSION_OVERRIDE,
    TRIGGER_GIT_BRANCH,
    TRIGGER_GIT_SHA,
    TRIGGER_SCHEMA_OVERRIDE,
    TRIGGER_STEPS_OVERRIDE,
    WARNINGS_ONLY,
)
from dbt_mcp.dbt_admin.run_artifacts.parser import ErrorFetcher, WarningFetcher
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.errors import InvalidParameterError
from dbt_mcp.result_limits import ensure_result_size
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

from dbt_mcp.pagination import LIMIT_FIELD, OFFSET_FIELD, ResultPage, validate_page_size

logger = logging.getLogger(__name__)


@dataclass
class AdminToolContext:
    admin_client: DbtAdminAPIClient
    admin_api_config_provider: ConfigProvider[AdminApiConfig]

    def __init__(self, admin_api_config_provider: ConfigProvider[AdminApiConfig]):
        self.admin_api_config_provider = admin_api_config_provider
        self.admin_client = DbtAdminAPIClient(admin_api_config_provider)


@dbt_mcp_tool(
    description=get_prompt("admin_api/list_projects"),
    title="List Projects",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_projects(
    context: AdminToolContext,
    limit: Annotated[int, LIMIT_FIELD] = 50,
    offset: Annotated[int, OFFSET_FIELD] = 0,
) -> ResultPage[list[dict[str, Any]]]:
    """List active projects in the account."""
    validate_page_size(limit, offset)
    admin_api_config = await context.admin_api_config_provider.get_config()
    return await context.admin_client.list_projects(
        admin_api_config.account_id, limit=limit, offset=offset
    )


@dbt_mcp_tool(
    description=get_prompt("admin_api/list_jobs"),
    title="List Jobs",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_jobs(
    context: AdminToolContext,
    limit: Annotated[int, LIMIT_FIELD] = 50,
    offset: Annotated[int, OFFSET_FIELD] = 0,
    *,
    project_id: Annotated[
        int | None, Field(description=JOBS_PROJECT_ID_FILTER, gt=0)
    ] = None,
) -> ResultPage[list[dict[str, Any]]]:
    """List jobs in an account, optionally across all environments of a project."""
    validate_page_size(limit, offset)
    admin_api_config = await context.admin_api_config_provider.get_config()
    params = {}
    if project_id is not None:
        params["project_id"] = project_id
    elif admin_api_config.prod_environment_id:
        params["environment_id"] = admin_api_config.prod_environment_id
    params["limit"] = limit
    params["offset"] = offset
    return await context.admin_client.list_jobs(admin_api_config.account_id, **params)


@dbt_mcp_tool(
    description=get_prompt("admin_api/get_job_details"),
    title="Get Job Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_job_details(
    context: AdminToolContext,
    job_id: Annotated[int, Field(description=JOB_DEFINITION_ID)],
) -> dict[str, Any]:
    """Get details for a specific job."""
    admin_api_config = await context.admin_api_config_provider.get_config()
    return await context.admin_client.get_job_details(
        admin_api_config.account_id, job_id
    )


@dbt_mcp_tool(
    description=get_prompt("admin_api/trigger_job_run"),
    title="Trigger Job Run",
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=False,
)
async def trigger_job_run(
    context: AdminToolContext,
    job_id: Annotated[int, Field(description=JOB_DEFINITION_ID)],
    cause: Annotated[str, Field(description=TRIGGER_CAUSE)] = "Triggered by dbt MCP",
    git_branch: Annotated[str | None, Field(description=TRIGGER_GIT_BRANCH)] = None,
    git_sha: Annotated[str | None, Field(description=TRIGGER_GIT_SHA)] = None,
    schema_override: Annotated[
        str | None, Field(description=TRIGGER_SCHEMA_OVERRIDE)
    ] = None,
    steps_override: Annotated[
        list[str] | None, Field(description=TRIGGER_STEPS_OVERRIDE)
    ] = None,
    dbt_version_override: Annotated[
        str | None, Field(description=TRIGGER_DBT_VERSION_OVERRIDE)
    ] = None,
) -> dict[str, Any]:
    """Trigger a job run."""
    admin_api_config = await context.admin_api_config_provider.get_config()
    kwargs: dict[str, str | list[str]] = {}
    if git_branch:
        kwargs["git_branch"] = git_branch
    if git_sha:
        kwargs["git_sha"] = git_sha
    if schema_override:
        kwargs["schema_override"] = schema_override
    if steps_override is not None:
        kwargs["steps_override"] = steps_override
    if dbt_version_override:
        kwargs["dbt_version_override"] = dbt_version_override
    return await context.admin_client.trigger_job_run(
        admin_api_config.account_id, job_id, cause, **kwargs
    )


@dbt_mcp_tool(
    description=get_prompt("admin_api/list_jobs_runs"),
    title="List Jobs Runs",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_jobs_runs(
    context: AdminToolContext,
    job_id: Annotated[
        int | None, Field(description=JOB_RUNS_JOB_DEFINITION_ID_FILTER)
    ] = None,
    status: Annotated[JobRunStatus | None, Field(description=JOB_RUN_STATUS)] = None,
    limit: Annotated[int, LIMIT_FIELD] = 50,
    offset: Annotated[int, OFFSET_FIELD] = 0,
    order_by: Annotated[str | None, Field(description=JOB_RUNS_ORDER_BY)] = None,
) -> ResultPage[list[dict[str, Any]]]:
    """List runs in an account."""
    validate_page_size(limit, offset)
    admin_api_config = await context.admin_api_config_provider.get_config()
    params: dict[str, Any] = {}
    if job_id:
        params["job_definition_id"] = job_id
    if status:
        status_id = STATUS_MAP[status]
        params["status"] = status_id
    params["limit"] = limit
    params["offset"] = offset
    if order_by:
        params["order_by"] = order_by
    return await context.admin_client.list_jobs_runs(
        admin_api_config.account_id, **params
    )


@dbt_mcp_tool(
    description=get_prompt("admin_api/get_job_run_details"),
    title="Get Job Run Details",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_job_run_details(
    context: AdminToolContext,
    run_id: Annotated[int, Field(description=JOB_RUN_ID)],
) -> dict[str, Any]:
    """Get details for a specific job run."""
    admin_api_config = await context.admin_api_config_provider.get_config()
    return await context.admin_client.get_job_run_details(
        admin_api_config.account_id, run_id
    )


@dbt_mcp_tool(
    description=get_prompt("admin_api/cancel_job_run"),
    title="Cancel Job Run",
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=False,
)
async def cancel_job_run(
    context: AdminToolContext,
    run_id: Annotated[int, Field(description=JOB_RUN_ID)],
) -> dict[str, Any]:
    """Cancel a job run."""
    admin_api_config = await context.admin_api_config_provider.get_config()
    return await context.admin_client.cancel_job_run(
        admin_api_config.account_id, run_id
    )


@dbt_mcp_tool(
    description=get_prompt("admin_api/retry_job_run"),
    title="Retry Job Run",
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=False,
)
async def retry_job_run(
    context: AdminToolContext,
    run_id: Annotated[int, Field(description=JOB_RUN_ID)],
) -> dict[str, Any]:
    """Retry a failed job run."""
    admin_api_config = await context.admin_api_config_provider.get_config()
    return await context.admin_client.retry_job_run(admin_api_config.account_id, run_id)


@dbt_mcp_tool(
    description=get_prompt("admin_api/list_job_run_artifacts"),
    title="List Job Run Artifacts",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def list_job_run_artifacts(
    context: AdminToolContext,
    run_id: Annotated[int, Field(description=JOB_RUN_ID)],
) -> list[str]:
    """List artifacts for a job run."""
    admin_api_config = await context.admin_api_config_provider.get_config()
    return await context.admin_client.list_job_run_artifacts(
        admin_api_config.account_id, run_id
    )


@dbt_mcp_tool(
    description=get_prompt("admin_api/get_job_run_artifacts"),
    title="Get Job Run Artifacts",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_job_run_artifacts(
    context: AdminToolContext,
    run_id: Annotated[int, Field(description=JOB_RUN_ID)],
    artifact_path: Annotated[str, Field(description=ARTIFACT_PATH)],
    step: Annotated[int | None, Field(ge=1, description=ARTIFACT_STEP)] = None,
    jq_filter: Annotated[str | None, Field(description=ARTIFACT_JQ_FILTER)] = None,
) -> str:
    """Get a specific artifact from a job run."""
    admin_api_config = await context.admin_api_config_provider.get_config()
    if jq_filter is not None:
        return await context.admin_client.get_job_run_artifact(
            admin_api_config.account_id,
            run_id,
            artifact_path,
            step=step,
            jq_filter=jq_filter,
        )
    try:
        content = await context.admin_client.get_job_run_artifact(
            admin_api_config.account_id, run_id, artifact_path, step=step
        )
    except InlineArtifactLimitError:
        content = None
    if content is not None and len(content.encode("utf-8")) < INLINE_CONTENT_LIMIT:
        return content
    hint = (
        f"Artifact '{artifact_path}' (run {run_id}) is too large to "
        "return inline. Re-call get_job_run_artifacts with a jq_filter to extract "
        "just what you need. To explore structure first, try jq_filter='keys' or "
        "'.metadata'; to list nodes use '.nodes | keys'; to find failures use "
        "'.results[] | select(.status == \"error\")'."
    )
    if artifact_path.endswith("run_results.json"):
        hint += " For run failures specifically, use get_job_run_error tool."
    return hint


@dbt_mcp_tool(
    description=get_prompt("admin_api/get_job_run_error"),
    title="Get Job Run Error",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_job_run_error(
    context: AdminToolContext,
    run_id: Annotated[int, Field(description=JOB_RUN_ID)],
    include_warnings: Annotated[
        bool, Field(description=INCLUDE_WARNINGS_WITH_ERRORS)
    ] = False,
    warning_only: Annotated[bool, Field(description=WARNINGS_ONLY)] = False,
) -> dict[str, Any]:
    """Get focused error/warning information for a job run."""
    admin_api_config = await context.admin_api_config_provider.get_config()

    if warning_only:
        run_details = await context.admin_client.get_job_run_details(
            admin_api_config.account_id, run_id, include_logs=True
        )
        if len(run_details.get("run_steps", [])) > 20:
            raise InvalidParameterError(
                "Run has more than 20 steps; inspect a specific step with get_job_run_artifacts."
            )
        warning_fetcher = WarningFetcher(
            run_id, run_details, context.admin_client, admin_api_config
        )
        result = await warning_fetcher.analyze_run_warnings()
        ensure_result_size(result)
        return result

    run_details = await context.admin_client.get_job_run_details(
        admin_api_config.account_id, run_id, include_logs=True
    )
    if len(run_details.get("run_steps", [])) > 20:
        raise InvalidParameterError(
            "Run has more than 20 steps; inspect a specific step with get_job_run_artifacts."
        )
    error_fetcher = ErrorFetcher(
        run_id, run_details, context.admin_client, admin_api_config
    )
    error_result = await error_fetcher.analyze_run_errors()

    if include_warnings:
        warning_fetcher = WarningFetcher(
            run_id, run_details, context.admin_client, admin_api_config
        )
        warning_result = await warning_fetcher.analyze_run_warnings()

        result = {**error_result, "warnings": warning_result}
        ensure_result_size(result)
        return result

    ensure_result_size(error_result)
    return error_result


ADMIN_TOOLS = [
    list_projects,
    list_jobs,
    get_job_details,
    trigger_job_run,
    list_jobs_runs,
    get_job_run_details,
    cancel_job_run,
    retry_job_run,
    list_job_run_artifacts,
    get_job_run_artifacts,
    get_job_run_error,
]


def register_admin_api_tools(
    dbt_mcp: FastMCP,
    admin_config_provider: ConfigProvider[AdminApiConfig],
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    """Register dbt Admin API tools."""

    def bind_context() -> AdminToolContext:
        return AdminToolContext(admin_api_config_provider=admin_config_provider)

    register_tools(
        dbt_mcp,
        tool_definitions=[tool.adapt_context(bind_context) for tool in ADMIN_TOOLS],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
