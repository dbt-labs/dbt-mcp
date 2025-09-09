from enum import Enum

from pydantic.dataclasses import dataclass

from dbt_mcp.tools.tool_names import ToolName


class ToolBehavior(Enum):
    """Behavior of the tool."""

    # The tool can return row-level data.
    RESULT_SET = "result_set"
    # The tool only returns metadata.
    METADATA = "metadata"


@dataclass
class ToolPolicy:
    """Policy for a tool."""

    name: str
    behavior: ToolBehavior


# Defining tool policies is important for our internal usage of dbt-mcp.
# Our policies dictate that we do not send row-level data to LLMs.
tool_policies = {
    # CLI tools
    ToolName.SHOW.value: ToolPolicy(
        name=ToolName.SHOW.value, behavior=ToolBehavior.RESULT_SET
    ),
    ToolName.LIST.value: ToolPolicy(
        name=ToolName.LIST.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.DOCS.value: ToolPolicy(
        name=ToolName.DOCS.value, behavior=ToolBehavior.METADATA
    ),
    # Compile tool can have result_set behavior because of macros like print_table
    ToolName.COMPILE.value: ToolPolicy(
        name=ToolName.COMPILE.value, behavior=ToolBehavior.RESULT_SET
    ),
    ToolName.TEST.value: ToolPolicy(
        name=ToolName.TEST.value, behavior=ToolBehavior.METADATA
    ),
    # Run tool can have result_set behavior because of macros like print_table
    ToolName.RUN.value: ToolPolicy(
        name=ToolName.RUN.value, behavior=ToolBehavior.RESULT_SET
    ),
    # Build tool can have result_set behavior because of macros like print_table
    ToolName.BUILD.value: ToolPolicy(
        name=ToolName.BUILD.value, behavior=ToolBehavior.RESULT_SET
    ),
    ToolName.PARSE.value: ToolPolicy(
        name=ToolName.PARSE.value, behavior=ToolBehavior.METADATA
    ),
    # Semantic Layer tools
    ToolName.LIST_METRICS.value: ToolPolicy(
        name=ToolName.LIST_METRICS.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.GET_DIMENSIONS.value: ToolPolicy(
        name=ToolName.GET_DIMENSIONS.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.GET_ENTITIES.value: ToolPolicy(
        name=ToolName.GET_ENTITIES.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.QUERY_METRICS.value: ToolPolicy(
        name=ToolName.QUERY_METRICS.value, behavior=ToolBehavior.RESULT_SET
    ),
    ToolName.GET_METRICS_COMPILED_SQL.value: ToolPolicy(
        name=ToolName.GET_METRICS_COMPILED_SQL.value, behavior=ToolBehavior.METADATA
    ),
    # Discovery tools
    ToolName.GET_MODEL_PARENTS.value: ToolPolicy(
        name=ToolName.GET_MODEL_PARENTS.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.GET_MODEL_CHILDREN.value: ToolPolicy(
        name=ToolName.GET_MODEL_CHILDREN.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.GET_MODEL_DETAILS.value: ToolPolicy(
        name=ToolName.GET_MODEL_DETAILS.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.GET_MODEL_HEALTH.value: ToolPolicy(
        name=ToolName.GET_MODEL_HEALTH.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.GET_MART_MODELS.value: ToolPolicy(
        name=ToolName.GET_MART_MODELS.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.GET_ALL_MODELS.value: ToolPolicy(
        name=ToolName.GET_ALL_MODELS.value, behavior=ToolBehavior.METADATA
    ),
    # SQL tools
    ToolName.TEXT_TO_SQL.value: ToolPolicy(
        name=ToolName.TEXT_TO_SQL.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.EXECUTE_SQL.value: ToolPolicy(
        name=ToolName.EXECUTE_SQL.value, behavior=ToolBehavior.RESULT_SET
    ),
    # Admin API tools
    ToolName.LIST_JOBS.value: ToolPolicy(
        name=ToolName.LIST_JOBS.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.GET_JOB_DETAILS.value: ToolPolicy(
        name=ToolName.GET_JOB_DETAILS.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.TRIGGER_JOB_RUN.value: ToolPolicy(
        name=ToolName.TRIGGER_JOB_RUN.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.LIST_JOBS_RUNS.value: ToolPolicy(
        name=ToolName.LIST_JOBS_RUNS.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.GET_JOB_RUN_DETAILS.value: ToolPolicy(
        name=ToolName.GET_JOB_RUN_DETAILS.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.CANCEL_JOB_RUN.value: ToolPolicy(
        name=ToolName.CANCEL_JOB_RUN.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.RETRY_JOB_RUN.value: ToolPolicy(
        name=ToolName.RETRY_JOB_RUN.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.LIST_JOB_RUN_ARTIFACTS.value: ToolPolicy(
        name=ToolName.LIST_JOB_RUN_ARTIFACTS.value, behavior=ToolBehavior.METADATA
    ),
    ToolName.GET_JOB_RUN_ARTIFACT.value: ToolPolicy(
        name=ToolName.GET_JOB_RUN_ARTIFACT.value, behavior=ToolBehavior.METADATA
    ),
}
