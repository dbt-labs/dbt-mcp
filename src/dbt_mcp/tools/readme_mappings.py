"""Mappings for README doc auto-generation."""

from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

HUMAN_DESCRIPTIONS: dict[ToolName, str] = {
    # dbt CLI tools
    ToolName.BUILD: "Executes models, tests, snapshots, and seeds in DAG order.",
    ToolName.COMPILE: "Generates executable SQL from models/tests/analyses; useful for validating Jinja logic.",
    ToolName.DOCS: "Generates documentation for the dbt project.",
    ToolName.LIST: "Lists resources in the dbt project by type with selector support.",
    ToolName.PARSE: "Parses and validates project files for syntax correctness.",
    ToolName.RUN: "Executes models to materialize them in the database.",
    ToolName.TEST: "Runs tests to validate data and model integrity.",
    ToolName.SHOW: "Executes SQL against the database and returns results.",
    ToolName.GET_LINEAGE_DEV: "Retrieves lineage from local manifest.json with type and depth filtering.",
    ToolName.GET_NODE_DETAILS_DEV: "Retrieves node details from local manifest.json (models, seeds, snapshots, sources).",
    # Semantic Layer tools
    ToolName.LIST_METRICS: "Retrieves all defined metrics.",
    ToolName.LIST_SAVED_QUERIES: "Retrieves all saved queries.",
    ToolName.GET_DIMENSIONS: "Gets dimensions for specified metrics.",
    ToolName.GET_ENTITIES: "Gets entities for specified metrics.",
    ToolName.QUERY_METRICS: "Executes metric queries with filtering and grouping options.",
    ToolName.GET_METRICS_COMPILED_SQL: "Returns compiled SQL for metrics without executing the query.",
    # Discovery tools
    ToolName.GET_MART_MODELS: "Retrieves all mart models.",
    ToolName.GET_ALL_MODELS: "Retrieves name and description of all models.",
    ToolName.GET_MODEL_DETAILS: "Gets model details including compiled SQL, columns, and schema.",
    ToolName.GET_MODEL_PARENTS: "Gets upstream dependencies of a model.",
    ToolName.GET_MODEL_CHILDREN: "Gets downstream dependents of a model.",
    ToolName.GET_MODEL_HEALTH: "Gets health signals: run status, test results, and upstream source freshness.",
    ToolName.GET_MODEL_PERFORMANCE: "Gets execution history for a model; option to include test results.",
    ToolName.GET_LINEAGE: "Gets full lineage graph (ancestors and descendants) with type and depth filtering.",
    ToolName.GET_ALL_SOURCES: "Gets all sources with freshness status; option to filter by source name.",
    ToolName.GET_SOURCE_DETAILS: "Gets source details including columns and freshness.",
    ToolName.GET_EXPOSURES: "Gets all exposures (downstream dashboards, apps, or analyses).",
    ToolName.GET_EXPOSURE_DETAILS: "Gets exposure details including owner, parents, and freshness status.",
    ToolName.GET_RELATED_MODELS: "Finds similar models using semantic search.",
    ToolName.GET_ALL_MACROS: "Retrieves macros; option to filter by package or return package names only.",
    ToolName.GET_MACRO_DETAILS: "Gets details for a specific macro.",
    ToolName.GET_SEED_DETAILS: "Gets details for a specific seed.",
    ToolName.GET_SEMANTIC_MODEL_DETAILS: "Gets details for a specific semantic model.",
    ToolName.GET_SNAPSHOT_DETAILS: "Gets details for a specific snapshot.",
    ToolName.GET_TEST_DETAILS: "Gets details for a specific test.",
    ToolName.SEARCH: "[Alpha] Searches for resources across the dbt project (not generally available).",
    # SQL tools
    ToolName.TEXT_TO_SQL: "Generates SQL from natural language using project context.",
    ToolName.EXECUTE_SQL: "Executes SQL on dbt Platform infrastructure with Semantic Layer support.",
    # Admin API tools
    ToolName.LIST_PROJECTS: "Lists all projects in the dbt Platform account.",
    ToolName.LIST_JOBS: "Lists jobs in a dbt Platform account; option to filter by project or environment.",
    ToolName.GET_JOB_DETAILS: "Gets job configuration including triggers, schedule, and dbt commands.",
    ToolName.TRIGGER_JOB_RUN: "Triggers a job run; option to override git branch, schema, or other settings.",
    ToolName.LIST_JOBS_RUNS: "Lists job runs; option to filter by job, status, or order by field.",
    ToolName.GET_JOB_RUN_DETAILS: "Gets run details including status, timing, steps, and artifacts.",
    ToolName.CANCEL_JOB_RUN: "Cancels a running job.",
    ToolName.RETRY_JOB_RUN: "Retries a failed job run.",
    ToolName.LIST_JOB_RUN_ARTIFACTS: "Lists available artifacts from a job run.",
    ToolName.GET_JOB_RUN_ARTIFACT: "Downloads a specific artifact file from a job run.",
    ToolName.GET_JOB_RUN_ERROR: "Gets error and/or warning details for a job run; option to include or show warnings only.",
    # dbt-codegen tools
    ToolName.GENERATE_SOURCE: "Generates source YAML by introspecting database schemas; option to include columns.",
    ToolName.GENERATE_MODEL_YAML: "Generates model YAML with columns; option to inherit upstream descriptions.",
    ToolName.GENERATE_STAGING_MODEL: "Generates staging model SQL from a source table.",
    # dbt LSP tools
    ToolName.GET_COLUMN_LINEAGE: "Traces column-level lineage locally (requires dbt-lsp via dbt Labs VSCE).",
    ToolName.FUSION_COMPILE_SQL: "Compiles SQL in project context via dbt Platform.",
    ToolName.FUSION_GET_COLUMN_LINEAGE: "Traces column-level lineage via dbt Platform.",
    # MCP Server tools
    ToolName.GET_MCP_SERVER_VERSION: "Returns the current version of the dbt MCP server.",
    # Product docs tools
    ToolName.SEARCH_PRODUCT_DOCS: "Searches docs.getdbt.com for pages matching a query; returns titles, URLs, and descriptions ranked by relevance. Use get_product_doc_pages to fetch full content.",
    ToolName.GET_PRODUCT_DOC_PAGES: "Fetches the full Markdown content of one or more docs.getdbt.com pages by path or URL.",
    ToolName.GET_MCP_SERVER_BRANCH: "Returns the current git branch of the running dbt MCP server.",
}

TOOLSET_DESCRIPTIONS: dict[Toolset, str] = {
    Toolset.SQL: "Tools for executing and generating SQL on dbt Platform infrastructure.",
    Toolset.SEMANTIC_LAYER: "To learn more about the dbt Semantic Layer, click [here](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl).",
    Toolset.DISCOVERY: "To learn more about the dbt Discovery API, click [here](https://docs.getdbt.com/docs/dbt-cloud-apis/discovery-api).",
    Toolset.DBT_CLI: "Allowing your client to utilize dbt commands through the MCP tooling could modify your data models, sources, and warehouse objects. Proceed only if you trust the client and understand the potential impact.",
    Toolset.ADMIN_API: "To learn more about the dbt Administrative API, click [here](https://docs.getdbt.com/docs/dbt-cloud-apis/admin-cloud-api).",
    Toolset.DBT_CODEGEN: "These tools help automate boilerplate code generation for dbt project files.",
    Toolset.DBT_LSP: "A set of tools that leverage the Fusion engine for advanced SQL compilation and column-level lineage analysis.",
    Toolset.MCP_SERVER_METADATA: "These tools provide information about the MCP server itself.",
    Toolset.PRODUCT_DOCS: "Tools for searching and fetching content from the official dbt documentation at docs.getdbt.com.",
}


def validate_human_descriptions() -> None:
    """Ensure all ToolName members have a human description.

    Raises:
        ValueError: If any tools are missing a human description
    """
    missing = set(ToolName) - set(HUMAN_DESCRIPTIONS.keys())
    if missing:
        missing_names = [tool.value for tool in missing]
        raise ValueError(
            f"The following tools are missing human descriptions: {', '.join(missing_names)}"
        )


def validate_toolset_descriptions() -> None:
    """Ensure all Toolset members have an intro description.

    Raises:
        ValueError: If any toolsets are missing a description
    """
    missing = set(Toolset) - set(TOOLSET_DESCRIPTIONS.keys())
    if missing:
        missing_names = [ts.value for ts in missing]
        raise ValueError(
            f"The following toolsets are missing descriptions: {', '.join(missing_names)}"
        )


# Check when modules imported
validate_human_descriptions()
validate_toolset_descriptions()
