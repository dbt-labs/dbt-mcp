# dbt MCP Server
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/11137/badge)](https://www.bestpractices.dev/projects/11137)

This MCP (Model Context Protocol) server provides various tools to interact with dbt. You can use this MCP server to provide AI agents with context of your project in dbt Core, dbt Fusion, and dbt Platform.

Read our documentation [here](https://docs.getdbt.com/docs/dbt-ai/about-mcp) to learn more. [This](https://docs.getdbt.com/blog/introducing-dbt-mcp-server) blog post provides more details for what is possible with the dbt MCP server.

## Feedback

If you have comments or questions, create a GitHub Issue or join us in [the community Slack](https://www.getdbt.com/community/join-the-community) in the `#tools-dbt-mcp` channel.


## Architecture

The dbt MCP server architecture allows for your agent to connect to a variety of tools.

![architecture diagram of the dbt MCP server](https://raw.githubusercontent.com/dbt-labs/dbt-mcp/refs/heads/main/docs/d2.png)

## Tools

### SQL
- `execute_sql`
- `text_to_sql`

### Semantic Layer
- `get_dimensions`
- `get_entities`
- `get_metrics_compiled_sql`
- `list_metrics`
- `list_saved_queries`
- `query_metrics`

### Discovery
- `get_all_models`
- `get_all_sources`
- `get_exposure_details`
- `get_exposures`
- `get_macro_details`
- `get_mart_models`
- `get_model_children`
- `get_model_details`
- `get_model_health`
- `get_model_parents`
- `get_related_models`
- `get_seed_details`
- `get_semantic_model_details`
- `get_snapshot_details`
- `get_source_details`
- `get_test_details`
- `search`

### dbt CLI
- `build`
- `compile`
- `docs`
- `get_model_lineage_dev`
- `list`
- `parse`
- `run`
- `show`
- `test`

### Admin API
- `cancel_job_run`
- `get_job_details`
- `get_job_run_artifact`
- `get_job_run_details`
- `get_job_run_error`
- `list_job_run_artifacts`
- `list_jobs`
- `list_jobs_runs`
- `retry_job_run`
- `trigger_job_run`

### dbt Codegen
- `generate_model_yaml`
- `generate_source`
- `generate_staging_model`

### dbt LSP
- `fusion.compile_sql`
- `fusion.get_column_lineage`
- `get_column_lineage`


## Examples

Commonly, you will connect the dbt MCP server to an agent product like Claude or Cursor. However, if you are interested in creating your own agent, check out [the examples directory](https://github.com/dbt-labs/dbt-mcp/tree/main/examples) for how to get started.

## Contributing

Read `CONTRIBUTING.md` for instructions on how to get involved!
