# dbt MCP Server

This MCP (Model Context Protocol) server provides various tools to interact with dbt. You can use this MCP server to provide AI agents with context of your project in dbt Core, dbt Fusion, and dbt Platform.

Read our documentation [here](https://docs.getdbt.com/docs/dbt-ai/about-mcp) to learn more. [This](https://docs.getdbt.com/blog/introducing-dbt-mcp-server) blog post provides more details for what is possible with the dbt MCP server.

## Feedback

If you have comments or questions, create a GitHub Issue or join us in [the community Slack](https://www.getdbt.com/community/join-the-community) in the `#tools-dbt-mcp` channel.


## Architecture

The dbt MCP server architecture allows for your agent to connect to a variety of tools.

![architecture diagram of the dbt MCP server](https://raw.githubusercontent.com/dbt-labs/dbt-mcp/refs/heads/main/docs/d2.png)

## Examples

Commonly, you will connect the dbt MCP server to an agent product like Claude or Cursor. However, if you are interested in creating your own agent, check out [the examples directory](https://github.com/dbt-labs/dbt-mcp/tree/main/examples) for how to get started.

## Contributing

Read `CONTRIBUTING.md` for instructions on how to get involved!
