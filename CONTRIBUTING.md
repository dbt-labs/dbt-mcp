# Contributing

With [task](https://taskfile.dev/) installed, simply run `task` to see the list of available commands. For comments, questions, or requests open a GitHub issue.

## Setup

1. Clone the repository:
```shell
git clone https://github.com/dbt-labs/dbt-mcp.git
cd dbt-mcp
```

2. [Install uv](https://docs.astral.sh/uv/getting-started/installation/)

3. [Install Task](https://taskfile.dev/installation/)

4. Run `task install`

5. Configure environment variables:
```shell
cp .env.example .env
```
Then edit `.env` with your specific environment variables (see the `Configuration` section of the `README.md`).

## Testing

This repo has automated tests which can be run with `task test:unit`. Additionally, there is a simple CLI tool which can be used to test by running `task client`. If you would like to test in a client like Cursor or Claude. Use a configuration file like this:

```
{
  "mcpServers": {
    "dbt": {
      "command": "<path-to-this-directory>/.venv/bin/mcp",
      "args": [
        "run",
        "<path-to-this-directory>/src/dbt_mcp/main.py"
      ]
    }
  }
}
```

## Changelog

Every PR requires a changelog entry. [Install changie](https://changie.dev/) and run `changie new` to create a new changelog entry.

## Debugging

If you encounter any problems. You can try running `task run` to see errors in your terminal.

## Release

Only people in the `CODEOWNERS` file should trigger a new release with these steps:

1. Trigger the [Create release PR Action](https://github.com/dbt-labs/dbt-mcp/actions/workflows/create-release-pr.yml).
2. Get this PR approved & merged in.
3. This will trigger the `Release dbt-mcp` Action. On the `Summary` page of this Action a member of the `CODEOWNERS` file will have to manually approve the release. The rest of the release process is automated.