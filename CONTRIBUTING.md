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

This repo has automated tests which can be run with `task test:unit`. Additionally, there is a simple CLI tool which can be used to test by running `task client`. If you would like to test in a client like Cursor or Claude, use a configuration file like this:

```
{
  "mcpServers": {
    "dbt": {
      "command": "<path-to-this-uv>",
      "args": [
        "run",
        "--env-file",
        "<path-to-this-directory>/dbt-mcp/.env",
        "<path-to-this-directory>/dbt-mcp/.venv/bin/mcp",
        "run",
        "<path-to-this-directory>/dbt-mcp/src/dbt_mcp/main.py"
      ]
    }
  }
}
```

Or, if you would like to test with Oauth, use a configuration like this:

```
{
  "mcpServers": {
    "dbt": {
      "command": "<path-to-this-directory>/dbt-mcp/.venv/bin/mcp",
      "args": [
        "run",
        "<path-to-this-directory>/dbt-mcp/src/dbt_mcp/main.py"
      ],
      "env": {
        "DBT_HOST": "<dbt-host-with-custom-subdomain>",
        "ENABLE_EXPERIMENAL_SECURE_OAUTH": "true",
      }
    }
  }
}
```

## Signed Commits

Before committing changes, ensure that you have set up [signed commits](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits).
This repo requires signing on all commits for PRs.

## Changelog

Every PR requires a changelog entry. [Install changie](https://changie.dev/) and run `changie new` to create a new changelog entry.

## Debugging

If you encounter any problems. You can try running `task run` to see errors in your terminal.

## Release

Only people in the `CODEOWNERS` file should trigger a new release with these steps:

1. Trigger the [Create release PR Action](https://github.com/dbt-labs/dbt-mcp/actions/workflows/create-release-pr.yml).
  - If the release is NOT a pre-release, just pick if the bump should be patch, minor or major
  - If the release is a pre-release, set the bump and the pre-release suffix. We support alpha.N, beta.N and rc.N.
    - use alpha for early releases of experimental features that specific people might want to test. Significant changes can be expected between alpha and the official release.
    - use beta for releases that are mostly stable but still in development. It can be used to gather feedback from a group of peopleon how a specific feature should work.
    - use rc for releases that are mostly stable and already feature complete. Only bugfixes and minor changes are expected between rc and the official release.
  - Picking the prerelease suffix will depend on whether the last release was the stable release or a pre-release:

| Last Stable | Last Pre-release | Bump  | Pre-release Suffix | Resulting Version |
| ----------- | ---------------- | ----- | ------------------ | ----------------- |
| 1.2.0       | -                | minor | beta.1             | 1.3.0-beta.1      |
| 1.2.0       | 1.3.0-beta.1     | minor | beta.2             | 1.3.0-beta.2      |
| 1.2.0       | 1.3.0-beta.2     | minor | rc.1               | 1.3.0-rc.1        |
| 1.2.0       | 1.3.0-rc.1       | minor |                    | 1.3.0             |
| 1.2.0       | 1.3.0-beta.2     | minor | -                  | 1.3.0             |
| 1.2.0       | -                | major | rc.1               | 2.0.0-rc.1        |
| 1.2.0       | 2.0.0-rc.1       | major | -                  | 2.0.0             |

2. Get this PR approved & merged in (if the resulting release name is not the one expected in the PR, just close the PR and try again step 1)
3. This will trigger the `Release dbt-mcp` Action. On the `Summary` page of this Action a member of the `CODEOWNERS` file will have to manually approve the release. The rest of the release process is automated.
