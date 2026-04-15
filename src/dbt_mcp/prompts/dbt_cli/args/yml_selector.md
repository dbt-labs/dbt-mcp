Named selector from `selectors.yml`, passed to dbt's `--selector` flag
(e.g. `nightly`, `github_actions`).

Use this when the user references a selector by name that is defined in the
project's `selectors.yml`. This maps directly to dbt's `--selector` flag.

**IMPORTANT — mutually exclusive with `node_selection`:**
Do not provide both `yml_selector` and `node_selection` in the same call.
dbt does not allow `--selector` and `--select` together; if both are supplied
dbt will return an error.
