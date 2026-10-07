# Monty Code Mode

An example of "code mode" with the remote dbt MCP server. Instead of the LLM calling MCP tools one at a time and receiving every intermediate result in its context, it writes a short Python script. The script runs in a [Monty](https://github.com/pydantic/monty) sandbox where each MCP tool is an async function. The script chains calls and filters in Python, and only what it `print()`s goes back to the LLM.

The sandbox has no filesystem, network, subprocess or environment access. The only thing it can do is call the tools the MCP server exposes. Credentials stay in the host process.

## Config

Set the following environment variables:
- `DBT_TOKEN`
- `DBT_PROD_ENV_ID`
- `DBT_HOST` (the full hostname, for example `abc123.us1.dbt.com`)

## Usage

Run the built-in demo, which lists the metrics and queries one of them:

`uv run main.py`

Run your own script from a file, or from stdin with `-`:

```shell
uv run main.py script.py
echo 'print(await list_metrics())' | uv run main.py -
```

## Using it with an LLM

`uv run main.py --instructions` prints a guide listing the functions available in the sandbox, generated from the tools the server exposes. Add it to your agent's instructions (for example in `CLAUDE.md`) and tell the agent to run its scripts through `main.py`.

Tool results are parsed as JSON when possible, and returned as raw text otherwise. Some Semantic Layer tools return compact text rather than structured data, so the script may need to parse that text itself.
