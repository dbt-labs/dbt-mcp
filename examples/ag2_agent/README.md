# AG2 Agent Integration

Examples of using dbt-mcp with [AG2](https://github.com/ag2ai/ag2) (formerly AutoGen),
a multi-agent framework with native MCP client support.

## Examples

| File | Transport | Pattern |
|---|---|---|
| `main_multiagent.py` | dbt Cloud Streamable HTTP | Analyst agent + Executor agent with framework-native handoffs |
| `main_stdio.py` | Local stdio | Single agent, interactive loop |
| `main_remote.py` | dbt Cloud Streamable HTTP | Single agent, interactive loop |

## Setup

Follow the instructions in the project README to configure the required environment
variables for dbt-mcp. Then:

```bash
pip install "ag2[openai,mcp]>=0.11.0"
```

### Multi-agent example (dbt Cloud)

Requires:
- `OPENAI_API_KEY`
- `DBT_TOKEN` — dbt Cloud API token
- `DBT_PROD_ENV_ID` — dbt production environment ID
- `DBT_HOST` — dbt Cloud host (default: `cloud.getdbt.com`)

```bash
uv run main_multiagent.py
```

### Single agent, local

Requires the `.env` file to be configured as described in the root README.

```bash
uv run main_stdio.py
```

### Single agent, dbt Cloud

Requires the same environment variables as the multi-agent example.

```bash
uv run main_remote.py
```

## How the multi-agent system works

The system separates concerns into two specialized agents:

- **`analyst_agent`** — uses read-only Discovery, Semantic Layer, and SQL tools to
  investigate the dbt project and form a recommendation.
- **`executor_agent`** — uses dbt CLI, Admin API, and Codegen tools to carry out
  the recommended action. It runs in `human_input_mode="ALWAYS"` as an approval gate
  before any warehouse-modifying operation.

AG2 connects them using a four-priority handoff system evaluated on every agent turn:
1. **Context conditions** — deterministic, no LLM call (e.g. block executor until approved)
2. **LLM conditions** — analyst hands off when it judges an action is warranted
3. **Tool-based handoffs** — a tool can return the next agent directly via `ReplyResult`
4. **After-work fallback** — both agents return control to the user when nothing else fires

## Example session

```
User > What is the health of the orders model? If there are test failures, run them again.

analyst_agent > [calls get_model_health(node_id="model.jaffle_shop.orders")]
analyst_agent > The orders model has 3 failing tests: not_null_orders_id,
                unique_orders_id, accepted_values_orders_status.
                Last run: 14 minutes ago. Upstream sources are fresh.
                Recommendation: re-run dbt test --select orders.
                Handing off to executor.

executor_agent > [APPROVAL REQUIRED]
                 About to run: test(select="orders")
                 Proceed? (yes/no)

User > yes

executor_agent > [calls test(select="orders")]
executor_agent > Tests complete. 3 passed, 0 failed.
                 All previously failing tests now pass.
```
