"""AG2 multi-agent dbt workflow: Analyst + Executor.

The analyst agent uses read-only Discovery, Semantic Layer, and SQL tools to
investigate the dbt project. The executor agent uses dbt CLI, Admin API, and
Codegen tools to carry out actions. AG2's four-priority handoff system connects
them without bespoke routing code.

Required environment variables:
  OPENAI_API_KEY
  DBT_TOKEN         - dbt Cloud API token
  DBT_PROD_ENV_ID   - dbt production environment ID
  DBT_HOST          - dbt Cloud host (optional, default: cloud.getdbt.com)
"""

import asyncio
import os
from typing import Annotated

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from autogen import ConversableAgent, LLMConfig
from autogen.mcp import create_toolkit
from autogen.agentchat import initiate_group_chat
from autogen.agentchat.group import (
    AgentTarget,
    ContextVariables,
    ExpressionContextCondition,
    ContextExpression,
    OnCondition,
    OnContextCondition,
    ReplyResult,
    RevertToUserTarget,
    StringLLMCondition,
)
from autogen.agentchat.group.patterns import DefaultPattern


# ---------------------------------------------------------------------------
# Tool-based handoff helper (Priority 3)
# ---------------------------------------------------------------------------
# This function can be registered as a tool on the analyst agent.
# When the analyst calls it, the framework uses the ReplyResult target
# to route directly to the executor, regardless of any LLM condition.
# The routing decision is co-located with the logic producing it.

def recommend_action(
    action: Annotated[str, "The dbt action to recommend, e.g. 'test --select orders'"],
    reason: Annotated[str, "Why this action is needed"],
    context_variables: ContextVariables,
) -> ReplyResult:
    """Record an action recommendation and hand off to the executor agent."""
    context_variables["pending_action"] = action
    context_variables["action_reason"] = reason
    return ReplyResult(
        message=(
            f"Analysis complete. Recommended action: dbt {action}\n"
            f"Reason: {reason}\n"
            "Handing off to executor."
        ),
        target=AgentTarget(executor_agent),  # defined below
        context_variables=context_variables,
    )


async def main() -> None:
    host = os.environ.get("DBT_HOST", "cloud.getdbt.com")
    token = os.environ["DBT_TOKEN"]
    prod_env_id = os.environ["DBT_PROD_ENV_ID"]

    mcp_url = f"https://{host}/api/ai/v1/mcp/"
    headers = {
        "Authorization": f"token {token}",
        "x-dbt-prod-environment-id": prod_env_id,
    }

    llm_config = LLMConfig(
        api_type="openai",
        model="gpt-4.1",
        api_key=os.environ["OPENAI_API_KEY"],
    )

    async with (
        streamablehttp_client(mcp_url, headers=headers) as (read, write, _),
        ClientSession(read, write) as session,
    ):
        await session.initialize()

        # All dbt-mcp tools available in this toolkit.
        toolkit = await create_toolkit(session=session)

        # ------------------------------------------------------------------
        # Agent 1: Analyst
        # Uses read-only Discovery, Semantic Layer, and SQL tools.
        # Does NOT call run / test / build / trigger_job_run or other
        # warehouse-modifying operations — enforced by system prompt.
        # ------------------------------------------------------------------
        analyst_agent = ConversableAgent(
            name="analyst_agent",
            system_message=(
                "You are a senior analytics engineer specializing in dbt project analysis.\n\n"
                "YOUR TOOLS (read-only):\n"
                "- Discovery: get_all_models, get_model_details, get_model_health,\n"
                "  get_model_performance, get_lineage, get_model_parents,\n"
                "  get_model_children, get_all_sources, get_source_details,\n"
                "  get_exposures, get_exposure_details\n"
                "- Semantic Layer: list_metrics, query_metrics, get_dimensions,\n"
                "  get_entities, get_metrics_compiled_sql, list_saved_queries\n"
                "- SQL: execute_sql, text_to_sql\n"
                "- Product Docs: search_product_docs, get_product_doc_pages\n\n"
                "YOUR WORKFLOW:\n"
                "1. Investigate the dbt project using only the tools above.\n"
                "2. Summarize your findings clearly and concisely.\n"
                "3. If the user's request requires running, testing, building, or\n"
                "   triggering dbt models or jobs, call recommend_action() with\n"
                "   the specific dbt command and your reason. This hands off to\n"
                "   the executor agent.\n"
                "4. If no action is needed, answer the user directly.\n\n"
                "NEVER call: run, test, build, compile (execution), show, parse,\n"
                "trigger_job_run, cancel_job_run, retry_job_run, generate_model_yaml,\n"
                "generate_source, generate_staging_model."
            ),
            llm_config=llm_config,
            human_input_mode="NEVER",
            functions=[recommend_action],
        )

        # ------------------------------------------------------------------
        # Agent 2: Executor
        # Uses dbt CLI, Admin API, and Codegen tools.
        # Runs with human_input_mode="ALWAYS" as an explicit approval gate
        # before any warehouse-modifying operation.
        # ------------------------------------------------------------------
        executor_agent = ConversableAgent(
            name="executor_agent",
            system_message=(
                "You are a dbt execution engineer. You receive a recommended action\n"
                "from the analyst and carry it out.\n\n"
                "YOUR TOOLS:\n"
                "- dbt CLI: run, test, build, compile, show, list, parse\n"
                "- Admin API: trigger_job_run, retry_job_run, cancel_job_run,\n"
                "  get_job_run_details, get_job_run_error, list_jobs, list_jobs_runs\n"
                "- Codegen: generate_model_yaml, generate_source, generate_staging_model\n\n"
                "YOUR WORKFLOW:\n"
                "1. Review the recommended action from context (pending_action).\n"
                "2. Confirm with the user before running any warehouse-modifying command.\n"
                "3. Execute the action and report the outcome.\n"
                "4. If the action fails, report the error and suggest next steps.\n\n"
                "You will be prompted for human input before executing — this is the\n"
                "approval gate. Type the action to confirm or 'cancel' to abort."
            ),
            llm_config=llm_config,
            human_input_mode="ALWAYS",  # approval gate before warehouse operations
        )

        # ------------------------------------------------------------------
        # Human agent — drives the conversation
        # ------------------------------------------------------------------
        user_agent = ConversableAgent(
            name="user",
            human_input_mode="ALWAYS",
        )

        # ------------------------------------------------------------------
        # Register ALL dbt-mcp tools on both agents.
        # The system prompts and handoff conditions enforce separation.
        # For stricter enforcement, filter toolkit.tools by name before
        # registering (see README for details).
        # ------------------------------------------------------------------
        toolkit.register_for_llm(analyst_agent)
        toolkit.register_for_execution(analyst_agent)
        toolkit.register_for_llm(executor_agent)
        toolkit.register_for_execution(executor_agent)

        # ------------------------------------------------------------------
        # Handoffs: Priority 1 — OnContextCondition (deterministic)
        # Evaluated BEFORE the LLM fires. No API call, no token spend.
        #
        # If the analyst has already populated 'pending_action' in context,
        # skip re-analysis and route directly to the executor.
        # ------------------------------------------------------------------
        analyst_agent.handoffs.add_context_conditions([
            OnContextCondition(
                target=AgentTarget(executor_agent),
                condition=ExpressionContextCondition(
                    expression=ContextExpression(
                        "${pending_action} != None and ${pending_action} != ''"
                    )
                ),
            ),
        ])

        # ------------------------------------------------------------------
        # Handoffs: Priority 2 — OnCondition / StringLLMCondition (LLM-driven)
        # The LLM judges whether its own output represents an action
        # recommendation. If the analyst's reasoning concludes with a
        # concrete recommendation, this fires and routes to executor.
        # The analyst's recommend_action() tool (Priority 3) can also
        # trigger a handoff directly.
        # ------------------------------------------------------------------
        analyst_agent.handoffs.add_llm_conditions([
            OnCondition(
                target=AgentTarget(executor_agent),
                condition=StringLLMCondition(
                    prompt=(
                        "Hand off to the executor when your analysis concludes with a "
                        "specific action recommendation: running models, executing tests, "
                        "triggering a dbt Cloud job, or generating dbt code. Do NOT hand "
                        "off if the question is purely informational and fully answered."
                    )
                ),
            ),
        ])

        # ------------------------------------------------------------------
        # Handoffs: Priority 4 — AfterWork fallback
        # Fires when no context condition and no LLM condition triggered.
        # Both agents return to the user — no silent stalls.
        # ------------------------------------------------------------------
        analyst_agent.handoffs.set_after_work(RevertToUserTarget())
        executor_agent.handoffs.set_after_work(RevertToUserTarget())

        # ------------------------------------------------------------------
        # Shared context — typed state visible to all agents
        # ------------------------------------------------------------------
        context = ContextVariables(data={
            "models_investigated": [],   # analyst populates; avoids re-fetching
            "failing_tests": [],         # analyst populates; executor uses for selector
            "pending_action": None,      # set by recommend_action() tool
            "action_reason": None,       # set by recommend_action() tool
            "last_job_run_id": None,     # set by executor after trigger_job_run
        })

        # ------------------------------------------------------------------
        # DefaultPattern: analyst is the initial agent.
        # AutoPattern could be used instead for LLM-managed speaker selection.
        # ------------------------------------------------------------------
        pattern = DefaultPattern(
            initial_agent=analyst_agent,
            agents=[analyst_agent, executor_agent],
            user_agent=user_agent,
            context_variables=context,
            group_after_work=RevertToUserTarget(),
        )

        print("dbt Multi-Agent System ready (AG2). Type your question below.")
        print("Examples:")
        print("  'What is the health of the orders model?'")
        print("  'List all models with failing tests and re-run them'")
        print("  'Show me monthly revenue metrics, then trigger the nightly job'\n")

        user_input = input("User > ").strip()
        if user_input:
            result, final_context, last_agent = initiate_group_chat(
                pattern=pattern,
                messages=user_input,
                max_rounds=30,
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")
    except Exception as e:
        print(f"Error: {e}")
