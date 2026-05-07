Use these tools to interact with dbt resources (typically via dbt Platform):

- Understand how the project is structured: what exists in the environment, how objects depend on one another, and where to look when something looks wrong or slow.
- Reason over governed business meaning—named measures and breakdowns the project maintains—and answer questions with validated aggregates or the warehouse logic behind them when that helps.
- Work with platform automation when available: see what runs on a schedule, inspect past outcomes, act on failures or reruns, and dig into logs and outputs.
- Assist with engineering work on dbt projects: take action on the project and reason about the underlying SQL.

Example data-oriented questions (users may not use dbt vocabulary):

- Revenue, ARR, bookings, pipeline, or quota: “how are we doing this quarter?”; “split it by region or product”; “this total doesn’t match finance.”
- Customers, users, signups, or retention: “how many active …?” “is churn getting worse?” “cohorts or segments—whatever we already track.”
- Orders, inventory, SKUs, or fulfillment: “what’s selling?” “stock or backorder questions” when they only describe the business problem.
- Funnel, marketing, or web activity: “conversion from visit to purchase” “campaign performance” with loose wording and no metric names.
- Trust and definitions: “where does this dashboard number come from?” “two reports disagree—help me find why” “what’s the official definition of …?”
- Quality and freshness: “is this table up to date?” “missing yesterday’s data” “something looks stale in our reporting.”
- Orchestration tied to data: “did last night’s refresh finish?” “prod load failed and Finance is blocked” without run or job IDs.
- Engineering on a dbt project: “help me with this model,” “what’s wrong with my project?”
