Creates a brand-new dbt platform account and returns an owner API token.

Use this only when the user does not have an account yet. It is billable — it spins up a trial account — so confirm with the user before calling. If the user already has an account, do not call this; use their existing account instead.

Provide the desired account `name` and the owner's `owner_email`. Optionally pass `created_via` (e.g. `onboarding_api`) for funnel attribution; leave it unset if unsure.

The returned owner token is stored for the rest of this session, so the account-scoped admin and onboarding tools will authenticate automatically afterwards — you do not need to ask the user for a token.
