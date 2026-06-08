Return the current onboarding session state for this account.

Use this at the start of a resumed conversation ("continue setting up platform") or after any onboarding step to show the user what has been completed and what remains.

Returns:
- **phase**: The local session phase (`deciding`, `reviewing`, `applying`, `apply_failed`, `awaiting_validation`, `validating`, `complete`, `validation_failed`, or `applied_unvalidated`). `null` if no session has been started yet.
- **session_id**: The active session identifier. `null` if no session exists.
- **server_state**: Applied resource progress from the platform API (project_id, connection_id, environment_id, etc.). Only present once the apply phase has started. `null` before apply.

If `phase` is `null`, suggest calling `dbt_admin_onboarding_init` to start the onboarding flow.
If `phase` is `apply_failed`, surface the error and suggest retrying or correcting the failed decision.
If `phase` is `complete`, congratulate the user and suggest next steps (teammates, alerts, Explorer, CI).
