Start or resume an onboarding session for this dbt platform account.

Call this at the beginning of any onboarding conversation. It issues a `session_id` and returns the full decision tree — the set of values the user needs to confirm before the platform can be configured.

Returns:
- **session_id**: A unique identifier for this onboarding session. Pass it to subsequent onboarding tools.
- **phase**: The current session phase. Starts as `deciding` and progresses through `reviewing → applying → awaiting_validation → complete`.
- **account_id**: The account being onboarded.
- **decision_points**: The list of decisions the user must make, each with a `key`, `label`, `description`, optional `rationale_hint` for how to suggest a value, and `examples`. Empty in the Foundation phase; populated as account/project/environment/job phases ship.

If a session already exists for this account, it is resumed from the current phase — the user does not need to restart.

Suggest values for non-`sensitive` decision points based on detected project context. Never invent values for `sensitive: true` keys — ask the user explicitly.
