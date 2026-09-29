"""Helpful hints for common error conditions."""

MULTICELL_HINT = (
    "Hint: If you are on a multi-cell dbt platform instance, make sure DBT_HOST is set "
    "to the full hostname including the account prefix (for example, 'abc123.us1.dbt.com'). "
    "See https://docs.getdbt.com/docs/dbt-ai/setup-local-mcp#api-and-sql-tool-settings"
)


def looks_like_ssl_error(error: str | Exception) -> bool:
    """Check if an exception or message looks like an SSL/certificate error."""
    error_str = str(error).lower()
    return any(kw in error_str for kw in ["ssl", "certificate"])


def with_multicell_hint(message: str) -> str:
    """Add multicell hint to a message if it looks like an SSL error."""
    if looks_like_ssl_error(message):
        return f"{message}\n\n{MULTICELL_HINT}"
    return message


# The semantic layer may classify a warehouse query failure and mark it by
# embedding one of these bracketed tokens in the error message, followed by
# the original underlying error message. Any other message is unclassified
# and must be left completely untouched.
_WAREHOUSE_AUTHENTICATION_MARKER = "[WAREHOUSE_AUTHENTICATION_FAILED]"
_WAREHOUSE_PERMISSION_MARKER = "[WAREHOUSE_PERMISSION_DENIED]"

WAREHOUSE_AUTHENTICATION_HINT = (
    "Hint: The warehouse rejected the configured credentials as invalid. Check "
    "your configured warehouse credentials and reconfigure them if they have "
    "expired, been rotated, or changed."
)

WAREHOUSE_PERMISSION_HINT = (
    "Hint: The warehouse accepted the configured credentials but denied access "
    "to the object being queried. Note this can also mean the referenced object "
    "does not exist or is misspelled — the warehouse (e.g. Snowflake) reports "
    "both situations identically, so don't assume it's strictly a permissions "
    "issue. Double-check both the object name/spelling and the access grants "
    "for the configured credentials."
)


def classify_warehouse_error(message: str) -> tuple[str, str | None]:
    """Detect and strip a warehouse error marker from a semantic layer error message.

    Returns a tuple of (message_with_marker_removed, category), where category
    is one of "authentication", "permission", or None if the message carries no
    marker (the overwhelmingly common case, which must pass through unchanged).

    Uses substring containment rather than a prefix check, since the marker is
    not guaranteed to be at the very start of the message.
    """
    if _WAREHOUSE_AUTHENTICATION_MARKER in message:
        stripped = message.replace(_WAREHOUSE_AUTHENTICATION_MARKER, "", 1)
        return stripped.strip(), "authentication"
    if _WAREHOUSE_PERMISSION_MARKER in message:
        stripped = message.replace(_WAREHOUSE_PERMISSION_MARKER, "", 1)
        return stripped.strip(), "permission"
    return message, None


def warehouse_error_hint(category: str | None) -> str | None:
    """Return the actionable hint text for a warehouse error category, if any."""
    if category == "authentication":
        return WAREHOUSE_AUTHENTICATION_HINT
    if category == "permission":
        return WAREHOUSE_PERMISSION_HINT
    return None
