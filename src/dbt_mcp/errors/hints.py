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
