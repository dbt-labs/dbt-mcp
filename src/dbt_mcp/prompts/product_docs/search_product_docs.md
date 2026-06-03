Search the dbt product documentation at docs.getdbt.com for pages matching a query. Returns matching page titles, URLs, and descriptions ranked by relevance. Only returns metadata, not page content — use get_product_doc_pages with URLs from the results to retrieve full page content.

If the title/description search finds few matches, it automatically falls back to a deep full-text search across all documentation content to find pages where the topic is discussed in the body text.

If your first query returns few results, try rephrasing with synonyms or the full term (e.g. 'user-defined functions' instead of 'UDFs', 'version control' instead of 'git'). Use the abbreviations on the page as well.

VERSION AWARENESS:
The response includes a `dbt_project_version` field (e.g. "1.8") detected by running `dbt --version` against the user's installed dbt binary. If null, the version could not be detected — present docs normally without version-specific callouts.

When presenting results to the user, you MUST:
1. State the user's detected dbt version upfront if available (e.g. "Based on your project (dbt 1.8), here's what I found:").
2. If a result title or description clearly references an older/EOL version (v1.6 or earlier), flag it: "Note: this page covers dbt v1.X, which has reached end-of-life and is no longer supported."
3. Search results only include titles, descriptions, and URLs — not page content. Do NOT make version-specific claims based on search results alone. If the user needs to know whether a feature applies to their version, fetch the page with `get_product_doc_pages` first.
