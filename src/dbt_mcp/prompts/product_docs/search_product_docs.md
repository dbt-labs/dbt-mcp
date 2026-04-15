Search the dbt product documentation at docs.getdbt.com for pages matching a query. Returns matching page titles, URLs, and descriptions ranked by relevance. Only returns metadata, not page content — use get_product_doc_pages with URLs from the results to retrieve full page content.

If the title/description search finds few matches, it automatically falls back to a deep full-text search across all documentation content to find pages where the topic is discussed in the body text.

If your first query returns few results, try rephrasing with synonyms or the full term (e.g. 'user-defined functions' instead of 'UDFs', 'version control' instead of 'git'). Use the abbreviations on the page as well.

VERSION AWARENESS:
The response includes a `dbt_project_version` field (e.g. "1.8") detected from the user's dbt_project.yml `require-dbt-version`. If null, the version could not be detected — present docs normally without version-specific callouts.

When presenting results to the user, you MUST:
1. State the user's detected dbt version upfront if available (e.g. "Based on your project (dbt 1.8), here's what I found:").
2. If a result is about a feature introduced in a newer version than the user's, say so explicitly (e.g. "This feature requires dbt 1.9+. You're currently on 1.8 — upgrading would give you access to this.").
3. If a result is for an older/EOL version (v1.6 or earlier), clearly flag it: "Note: this page covers dbt v1.X, which has reached end-of-life and is no longer supported."
4. Results include content for ALL dbt versions. The docs site does not serve version-specific pages, so always tell the user which version(s) a page applies to when it matters.
