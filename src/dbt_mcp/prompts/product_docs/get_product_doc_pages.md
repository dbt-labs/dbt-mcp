Retrieve the Markdown content of one or more dbt documentation pages from docs.getdbt.com. Pass a list of URLs or relative paths (e.g. from search_product_docs results). Up to 5 pages can be fetched per call; pages are fetched in parallel for speed. Pass the optional `query` parameter to retrieve only the most relevant sections of each page rather than the full content — this significantly reduces response size and is recommended when you already know what topic you are looking for.

IMPORTANT — how to present docs content to the user:
1. Give a direct answer first, then explain. Don't just summarize — be specific about WHERE to find a feature in the UI and HOW it works.
2. Structure around the user's task: lead with what they asked, not the doc's own structure. Include step-by-step actionable detail when the docs describe a workflow or UI feature.
3. Call out practical limitations and edge cases from the docs.
4. Close with clear guidance: if the feature only partially answers the question, say what else they can do and where.
5. ALWAYS include the docs page URL(s) as markdown hyperlinks at the end of your response, e.g. [Page title](https://docs.getdbt.com/...).

VERSION AWARENESS:
The response includes a `dbt_project_version` field with the user's installed dbt version (detected via `dbt --version`). Page content is automatically filtered to show only sections relevant to the detected version — VersionBlock tags from the docs site are resolved so you receive clean, version-appropriate markdown. Pages may also have a `version_note` field if they cover an EOL version. If `dbt_project_version` is null, the version could not be detected — all version-specific content is kept with tags stripped.

When presenting page content to the user, you MUST:
1. If `dbt_project_version` is set, tell the user their detected version upfront (e.g. "You're on dbt 1.8").
2. If `version_note` is set, prominently warn the user that the page is for an end-of-life version and the information may be outdated. Suggest they look at the current docs instead.
3. Page content has already been filtered to show only sections relevant to the detected version. If the content seems sparse or incomplete, let the user know that some sections may have been filtered out because they apply to a different dbt version, and suggest they check the full page on docs.getdbt.com.
