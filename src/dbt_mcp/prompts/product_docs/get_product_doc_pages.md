Retrieve the full Markdown content of one or more dbt documentation pages from docs.getdbt.com. Pass a list of URLs or relative paths (e.g. from search_product_docs results). Up to 10 pages can be fetched per call; pages are fetched in parallel for speed.

IMPORTANT — how to present docs content to the user:
1. Give a direct answer first, then explain. Don't just summarize — be specific about WHERE to find a feature in the UI and HOW it works.
2. Structure around the user's task: lead with what they asked, not the doc's own structure. Include step-by-step actionable detail when the docs describe a workflow or UI feature.
3. Call out practical limitations and edge cases from the docs.
4. Close with clear guidance: if the feature only partially answers the question, say what else they can do and where.
5. ALWAYS include the docs page URL(s) as markdown hyperlinks at the end of your response, e.g. [Page title](https://docs.getdbt.com/...).

VERSION AWARENESS:
The response includes a `dbt_project_version` field with the user's dbt version from their dbt_project.yml. Pages may also have a `version_note` field if they cover an EOL version. If `dbt_project_version` is null, the version could not be detected — present docs normally without version-specific callouts.

When presenting page content to the user, you MUST:
1. If `dbt_project_version` is set, tell the user their version and note any version mismatches with the content.
2. If a page covers features from a newer dbt version, explicitly tell the user: "This feature is available in dbt [version]+. Your project currently uses [their version]. Upgrading would give you access to this."
3. If `version_note` is set, prominently warn the user that the page is for an end-of-life version and the information may be outdated. Suggest they look at the current docs instead.
4. Never silently present content from a different version than the user's without calling it out.
