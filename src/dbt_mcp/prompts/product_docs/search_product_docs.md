Search the dbt product documentation at docs.getdbt.com for pages matching a query. Returns matching page titles, URLs, and descriptions ranked by relevance. Only returns metadata, not page content — use get_product_doc_pages with URLs from the results to retrieve full page content.

If the title/description search finds few matches, it automatically falls back to a deep full-text search across all documentation content to find pages where the topic is discussed in the body text.

If your first query returns few results, try rephrasing with synonyms or the full term (e.g. 'user-defined functions' instead of 'UDFs', 'version control' instead of 'git'). Use the abbreviations on the page as well.
