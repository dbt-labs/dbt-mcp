List all saved queries from the dbt Semantic Layer.

Saved queries are pre-defined queries that have been saved in the Semantic Layer. 
They contain specific metrics, group-by dimensions, and filters that are commonly used.
This tool helps discover what predefined queries are available for quick execution.

Use this tool when:
- The user wants to see what pre-built queries are available
- The user mentions "saved queries" or "predefined queries"
- You need to find commonly used analytical queries

Examples:
- "What saved queries are available?"
- "Show me the predefined reports"
- "List all saved queries for revenue analysis"

Returns one source page in `result`, with `pagination.has_more` and `pagination.next_page`. Pass the next page as `page_num` with the same filters and `page_size` (default 50, maximum 100). Local filtering may leave an empty page with more pages available. Restart at page 1 if you reduce the page size after a size-limit error.
