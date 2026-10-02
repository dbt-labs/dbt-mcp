GRAPHQL_QUERIES = {
    "metrics": """
query GetMetrics($environmentId: BigInt!, $search: String, $pageNum: Int!, $pageSize: Int!) {
  metricsPaginated(
    environmentId: $environmentId, search: $search,
    pageNum: $pageNum, pageSize: $pageSize
  ) {
    pageNum
    pageSize
    totalItems
    totalPages
    items {
      name
      label
      description
      type
      config {
        meta
      }
    }
  }
}
    """,
    "dimensions": """
query GetDimensions($environmentId: BigInt!, $metrics: [MetricInput!]!, $search: String, $pageNum: Int!, $pageSize: Int!) {
  dimensionsPaginated(environmentId: $environmentId, metrics: $metrics, search: $search, pageNum: $pageNum, pageSize: $pageSize) {
    pageNum
    pageSize
    totalItems
    totalPages
    items {
      description
      name
      type
      queryableGranularities
      queryableTimeGranularities
      label
      config {
        meta
      }
    }
  }
}
    """,
    "entities": """
query GetEntities($environmentId: BigInt!, $metrics: [MetricInput!]!, $search: String, $pageNum: Int!, $pageSize: Int!) {
  entitiesPaginated(environmentId: $environmentId, metrics: $metrics, search: $search, pageNum: $pageNum, pageSize: $pageSize) {
    pageNum
    pageSize
    totalItems
    totalPages
    items {
      description
      name
      type
    }
  }
}
    """,
    "metrics_with_related": """
query GetMetricsWithRelated($environmentId: BigInt!, $search: String, $pageNum: Int!, $pageSize: Int!) {
  metricsPaginated(environmentId: $environmentId, search: $search, pageNum: $pageNum, pageSize: $pageSize) {
    pageNum
    pageSize
    totalItems
    totalPages
    items {
      name
      label
      description
      type
      config {
        meta
      }
      dimensions {
        name
      }
      entities {
        name
      }
    }
  }
}
    """,
    "saved_queries": """
query GetSavedQueries($environmentId: BigInt!, $pageNum: Int!, $pageSize: Int!) {
  savedQueriesPaginated(environmentId: $environmentId, pageNum: $pageNum, pageSize: $pageSize) {
    pageNum
    pageSize
    totalItems
    totalPages
    items {
      name
      description
      label
    }
  }
}
    """,
    "saved_queries_with_params": """
query GetSavedQueriesWithParams($environmentId: BigInt!, $pageNum: Int!, $pageSize: Int!) {
  savedQueriesPaginated(environmentId: $environmentId, pageNum: $pageNum, pageSize: $pageSize) {
    pageNum
    pageSize
    totalItems
    totalPages
    items {
      name
      description
      label
      queryParams {
        metrics {
          name
        }
        groupBy {
          name
        }
        where {
          whereSqlTemplate
        }
      }
    }
  }
}
    """,
}
