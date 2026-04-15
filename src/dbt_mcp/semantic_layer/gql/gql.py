GRAPHQL_QUERIES = {
    "metrics": """
query GetMetrics($environmentId: BigInt!, $search: String) {
  metricsPaginated(
    environmentId: $environmentId, search: $search
  ){
    items{
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
query GetDimensions($environmentId: BigInt!, $metrics: [MetricInput!]!, $search: String) {
  dimensionsPaginated(environmentId: $environmentId, metrics: $metrics, search: $search) {
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
query GetEntities($environmentId: BigInt!, $metrics: [MetricInput!]!, $search: String) {
  entitiesPaginated(environmentId: $environmentId, metrics: $metrics, search: $search) {
    items {
      description
      name
      type
    }
  }
}
    """,
    "metrics_with_related": """
query GetMetricsWithRelated($environmentId: BigInt!, $search: String) {
  metricsPaginated(environmentId: $environmentId, search: $search) {
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
query GetSavedQueries($environmentId: BigInt!, $search: String) {
  savedQueriesPaginated(environmentId: $environmentId, search: $search) {
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
          grain
          datePart
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
