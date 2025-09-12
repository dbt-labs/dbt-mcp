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
}
