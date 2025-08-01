GRAPHQL_QUERIES = {
    "metrics": """
query GetMetrics($environmentId: BigInt!) {
  metrics(environmentId: $environmentId) {
    name
    label
    description
    type
  }
}
    """,
    "dimensions": """
query GetDimensions($environmentId: BigInt!, $metrics: [MetricInput!]!) {
  dimensions(environmentId: $environmentId, metrics: $metrics) {
    description
    name
    type
    queryableGranularities
    queryableTimeGranularities
  }
}
    """,
    "entities": """
query GetEntities($environmentId: BigInt!, $metrics: [MetricInput!]!) {
  entities(environmentId: $environmentId, metrics: $metrics) {
    description
    name
    type
  }
}
    """,
    "compile_sql": """
mutation CompileSql($environmentId: BigInt!, $metrics: [MetricInput!]!, $groupBy: [GroupByInput!]!) {
  compileSql(environmentId: $environmentId, metrics: $metrics, groupBy: $groupBy) {
    sql
  }
}
    """,
}
