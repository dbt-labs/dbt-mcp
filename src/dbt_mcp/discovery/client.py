import textwrap
from typing import Literal, TypedDict

import requests

from dbt_mcp.gql.errors import raise_gql_error

PAGE_SIZE = 100
MAX_NUM_MODELS = 1000


class GraphQLQueries:
    GET_MODELS = textwrap.dedent("""
        query GetModels(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter,
            $after: String,
            $first: Int,
            $sort: AppliedModelSort
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, after: $after, first: $first, sort: $sort) {
                        pageInfo {
                            endCursor
                        }
                        edges {
                            node {
                                name
                                uniqueId
                                description
                            }
                        }
                    }
                }
            }
        }
    """)

    GET_MODEL_DETAILS = textwrap.dedent("""
        query GetModelDetails(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter
            $first: Int,
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, first: $first) {
                        edges {
                            node {
                                name
                                uniqueId
                                compiledCode
                                description
                                database
                                schema
                                catalog {
                                    columns {
                                        description
                                        name
                                        type
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    """)

    COMMON_FIELDS_PARENTS_CHILDREN = textwrap.dedent("""
        {
        ... on ExposureAppliedStateNestedNode {
            resourceType
            name
            description
        }
        ... on ExternalModelNode {
            resourceType
            description
            name
        }
        ... on MacroDefinitionNestedNode {
            resourceType
            name
            description
        }
        ... on MetricDefinitionNestedNode {
            resourceType
            name
            description
        }
        ... on ModelAppliedStateNestedNode {
            resourceType
            name
            description
        }
        ... on SavedQueryDefinitionNestedNode {
            resourceType
            name
            description
        }
        ... on SeedAppliedStateNestedNode {
            resourceType
            name
            description
        }
        ... on SemanticModelDefinitionNestedNode {
            resourceType
            name
            description
        }
        ... on SnapshotAppliedStateNestedNode {
            resourceType
            name
            description
        }
        ... on SourceAppliedStateNestedNode {
            resourceType
            name
            description
        }
        ... on TestAppliedStateNestedNode {
            resourceType
            name
            description
        }
    """)

    GET_MODEL_PARENTS = (
        textwrap.dedent("""
        query GetModelParents(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter
            $first: Int,
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, first: $first) {
                        pageInfo {
                            endCursor
                        }
                        edges {
                            node {
                                parents 
    """)
        + COMMON_FIELDS_PARENTS_CHILDREN
        + textwrap.dedent("""
                                }
                            }
                        }
                    }
                }
            }
        }
    """)
    )

    GET_MODEL_CHILDREN = (
        textwrap.dedent("""
        query GetModelChildren(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter
            $first: Int,
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, first: $first) {
                        pageInfo {
                            endCursor
                        }
                        edges {
                            node {
                                children 
    """)
        + COMMON_FIELDS_PARENTS_CHILDREN
        + textwrap.dedent("""
                                }
                            }
                        }
                    }
                }
            }
        }
    """)
    )


class MetadataAPIClient:
    def __init__(
        self, *, host: str, token: str, multicell_account_prefix: str | None = None
    ):
        if multicell_account_prefix:
            self.url = f"https://{multicell_account_prefix}.metadata.{host}/graphql"
        else:
            self.url = f"https://metadata.{host}/graphql"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def execute_query(self, query: str, variables: dict) -> dict:
        response = requests.post(
            url=self.url,
            json={"query": query, "variables": variables},
            headers=self.headers,
        )
        return response.json()


class ModelFilter(TypedDict, total=False):
    modelingLayer: Literal["marts"] | None


class ModelsFetcher:
    def __init__(self, api_client: MetadataAPIClient, environment_id: int):
        self.api_client = api_client
        self.environment_id = environment_id

    def _parse_response_to_json(self, result: dict) -> list[dict]:
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        parsed_edges: list[dict] = []
        if not edges:
            return parsed_edges
        if result.get("errors"):
            raise Exception(f"GraphQL query failed: {result['errors']}")
        for edge in edges:
            if not isinstance(edge, dict) or "node" not in edge:
                continue
            node = edge["node"]
            if not isinstance(node, dict):
                continue
            parsed_edges.append(node)
        return parsed_edges

    def fetch_models(self, model_filter: ModelFilter | None = None) -> list[dict]:
        has_next_page = True
        after_cursor: str = ""
        all_edges: list[dict] = []
        while has_next_page and len(all_edges) < MAX_NUM_MODELS:
            variables = {
                "environmentId": self.environment_id,
                "after": after_cursor,
                "first": PAGE_SIZE,
                "modelsFilter": model_filter or {},
                "sort": {"field": "queryUsageCount", "direction": "desc"},
            }

            result = self.api_client.execute_query(GraphQLQueries.GET_MODELS, variables)
            all_edges.extend(self._parse_response_to_json(result))

            previous_after_cursor = after_cursor
            after_cursor = result["data"]["environment"]["applied"]["models"][
                "pageInfo"
            ]["endCursor"]
            if previous_after_cursor == after_cursor:
                has_next_page = False

        return all_edges

    def fetch_model_details(
        self, model_name: str, unique_id: str | None = None
    ) -> dict:
        model_filters: dict[str, list[str] | str] = (
            {"uniqueIds": [unique_id]} if unique_id else {"identifier": model_name}
        )
        variables = {
            "environmentId": self.environment_id,
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_DETAILS, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return {}
        return edges[0]["node"]

    def fetch_model_parents(
        self, model_name: str, unique_id: str | None = None
    ) -> list[dict]:
        model_filters: dict[str, list[str] | str] = (
            {"uniqueIds": [unique_id]} if unique_id else {"identifier": model_name}
        )
        variables = {
            "environmentId": self.environment_id,
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_PARENTS, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return []
        return edges[0]["node"]["parents"]

    def fetch_model_children(
        self, model_name: str, unique_id: str | None = None
    ) -> list[dict]:
        model_filters: dict[str, list[str] | str] = (
            {"uniqueIds": [unique_id]} if unique_id else {"identifier": model_name}
        )
        variables = {
            "environmentId": self.environment_id,
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_CHILDREN, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return []
        return edges[0]["node"]["children"]
