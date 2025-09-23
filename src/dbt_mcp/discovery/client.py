import textwrap
from typing import Literal, TypedDict

import requests

from dbt_mcp.config.config_providers import ConfigProvider, DiscoveryConfig
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

    GET_MODEL_HEALTH = textwrap.dedent("""
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
                                executionInfo {
                                    lastRunGeneratedAt
                                    lastRunStatus
                                    executeCompletedAt
                                    executeStartedAt
                                }
                                tests {
                                    name
                                    description
                                    columnName
                                    testType
                                    executionInfo {
                                        lastRunGeneratedAt
                                        lastRunStatus
                                        executeCompletedAt
                                        executeStartedAt
                                    }
                                }
                                ancestors(types: [Model, Source, Seed, Snapshot]) {
                                  ... on ModelAppliedStateNestedNode {
                                    name
                                    uniqueId
                                    resourceType
                                    materializedType
                                    modelexecutionInfo: executionInfo {
                                      lastRunStatus
                                      executeCompletedAt
                                      }
                                  }
                                  ... on SnapshotAppliedStateNestedNode {
                                    name
                                    uniqueId
                                    resourceType
                                    snapshotExecutionInfo: executionInfo {
                                      lastRunStatus
                                      executeCompletedAt
                                    }
                                  }
                                  ... on SeedAppliedStateNestedNode {
                                    name
                                    uniqueId
                                    resourceType
                                    seedExecutionInfo: executionInfo {
                                      lastRunStatus
                                      executeCompletedAt
                                    }
                                  }
                                  ... on SourceAppliedStateNestedNode {
                                    sourceName
                                    name
                                    resourceType
                                    freshness {
                                      maxLoadedAt
                                      maxLoadedAtTimeAgoInS
                                      freshnessStatus
                                    }
                                  }
                              }
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
                                alias
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

    GET_EXPOSURES = textwrap.dedent("""
        query Exposures($environmentId: BigInt!, $first: Int, $after: String) {
            environment(id: $environmentId) {
                definition {
                    exposures(first: $first, after: $after) {
                        totalCount
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        edges {
                            node {
                                name
                                uniqueId
                                url
                                description
                            }
                        }
                    }
                }
            }
        }
    """)

    GET_EXPOSURE_DETAILS = textwrap.dedent("""
        query ExposureDetails($environmentId: BigInt!, $filter: ExposureFilter, $first: Int) {
            environment(id: $environmentId) {
                definition {
                    exposures(first: $first, filter: $filter) {
                        edges {
                            node {
                                name
                                maturity
                                label
                                ownerEmail
                                ownerName
                                uniqueId
                                url
                                meta
                                freshnessStatus
                                exposureType
                                description
                                parents {
                                    uniqueId
                                }
                            }
                        }
                    }
                }
            }
        }
    """)


class MetadataAPIClient:
    def __init__(self, config_provider: ConfigProvider[DiscoveryConfig]):
        self.config_provider = config_provider

    async def execute_query(self, query: str, variables: dict) -> dict:
        config = await self.config_provider.get_config()
        url = config.url
        headers = config.headers_provider.get_headers()
        response = requests.post(
            url=url,
            json={"query": query, "variables": variables},
            headers=headers,
        )
        return response.json()


class ModelFilter(TypedDict, total=False):
    modelingLayer: Literal["marts"] | None


class ModelsFetcher:
    def __init__(self, api_client: MetadataAPIClient):
        self.api_client = api_client

    async def get_environment_id(self) -> int:
        config = await self.api_client.config_provider.get_config()
        return config.environment_id

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

    def _get_model_filters(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> dict[str, list[str] | str]:
        if unique_id:
            return {"uniqueIds": [unique_id]}
        elif model_name:
            return {"identifier": model_name}
        else:
            raise ValueError("Either model_name or unique_id must be provided")

    async def fetch_models(self, model_filter: ModelFilter | None = None) -> list[dict]:
        has_next_page = True
        after_cursor: str = ""
        all_edges: list[dict] = []
        while has_next_page and len(all_edges) < MAX_NUM_MODELS:
            variables = {
                "environmentId": await self.get_environment_id(),
                "after": after_cursor,
                "first": PAGE_SIZE,
                "modelsFilter": model_filter or {},
                "sort": {"field": "queryUsageCount", "direction": "desc"},
            }

            result = await self.api_client.execute_query(
                GraphQLQueries.GET_MODELS, variables
            )
            all_edges.extend(self._parse_response_to_json(result))

            previous_after_cursor = after_cursor
            after_cursor = result["data"]["environment"]["applied"]["models"][
                "pageInfo"
            ]["endCursor"]
            if previous_after_cursor == after_cursor:
                has_next_page = False

        return all_edges

    async def fetch_model_details(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> dict:
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_DETAILS, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return {}
        return edges[0]["node"]

    async def fetch_model_parents(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict]:
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_PARENTS, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return []
        return edges[0]["node"]["parents"]

    async def fetch_model_children(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict]:
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_CHILDREN, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return []
        return edges[0]["node"]["children"]

    async def fetch_model_health(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict]:
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_HEALTH, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return []
        return edges[0]["node"]


class ExposuresFetcher:
    def __init__(self, api_client: MetadataAPIClient):
        self.api_client = api_client

    async def get_environment_id(self) -> int:
        config = await self.api_client.config_provider.get_config()
        return config.environment_id

    def _parse_response_to_json(self, result: dict) -> list[dict]:
        raise_gql_error(result)
        edges = result["data"]["environment"]["definition"]["exposures"]["edges"]
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

    async def fetch_exposures(self) -> list[dict]:
        has_next_page = True
        after_cursor: str | None = None
        all_edges: list[dict] = []

        while has_next_page:
            variables: dict[str, int | str] = {
                "environmentId": await self.get_environment_id(),
                "first": PAGE_SIZE,
            }
            if after_cursor:
                variables["after"] = after_cursor

            result = await self.api_client.execute_query(
                GraphQLQueries.GET_EXPOSURES, variables
            )
            new_edges = self._parse_response_to_json(result)
            all_edges.extend(new_edges)

            page_info = result["data"]["environment"]["definition"]["exposures"][
                "pageInfo"
            ]
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")

        return all_edges

    def _get_exposure_filters(
        self, exposure_name: str | None = None, unique_ids: list[str] | None = None
    ) -> dict[str, list[str]]:
        if unique_ids:
            return {"uniqueIds": unique_ids}
        elif exposure_name:
            raise ValueError(
                "ExposureFilter only supports uniqueIds. Please use unique_ids parameter instead of exposure_name."
            )
        else:
            raise ValueError("unique_ids must be provided for exposure filtering")

    async def fetch_exposure_details(
        self, exposure_name: str | None = None, unique_ids: list[str] | None = None
    ) -> list[dict]:
        if exposure_name and not unique_ids:
            # Since ExposureFilter doesn't support filtering by name,
            # we need to fetch all exposures and find the one with matching name
            all_exposures = await self.fetch_exposures()
            for exposure in all_exposures:
                if exposure.get("name") == exposure_name:
                    return [exposure]
            return []
        elif unique_ids:
            exposure_filters = self._get_exposure_filters(unique_ids=unique_ids)
            variables = {
                "environmentId": await self.get_environment_id(),
                "filter": exposure_filters,
                "first": len(unique_ids),  # Request as many as we're filtering for
            }
            result = await self.api_client.execute_query(
                GraphQLQueries.GET_EXPOSURE_DETAILS, variables
            )
            raise_gql_error(result)
            edges = result["data"]["environment"]["definition"]["exposures"]["edges"]
            if not edges:
                return []
            return [edge["node"] for edge in edges]
        else:
            raise ValueError("Either exposure_name or unique_ids must be provided")
