import asyncio
import textwrap
from enum import StrEnum
from typing import Any, ClassVar, Literal, TypedDict

import httpx
from pydantic import BaseModel, ConfigDict, Field

from dbt_mcp.config.config_providers import ConfigProvider, DiscoveryConfig
from dbt_mcp.discovery.graphql import load_query
from dbt_mcp.errors import InvalidParameterError
from dbt_mcp.gql.errors import raise_gql_error

DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_NODE_QUERY_LIMIT = 10000


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
            sourceName
            uniqueId
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

    GET_SOURCES = textwrap.dedent("""
        query GetSources(
            $environmentId: BigInt!,
            $sourcesFilter: SourceAppliedFilter,
            $after: String,
            $first: Int
        ) {
            environment(id: $environmentId) {
                applied {
                    sources(filter: $sourcesFilter, after: $after, first: $first) {
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        edges {
                            node {
                                name
                                uniqueId
                                identifier
                                description
                                sourceName
                                resourceType
                                database
                                schema
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
    """)

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


class MetadataAPIClient:
    def __init__(self, config_provider: ConfigProvider[DiscoveryConfig]):
        self.config_provider = config_provider

    async def execute_query(self, query: str, variables: dict) -> dict:
        config = await self.config_provider.get_config()
        url = config.url
        headers = config.headers_provider.get_headers()

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url=url,
                json={"query": query, "variables": variables},
                headers=headers,
            )
            response.raise_for_status()
            return response.json()


class PageInfo(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    end_cursor: str | None = Field(default=None, alias="endCursor")
    has_next_page: bool | None = Field(default=None, alias="hasNextPage")


class PaginatedResourceFetcher:
    def __init__(
        self,
        api_client: MetadataAPIClient,
        *,
        edges_path: tuple[str, ...],
        page_info_path: tuple[str, ...],
        page_size: int = DEFAULT_PAGE_SIZE,
        max_node_query_limit: int = DEFAULT_MAX_NODE_QUERY_LIMIT,
    ):
        self.api_client = api_client
        self._edges_path = edges_path
        self._page_info_path = page_info_path
        self._page_size = page_size
        self._max_node_query_limit = max_node_query_limit

    async def get_environment_id(self) -> int:
        config = await self.api_client.config_provider.get_config()
        return config.environment_id

    def _extract_path(self, payload: dict, path: tuple[str, ...]) -> Any:
        current = payload
        for key in path:
            current = current[key]
        return current

    def _parse_edges(self, result: dict) -> list[dict]:
        raise_gql_error(result)
        edges = self._extract_path(result, self._edges_path)
        parsed_edges: list[dict] = []
        if not edges:
            return parsed_edges
        for edge in edges:
            if not isinstance(edge, dict) or "node" not in edge:
                continue
            node = edge["node"]
            if not isinstance(node, dict):
                continue
            parsed_edges.append(node)
        return parsed_edges

    def _should_continue(
        self,
        page_info: PageInfo,
        previous_cursor: str | None,
    ) -> bool:
        next_cursor = page_info.end_cursor
        has_next = page_info.has_next_page
        next_cursor_valid = bool(next_cursor) and next_cursor != previous_cursor
        if isinstance(has_next, bool):
            return has_next and next_cursor_valid
        return next_cursor_valid

    async def fetch_paginated(
        self,
        query: str,
        variables: dict[str, Any],
    ) -> list[dict]:
        environment_id = await self.get_environment_id()
        collected: list[dict] = []
        current_cursor: str | None = None
        while True:
            if len(collected) >= self._max_node_query_limit:
                break
            remaining_capacity = self._max_node_query_limit - len(collected)
            request_variables = variables.copy()
            request_variables["environmentId"] = environment_id
            request_variables["first"] = min(self._page_size, remaining_capacity)
            if current_cursor is not None:
                request_variables["after"] = current_cursor
            result = await self.api_client.execute_query(query, request_variables)
            page_edges = self._parse_edges(result)
            collected.extend(page_edges)
            page_info_data = self._extract_path(result, self._page_info_path)
            page_info = PageInfo(**page_info_data)
            previous_cursor = current_cursor
            current_cursor = page_info.end_cursor
            if not self._should_continue(page_info, previous_cursor):
                break
        return collected


class ModelFilter(TypedDict, total=False):
    modelingLayer: Literal["marts"] | None


class SourceFilter(TypedDict, total=False):
    sourceNames: list[str]
    uniqueIds: list[str] | None
    identifier: str


class ModelsFetcher:
    def __init__(
        self,
        api_client: MetadataAPIClient,
        paginator: PaginatedResourceFetcher,
    ):
        self.api_client = api_client
        self._paginator = paginator

    def _get_model_filters(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> dict[str, list[str] | str]:
        if unique_id:
            return {"uniqueIds": [unique_id]}
        elif model_name:
            return {"identifier": model_name}
        else:
            raise InvalidParameterError(
                "Either model_name or unique_id must be provided"
            )

    async def fetch_models(self, model_filter: ModelFilter | None = None) -> list[dict]:
        return await self._paginator.fetch_paginated(
            GraphQLQueries.GET_MODELS,
            variables={
                "modelsFilter": model_filter or {},
                "sort": {"field": "queryUsageCount", "direction": "desc"},
            },
        )

    async def fetch_model_parents(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict]:
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self._paginator.get_environment_id(),
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
            "environmentId": await self._paginator.get_environment_id(),
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
            "environmentId": await self._paginator.get_environment_id(),
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
    def __init__(
        self,
        api_client: MetadataAPIClient,
        paginator: PaginatedResourceFetcher,
    ):
        self.api_client = api_client
        self._paginator = paginator

    async def fetch_exposures(self) -> list[dict]:
        return await self._paginator.fetch_paginated(
            GraphQLQueries.GET_EXPOSURES,
            variables={},
        )


class SourcesFetcher:
    def __init__(
        self,
        api_client: MetadataAPIClient,
        paginator: PaginatedResourceFetcher,
    ):
        self.api_client = api_client
        self._paginator = paginator

    async def get_environment_id(self) -> int:
        return await self._paginator.get_environment_id()

    async def fetch_sources(
        self,
        source_names: list[str] | None = None,
        unique_ids: list[str] | None = None,
    ) -> list[dict]:
        source_filter: SourceFilter = {}
        if source_names is not None:
            source_filter["sourceNames"] = source_names
        if unique_ids is not None:
            source_filter["uniqueIds"] = unique_ids

        return await self._paginator.fetch_paginated(
            GraphQLQueries.GET_SOURCES,
            variables={"sourcesFilter": source_filter},
        )


class AppliedResourceType(StrEnum):
    MODEL = "model"
    SOURCE = "source"
    EXPOSURE = "exposure"
    TEST = "test"
    SEED = "seed"
    SNAPSHOT = "snapshot"
    MACRO = "macro"
    SEMANTIC_MODEL = "semantic_model"


class ResourceDetailsFetcher:
    GET_PACKAGES_QUERY = load_query("get_packages.gql")
    GET_MODELS_DETAILS_QUERY = load_query("get_model_details.gql")
    GET_SOURCES_QUERY = load_query("get_source_details.gql")
    GET_EXPOSURES_QUERY = load_query("get_exposure_details.gql")
    GET_TESTS_QUERY = load_query("get_test_details.gql")
    GET_SEEDS_QUERY = load_query("get_seed_details.gql")
    GET_SNAPSHOTS_QUERY = load_query("get_snapshot_details.gql")
    GET_MACROS_QUERY = load_query("get_macro_details.gql")
    GET_SEMANTIC_MODELS_QUERY = load_query("get_semantic_model_details.gql")

    GQL_QUERIES: ClassVar[dict[AppliedResourceType, str]] = {
        AppliedResourceType.MODEL: GET_MODELS_DETAILS_QUERY,
        AppliedResourceType.SOURCE: GET_SOURCES_QUERY,
        AppliedResourceType.EXPOSURE: GET_EXPOSURES_QUERY,
        AppliedResourceType.TEST: GET_TESTS_QUERY,
        AppliedResourceType.SEED: GET_SEEDS_QUERY,
        AppliedResourceType.SNAPSHOT: GET_SNAPSHOTS_QUERY,
        AppliedResourceType.MACRO: GET_MACROS_QUERY,
        AppliedResourceType.SEMANTIC_MODEL: GET_SEMANTIC_MODELS_QUERY,
    }

    RESOURCE_TYPE_TO_GQL_TYPE: ClassVar[dict[AppliedResourceType, str]] = {
        AppliedResourceType.MODEL: "Model",
        AppliedResourceType.SOURCE: "Source",
        AppliedResourceType.EXPOSURE: "Exposure",
        AppliedResourceType.TEST: "Test",
        AppliedResourceType.SEED: "Seed",
        AppliedResourceType.SNAPSHOT: "Snapshot",
        AppliedResourceType.MACRO: "Macro",
        AppliedResourceType.SEMANTIC_MODEL: "SemanticModel",
    }

    def __init__(
        self,
        api_client: MetadataAPIClient,
    ):
        self.api_client = api_client

    async def fetch_details(
        self,
        resource_type: AppliedResourceType,
        name: str | None = None,
        unique_id: str | None = None,
    ) -> list[dict]:
        normalized_name = name.strip().lower() if name else None
        normalized_unique_id = unique_id.strip().lower() if unique_id else None
        environment_id = (
            await self.api_client.config_provider.get_config()
        ).environment_id
        if not normalized_name and not normalized_unique_id:
            raise InvalidParameterError("Either name or unique_id must be provided")
        if (
            normalized_name
            and normalized_unique_id
            and normalized_name != normalized_unique_id.split(".")[-1]
        ):
            raise InvalidParameterError(
                f"Name and unique_id do not match. The unique_id does not end with {normalized_name}."
            )
        if not normalized_unique_id:
            assert normalized_name is not None, "Name must be provided"
            packages_result = await asyncio.gather(
                self.api_client.execute_query(
                    self.GET_PACKAGES_QUERY,
                    variables={"resource": "macro", "environmentId": environment_id},
                ),
                self.api_client.execute_query(
                    self.GET_PACKAGES_QUERY,
                    variables={"resource": "model", "environmentId": environment_id},
                ),
            )
            raise_gql_error(packages_result[0])
            raise_gql_error(packages_result[1])
            macro_packages = packages_result[0]["data"]["environment"]["applied"][
                "packages"
            ]
            model_packages = packages_result[1]["data"]["environment"]["applied"][
                "packages"
            ]
            if not macro_packages and not model_packages:
                raise InvalidParameterError("No packages found for project")
            unique_ids = [
                f"{resource_type.value.lower()}.{package_name}.{normalized_name}"
                for package_name in macro_packages + model_packages
            ]
        else:
            unique_ids = [normalized_unique_id]
        query = self.GQL_QUERIES[resource_type]
        variables = {
            "environmentId": environment_id,
            "filter": {
                "uniqueIds": unique_ids,
                "types": [self.RESOURCE_TYPE_TO_GQL_TYPE[resource_type]],
            },
            "first": len(unique_ids),
        }
        get_details_result = await self.api_client.execute_query(query, variables)
        raise_gql_error(get_details_result)
        edges = get_details_result["data"]["environment"]["applied"]["resources"][
            "edges"
        ]
        if not edges:
            return []
        return [e["node"] for e in edges]
