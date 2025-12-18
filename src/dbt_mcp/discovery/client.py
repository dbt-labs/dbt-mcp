import asyncio
import logging
import textwrap
from enum import StrEnum
from typing import Any, ClassVar, Literal, TypedDict

import httpx
from pydantic import BaseModel, ConfigDict, Field

from dbt_mcp.config.config_providers import ConfigProvider, DiscoveryConfig
from dbt_mcp.discovery.graphql import load_query
from dbt_mcp.errors import InvalidParameterError
from dbt_mcp.gql.errors import raise_gql_error

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_NODE_QUERY_LIMIT = 10000
LINEAGE_LIMIT = 50  # Max nodes per direction for Lineage Tool(ancestors/descendants)


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

    # Detail queries - reused for lineage search
    GET_MODEL_DETAILS = load_query("get_model_details.gql")
    GET_SOURCE_DETAILS = load_query("get_source_details.gql")
    GET_SEED_DETAILS = load_query("get_seed_details.gql")
    GET_SNAPSHOT_DETAILS = load_query("get_snapshot_details.gql")

    # Lineage queries
    GET_NODE_RELATIONS = load_query("lineage/get_node_relations.gql")


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


class LineageDirection(StrEnum):
    """Direction for lineage traversal."""

    ANCESTORS = "ancestors"
    DESCENDANTS = "descendants"
    BOTH = "both"


class LineageResourceType(StrEnum):
    """Resource types supported by the lineage API."""

    MODEL = "Model"
    SOURCE = "Source"
    SEED = "Seed"
    SNAPSHOT = "Snapshot"
    EXPOSURE = "Exposure"
    METRIC = "Metric"
    SEMANTIC_MODEL = "SemanticModel"
    SAVED_QUERY = "SavedQuery"
    MACRO = "Macro"
    TEST = "Test"


_RESOURCE_SEARCH_CONFIG = {
    "Model": {
        "query": GraphQLQueries.GET_MODEL_DETAILS,
        "response_path": "resources",
        "gql_type": "Model",
    },
    "Source": {
        "query": GraphQLQueries.GET_SOURCE_DETAILS,
        "response_path": "resources",
        "gql_type": "Source",
    },
    "Seed": {
        "query": GraphQLQueries.GET_SEED_DETAILS,
        "response_path": "resources",
        "gql_type": "Seed",
    },
    "Snapshot": {
        "query": GraphQLQueries.GET_SNAPSHOT_DETAILS,
        "response_path": "resources",
        "gql_type": "Snapshot",
    },
}


class LineageFetcher:
    """Fetcher for lineage data using the Discovery API's lineage query."""

    def __init__(self, api_client: MetadataAPIClient):
        self.api_client = api_client

    async def get_environment_id(self) -> int:
        config = await self.api_client.config_provider.get_config()
        return config.environment_id

    async def search_resource_by_name(
        self, name: str, resource_type: str
    ) -> list[dict]:
        """Search for a resource by name/identifier.

        Generic method that handles searching for any supported resource type.

        Args:
            name: The resource name/identifier to search for
            resource_type: Type of resource ("Model", "Source", "Seed", "Snapshot")

        Returns:
            List of matches with uniqueId, name, and resourceType keys

        Raises:
            ValueError: If resource_type is not supported
        """
        if resource_type not in _RESOURCE_SEARCH_CONFIG:
            raise ValueError(
                f"Unsupported resource_type: {resource_type}. "
                f"Must be one of: {', '.join(_RESOURCE_SEARCH_CONFIG.keys())}"
            )

        config = _RESOURCE_SEARCH_CONFIG[resource_type]
        environment_id = await self.get_environment_id()

        packages_result = await self.api_client.execute_query(
            ResourceDetailsFetcher.GET_PACKAGES_QUERY,
            variables={"resource": "model", "environmentId": environment_id},
        )
        raise_gql_error(packages_result)

        packages = packages_result["data"]["environment"]["applied"]["packages"]
        if not packages:
            return []

        resource_type_lower = resource_type.lower()
        unique_ids = [
            f"{resource_type_lower}.{package_name}.{name}" for package_name in packages
        ]

        variables = {
            "environmentId": environment_id,
            "filter": {
                "uniqueIds": unique_ids,
                "types": [config["gql_type"]],
            },
            "first": len(unique_ids),
        }

        query = config["query"]
        result = await self.api_client.execute_query(query, variables)
        raise_gql_error(result)

        edges = result["data"]["environment"]["applied"]["resources"]["edges"]
        if not edges:
            return []

        return [
            {
                "uniqueId": edge["node"]["uniqueId"],
                "name": edge["node"]["name"],
                "resourceType": resource_type,
            }
            for edge in edges
        ]

    async def search_all_resources(self, name: str) -> list[dict]:
        """Search for resources by name across all supported types.

        Returns all matches found across all resource types.
        """
        tasks = [
            self.search_resource_by_name(name, resource_type)
            for resource_type in _RESOURCE_SEARCH_CONFIG
        ]
        results = await asyncio.gather(*tasks)

        matches: list[dict] = []
        for result in results:
            matches.extend(result)

        return matches

    async def _fetch_node_with_relations(
        self,
        unique_id: str,
    ) -> dict | None:
        """
        Fetch a single node with its parent/child relationships.

        Supports all resource types: models, sources, seeds, snapshots, exposures.

        Args:
            unique_id: The unique ID of the node (e.g., model.jaffle_shop.customers)

        Returns:
            Node dict with parents/children arrays, or None if not found
        """
        variables = {
            "environmentId": await self.get_environment_id(),
            "uniqueId": unique_id,
        }

        result = await self.api_client.execute_query(
            GraphQLQueries.GET_NODE_RELATIONS, variables
        )
        raise_gql_error(result)

        applied = result.get("data", {}).get("environment", {}).get("applied", {})

        for resource_type in ["models", "sources", "seeds", "snapshots", "exposures"]:
            edges = applied.get(resource_type, {}).get("edges", [])
            if edges:
                node = edges[0]["node"]

                if "children" in node:
                    node["children"] = [
                        c for c in node["children"] if c.get("uniqueId")
                    ]

                return node

        return None

    def _build_batch_node_query(self, batch_specs: dict[str, list[str]]) -> str:
        """
        Build GraphQL query that fetches multiple nodes using aliases.

        Args:
            batch_specs: Dict mapping resource types to lists of unique_ids
                        e.g., {"Model": ["model.x.a", "model.x.b"],
                               "Source": ["source.x.c"]}

        Returns:
            GraphQL query string with aliases
        """
        resource_queries = []

        for resource_type, unique_ids in batch_specs.items():
            resource_type_lower = resource_type.lower()
            resource_type_plural = f"{resource_type_lower}s"
            alias = f"batch_{resource_type_lower}"
            count = len(unique_ids)

            ids_str = ", ".join(f'"{uid}"' for uid in unique_ids)

            if resource_type_lower == "model":
                resource_queries.append(f"""
      {alias}: {resource_type_plural}(filter: {{ uniqueIds: [{ids_str}] }}, first: {count}) {{
        edges {{
          node {{
            uniqueId
            name
            database
            schema
            alias
            resourceType
            filePath
            tags
            fqn
            parents {{
              ... on ModelAppliedStateNestedNode {{ uniqueId resourceType name }}
              ... on SourceAppliedStateNestedNode {{ uniqueId resourceType name sourceName }}
              ... on SeedAppliedStateNestedNode {{ uniqueId resourceType name }}
              ... on SnapshotAppliedStateNestedNode {{ uniqueId resourceType name }}
            }}
            children {{
              ... on ModelAppliedStateNestedNode {{ uniqueId resourceType name }}
              ... on ExposureAppliedStateNestedNode {{ uniqueId resourceType name }}
              ... on TestAppliedStateNestedNode {{ uniqueId resourceType name }}
            }}
          }}
        }}
      }}""")
            elif resource_type_lower == "source":
                resource_queries.append(f"""
      {alias}: {resource_type_plural}(filter: {{ uniqueIds: [{ids_str}] }}, first: {count}) {{
        edges {{
          node {{
            uniqueId
            name
            database
            schema
            sourceName
            resourceType
            filePath
            tags
            fqn
            children {{
              ... on ModelAppliedStateNestedNode {{ uniqueId resourceType name }}
            }}
          }}
        }}
      }}""")
            elif resource_type_lower == "seed":
                resource_queries.append(f"""
      {alias}: {resource_type_plural}(filter: {{ uniqueIds: [{ids_str}] }}, first: {count}) {{
        edges {{
          node {{
            uniqueId
            name
            database
            schema
            alias
            resourceType
            filePath
            tags
            fqn
            children {{
              ... on ModelAppliedStateNestedNode {{ uniqueId resourceType name }}
            }}
          }}
        }}
      }}""")
            elif resource_type_lower == "snapshot":
                resource_queries.append(f"""
      {alias}: {resource_type_plural}(filter: {{ uniqueIds: [{ids_str}] }}, first: {count}) {{
        edges {{
          node {{
            uniqueId
            name
            database
            schema
            alias
            resourceType
            filePath
            tags
            fqn
            parents {{
              ... on ModelAppliedStateNestedNode {{ uniqueId resourceType name }}
              ... on SourceAppliedStateNestedNode {{ uniqueId resourceType name sourceName }}
              ... on SeedAppliedStateNestedNode {{ uniqueId resourceType name }}
            }}
            children {{
              ... on ModelAppliedStateNestedNode {{ uniqueId resourceType name }}
              ... on ExposureAppliedStateNestedNode {{ uniqueId resourceType name }}
            }}
          }}
        }}
      }}""")
            elif resource_type_lower == "exposure":
                resource_queries.append(f"""
      {alias}: {resource_type_plural}(filter: {{ uniqueIds: [{ids_str}] }}, first: {count}) {{
        edges {{
          node {{
            uniqueId
            name
            resourceType
            filePath
            tags
            fqn
            parents {{
              ... on ModelAppliedStateNestedNode {{ uniqueId resourceType name }}
              ... on SourceAppliedStateNestedNode {{ uniqueId resourceType name sourceName }}
            }}
          }}
        }}
      }}""")

        query_body = "\n".join(resource_queries)

        return f"""
query BatchNodeRelations($environmentId: BigInt!) {{
  environment(id: $environmentId) {{
    applied {{
{query_body}
    }}
  }}
}}
"""

    def _parse_batch_response(self, response: dict) -> dict[str, dict]:
        """
        Parse batched query response into unique_id -> node mapping.

        Args:
            response: GraphQL response with aliased sections

        Returns:
            Dict mapping unique_id to node data
        """
        nodes = {}
        applied = response.get("data", {}).get("environment", {}).get("applied", {})

        for alias_key, resource_data in applied.items():
            if not alias_key.startswith("batch_"):
                continue

            edges = resource_data.get("edges", [])
            for edge in edges:
                node = edge["node"]
                unique_id = node.get("uniqueId")
                if unique_id:
                    if "children" in node:
                        node["children"] = [
                            c for c in node["children"] if c.get("uniqueId")
                        ]
                    nodes[unique_id] = node

        return nodes

    async def _fetch_nodes_batch(self, unique_ids: list[str]) -> dict[str, dict]:
        """
        Fetch multiple nodes in single batched GraphQL query.

        Args:
            unique_ids: List of node unique IDs to fetch

        Returns:
            Dict mapping unique_id to node data with parents/children
        """
        if not unique_ids:
            return {}

        try:
            batch_specs: dict[str, list[str]] = {}
            for unique_id in unique_ids:
                resource_type = unique_id.split(".")[0].capitalize()
                if resource_type not in batch_specs:
                    batch_specs[resource_type] = []
                batch_specs[resource_type].append(unique_id)

            query = self._build_batch_node_query(batch_specs)

            variables = {"environmentId": await self.get_environment_id()}
            result = await self.api_client.execute_query(query, variables)
            raise_gql_error(result)

            return self._parse_batch_response(result)

        except Exception as e:
            logger.warning(
                f"Batch query failed, falling back to individual queries: {e}"
            )
            batch_results = {}
            for node_id in unique_ids:
                try:
                    node = await self._fetch_node_with_relations(node_id)
                    if node:
                        batch_results[node_id] = node
                except Exception:
                    logger.debug(f"Failed to fetch node {node_id}")
                    continue
            return batch_results

    async def _fetch_ancestors_recursive(
        self,
        unique_id: str,
        types: list[LineageResourceType] | None = None,
        max_depth: int = 100,
        target_node: dict | None = None,
    ) -> tuple[list[dict], set[str]]:
        """
        Recursively fetch all ancestors using batched queries.

        Args:
            unique_id: Starting node unique ID
            types: Optional filter for resource types
            max_depth: Maximum recursion depth
            target_node: Optional pre-fetched target node to avoid redundant fetch

        Returns:
            Tuple of (ancestor_list, visited_ids)
        """
        ancestors = []
        visited = {unique_id}
        current_level = [unique_id]
        depth = 0

        while current_level and depth < max_depth:
            if depth == 0 and target_node:
                batch_results = {unique_id: target_node}
            else:
                batch_results = await self._fetch_nodes_batch(current_level)

            next_level = []
            for node_id in current_level:
                node = batch_results.get(node_id)
                if not node:
                    continue

                parents = node.get("parents", [])
                for parent in parents:
                    parent_id = parent.get("uniqueId")
                    if not parent_id or parent_id in visited:
                        continue

                    visited.add(parent_id)
                    parent_type = parent.get("resourceType")

                    if types and parent_type.lower() not in [
                        t.value.lower() for t in types
                    ]:
                        continue

                    ancestors.append(
                        {
                            "uniqueId": parent_id,
                            "name": parent.get("name"),
                            "resourceType": parent_type,
                        }
                    )

                    if parent_type.lower() == "model":
                        next_level.append(parent_id)

            current_level = next_level
            depth += 1

        if depth >= max_depth and current_level:
            logger.warning(
                f"Max recursion depth {max_depth} reached. Lineage may be incomplete."
            )

        return (ancestors, visited)

    async def _fetch_descendants_recursive(
        self,
        unique_id: str,
        types: list[LineageResourceType] | None = None,
        max_depth: int = 100,
        target_node: dict | None = None,
    ) -> tuple[list[dict], set[str]]:
        """
        Recursively fetch all descendants using batched queries.

        Args:
            unique_id: Starting node unique ID
            types: Optional filter for resource types
            max_depth: Maximum recursion depth
            target_node: Optional pre-fetched target node to avoid redundant fetch

        Returns:
            Tuple of (descendant_list, visited_ids)
        """
        descendants = []
        visited = {unique_id}
        current_level = [unique_id]
        depth = 0

        while current_level and depth < max_depth:
            if depth == 0 and target_node:
                batch_results = {unique_id: target_node}
            else:
                batch_results = await self._fetch_nodes_batch(current_level)

            next_level = []
            for node_id in current_level:
                node = batch_results.get(node_id)
                if not node:
                    continue

                children = node.get("children", [])
                for child in children:
                    child_id = child.get("uniqueId")
                    if not child_id or child_id in visited:
                        continue

                    visited.add(child_id)
                    child_type = child.get("resourceType")

                    if types and child_type.lower() not in [
                        t.value.lower() for t in types
                    ]:
                        continue

                    descendants.append(
                        {
                            "uniqueId": child_id,
                            "name": child.get("name"),
                            "resourceType": child_type,
                        }
                    )

                    if child_type.lower() == "model":
                        next_level.append(child_id)

            current_level = next_level
            depth += 1

        if depth >= max_depth and current_level:
            logger.warning(
                f"Max recursion depth {max_depth} reached. Lineage may be incomplete."
            )

        return (descendants, visited)

    async def fetch_lineage(
        self,
        unique_id: str,
        types: list[LineageResourceType],
        direction: LineageDirection = LineageDirection.BOTH,
    ) -> dict:
        """
        Fetch lineage for a resource using recursive traversal.

        This method uses the parents/children fields instead of selector-based queries
        because the selector approach (+uniqueId syntax) returns empty results in some
        Discovery API environments.

        Args:
            unique_id: The dbt unique ID of the resource
            types: List of resource types to include in results
            direction: One of 'ancestors', 'descendants', or 'both'

        Returns:
            Dict with 'target', 'ancestors', 'descendants', and 'pagination' keys
        """
        # Validate inputs
        if direction not in LineageDirection:
            raise ValueError(
                f"Invalid direction: {direction}. "
                f"Must be one of: {', '.join(d.value for d in LineageDirection)}"
            )

        if types is not None:
            invalid_types = [t for t in types if t not in LineageResourceType]
            if invalid_types:
                raise ValueError(
                    f"Invalid resource type(s): {invalid_types}. "
                    f"Valid types are: {', '.join(rt.value for rt in LineageResourceType)}"
                )

        # Fetch target node
        target = await self._fetch_node_with_relations(unique_id)
        if not target:
            return {"target": None, "ancestors": [], "descendants": []}

        if direction == LineageDirection.BOTH:
            (ancestors, _), (descendants, _) = await asyncio.gather(
                self._fetch_ancestors_recursive(unique_id, types, target_node=target),
                self._fetch_descendants_recursive(unique_id, types, target_node=target),
            )
        elif direction == LineageDirection.ANCESTORS:
            ancestors, _ = await self._fetch_ancestors_recursive(
                unique_id, types, target_node=target
            )
            descendants = []
        else:  # DESCENDANTS
            descendants, _ = await self._fetch_descendants_recursive(
                unique_id, types, target_node=target
            )
            ancestors = []

        # Apply pagination/truncation (LINEAGE_LIMIT = 50)
        ancestors_total = len(ancestors)
        descendants_total = len(descendants)
        ancestors_truncated = ancestors_total > LINEAGE_LIMIT
        descendants_truncated = descendants_total > LINEAGE_LIMIT

        ancestors = ancestors[:LINEAGE_LIMIT]
        descendants = descendants[:LINEAGE_LIMIT]

        result: dict[str, Any] = {"target": target}

        if direction in [LineageDirection.ANCESTORS, LineageDirection.BOTH]:
            result["ancestors"] = ancestors
        if direction in [LineageDirection.DESCENDANTS, LineageDirection.BOTH]:
            result["descendants"] = descendants

        # Build pagination metadata
        if direction == LineageDirection.BOTH:
            result["pagination"] = {
                "limit": LINEAGE_LIMIT,
                "ancestors_total": ancestors_total,
                "ancestors_truncated": ancestors_truncated,
                "descendants_total": descendants_total,
                "descendants_truncated": descendants_truncated,
            }
        elif direction == LineageDirection.ANCESTORS:
            result["pagination"] = {
                "limit": LINEAGE_LIMIT,
                "ancestors_total": ancestors_total,
                "ancestors_truncated": ancestors_truncated,
            }
        else:  # DESCENDANTS
            result["pagination"] = {
                "limit": LINEAGE_LIMIT,
                "descendants_total": descendants_total,
                "descendants_truncated": descendants_truncated,
            }

        return result
