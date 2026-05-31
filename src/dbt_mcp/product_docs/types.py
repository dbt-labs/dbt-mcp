from dataclasses import dataclass, field


@dataclass
class DocSearchResult:
    title: str
    url: str
    description: str = ""
    section: str = ""


@dataclass
class SearchProductDocsResponse:
    query: str
    total_matches: int
    showing: int
    results: list[DocSearchResult]
    dbt_project_version: str | None = None
    search_method: str | None = None
    error: str | None = None


@dataclass
class ProductDocPageResponse:
    url: str
    content: str
    error: str | None = None
    version_note: str | None = None


@dataclass
class GetProductDocPagesResponse:
    pages: list[ProductDocPageResponse] = field(default_factory=list)
    dbt_project_version: str | None = None
