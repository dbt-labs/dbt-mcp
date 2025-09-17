from abc import ABC, abstractmethod
from typing import Protocol

from dbt_mcp.oauth.token_provider import TokenProvider


class HeadersProvider(Protocol):
    def get_headers(self) -> dict[str, str]: ...


class TokenHeadersProvider(ABC):
    def __init__(self, token_provider: TokenProvider):
        self.token_provider = token_provider

    @abstractmethod
    def headers_from_token(self, token: str) -> dict[str, str]: ...

    def get_headers(self) -> dict[str, str]:
        return self.headers_from_token(self.token_provider.get_token())


class AdminApiHeadersProvider(TokenHeadersProvider):
    def headers_from_token(self, token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}


class DiscoveryHeadersProvider(TokenHeadersProvider):
    def headers_from_token(self, token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }


class SemanticLayerHeadersProvider(TokenHeadersProvider):
    def headers_from_token(self, token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "x-dbt-partner-source": "dbt-mcp",
        }


class SqlHeadersProvider(TokenHeadersProvider):
    def headers_from_token(self, token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}
