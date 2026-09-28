"""
Tests for OAuth token models and verification.
"""

from datetime import datetime, tzinfo
from typing import Self

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt import PyJWKClient, api_jwt
from jwt.algorithms import RSAAlgorithm
from pydantic import ValidationError

from dbt_mcp.oauth.token import (
    AccessTokenResponse,
    DecodedAccessToken,
    fetch_jwks_and_verify_token,
)


FROZEN_NOW = 1_700_000_000
DBT_PLATFORM_URL = "https://dbt.example.com"
KEY_ID = "test-key"


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz: tzinfo | None = None) -> Self:
        return cls.fromtimestamp(FROZEN_NOW, tz=tz)


@pytest.fixture
def signing_key(monkeypatch: pytest.MonkeyPatch) -> rsa.RSAPrivateKey:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_jwk = RSAAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
    public_jwk.update({"kid": KEY_ID, "use": "sig"})
    monkeypatch.setattr(
        PyJWKClient, "fetch_data", lambda _client: {"keys": [public_jwk]}
    )
    monkeypatch.setattr(api_jwt, "datetime", FrozenDateTime)
    return private_key


def signed_token(
    private_key: rsa.RSAPrivateKey, *, issued_at: int, expires_at: int
) -> str:
    return jwt.encode(
        {"sub": "test-user", "iat": issued_at, "exp": expires_at},
        private_key,
        algorithm="RS256",
        headers={"kid": KEY_ID},
    )


class TestAccessTokenResponse:
    """Test the AccessTokenResponse model."""

    def test_valid_token_response(self):
        """Test creating a valid access token response."""
        token_data = {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "expires_in": 3600,
            "scope": "read write",
            "token_type": "Bearer",
            "expires_at": 1609459200,  # 2021-01-01 00:00:00 UTC
        }

        token_response = AccessTokenResponse(**token_data)

        assert token_response.access_token == "test_access_token"
        assert token_response.refresh_token == "test_refresh_token"
        assert token_response.expires_in == 3600
        assert token_response.scope == "read write"
        assert token_response.token_type == "Bearer"
        assert token_response.expires_at == 1609459200

    def test_missing_required_field(self):
        """Test that missing required fields raise validation errors."""
        incomplete_data = {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            # Missing expires_in, scope, token_type, expires_at
        }

        with pytest.raises(ValidationError) as exc_info:
            AccessTokenResponse(**incomplete_data)

        error = exc_info.value
        assert len(error.errors()) >= 4  # At least 4 missing fields

        missing_fields = {
            err["loc"][0] for err in error.errors() if err["type"] == "missing"
        }
        expected_missing = {"expires_in", "scope", "token_type", "expires_at"}
        assert expected_missing.issubset(missing_fields)

    def test_invalid_data_types(self):
        """Test that invalid data types raise validation errors."""
        invalid_data = {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "expires_in": "not_an_int",  # Should be int
            "scope": "read write",
            "token_type": "Bearer",
            "expires_at": "not_an_int",  # Should be int
        }

        with pytest.raises(ValidationError) as exc_info:
            AccessTokenResponse(**invalid_data)

        error = exc_info.value
        # Should have validation errors for expires_in and expires_at
        assert len(error.errors()) >= 2

        invalid_fields = {err["loc"][0] for err in error.errors()}
        assert "expires_in" in invalid_fields
        assert "expires_at" in invalid_fields

    def test_model_dict_conversion(self):
        """Test converting model to dict and back."""
        token_data = {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "expires_in": 3600,
            "scope": "read write",
            "token_type": "Bearer",
            "expires_at": 1609459200,
        }

        token_response = AccessTokenResponse(**token_data)
        token_dict = token_response.model_dump()

        # Should be able to recreate from dict
        recreated_token = AccessTokenResponse(**token_dict)
        assert recreated_token == token_response

    def test_model_json_serialization(self):
        """Test JSON serialization and deserialization."""
        token_data = {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "expires_in": 3600,
            "scope": "read write",
            "token_type": "Bearer",
            "expires_at": 1609459200,
        }

        token_response = AccessTokenResponse(**token_data)
        json_str = token_response.model_dump_json()

        # Should be valid JSON that can be parsed back
        import json

        parsed_data = json.loads(json_str)
        recreated_token = AccessTokenResponse(**parsed_data)
        assert recreated_token == token_response


class TestDecodedAccessToken:
    """Test the DecodedAccessToken model."""

    def test_valid_decoded_token(self):
        """Test creating a valid decoded access token."""
        access_token_response = AccessTokenResponse(
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_in=3600,
            scope="read write",
            token_type="Bearer",
            expires_at=1609459200,
        )

        decoded_claims = {
            "sub": "user123",
            "iss": "https://auth.example.com",
            "exp": 1609459200,
            "iat": 1609455600,
            "scope": "read write",
        }

        decoded_token = DecodedAccessToken(
            access_token_response=access_token_response, decoded_claims=decoded_claims
        )

        assert decoded_token.access_token_response == access_token_response
        assert decoded_token.decoded_claims == decoded_claims
        assert decoded_token.decoded_claims["sub"] == "user123"
        assert decoded_token.decoded_claims["scope"] == "read write"

    def test_empty_decoded_claims(self):
        """Test that empty decoded claims are allowed."""
        access_token_response = AccessTokenResponse(
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_in=3600,
            scope="read write",
            token_type="Bearer",
            expires_at=1609459200,
        )

        decoded_token = DecodedAccessToken(
            access_token_response=access_token_response, decoded_claims={}
        )

        assert decoded_token.access_token_response == access_token_response
        assert decoded_token.decoded_claims == {}

    def test_missing_access_token_response(self):
        """Test that missing access_token_response raises validation error."""
        decoded_claims = {"sub": "user123"}

        with pytest.raises(ValidationError) as exc_info:
            DecodedAccessToken(decoded_claims=decoded_claims)

        error = exc_info.value
        assert len(error.errors()) == 1
        assert error.errors()[0]["loc"] == ("access_token_response",)
        assert error.errors()[0]["type"] == "missing"

    def test_invalid_access_token_response_type(self):
        """Test that invalid access_token_response type raises validation error."""
        with pytest.raises(ValidationError) as exc_info:
            DecodedAccessToken(
                access_token_response="not_a_token_response",  # Should be AccessTokenResponse
                decoded_claims={"sub": "user123"},
            )

        error = exc_info.value
        assert len(error.errors()) >= 1
        # Should have validation error for access_token_response field
        assert any(err["loc"] == ("access_token_response",) for err in error.errors())

    def test_complex_decoded_claims(self):
        """Test with complex nested decoded claims."""
        access_token_response = AccessTokenResponse(
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_in=3600,
            scope="read write",
            token_type="Bearer",
            expires_at=1609459200,
        )

        complex_claims = {
            "sub": "user123",
            "roles": ["admin", "user"],
            "permissions": {"read": ["resource1", "resource2"], "write": ["resource1"]},
            "metadata": {
                "created_at": "2021-01-01T00:00:00Z",
                "last_login": "2021-01-01T12:00:00Z",
            },
        }

        decoded_token = DecodedAccessToken(
            access_token_response=access_token_response, decoded_claims=complex_claims
        )

        assert decoded_token.decoded_claims["roles"] == ["admin", "user"]
        assert decoded_token.decoded_claims["permissions"]["read"] == [
            "resource1",
            "resource2",
        ]
        assert (
            decoded_token.decoded_claims["metadata"]["created_at"]
            == "2021-01-01T00:00:00Z"
        )


class TestFetchJwksAndVerifyToken:
    """Test verification against a simulated JWKS and fixed clock."""

    def test_accepts_small_future_iat(self, signing_key: rsa.RSAPrivateKey) -> None:
        access_token = signed_token(
            signing_key, issued_at=FROZEN_NOW + 4, expires_at=FROZEN_NOW + 3600
        )

        claims = fetch_jwks_and_verify_token(access_token, DBT_PLATFORM_URL)

        assert claims["sub"] == "test-user"
        assert claims["iat"] == FROZEN_NOW + 4

    def test_rejects_iat_beyond_leeway(self, signing_key: rsa.RSAPrivateKey) -> None:
        access_token = signed_token(
            signing_key, issued_at=FROZEN_NOW + 11, expires_at=FROZEN_NOW + 3600
        )

        with pytest.raises(jwt.ImmatureSignatureError):
            fetch_jwks_and_verify_token(access_token, DBT_PLATFORM_URL)

    def test_rejects_invalid_signature(self, signing_key: rsa.RSAPrivateKey) -> None:
        other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        access_token = signed_token(
            other_key, issued_at=FROZEN_NOW, expires_at=FROZEN_NOW + 3600
        )

        with pytest.raises(jwt.InvalidSignatureError):
            fetch_jwks_and_verify_token(access_token, DBT_PLATFORM_URL)

    def test_rejects_expiration_beyond_leeway(
        self, signing_key: rsa.RSAPrivateKey
    ) -> None:
        access_token = signed_token(
            signing_key, issued_at=FROZEN_NOW - 3600, expires_at=FROZEN_NOW - 11
        )

        with pytest.raises(jwt.ExpiredSignatureError):
            fetch_jwks_and_verify_token(access_token, DBT_PLATFORM_URL)
