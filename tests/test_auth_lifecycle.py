"""Offline tests for Cognito token lifecycle handling."""

from __future__ import annotations

import asyncio
import importlib
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).parents[1]
PACKAGE_PATH = ROOT / "custom_components" / "whisker_ting"

if "aiohttp" not in sys.modules:
    aiohttp = ModuleType("aiohttp")
    aiohttp.ClientSession = object
    aiohttp.ClientResponse = object
    aiohttp.ClientError = Exception
    sys.modules["aiohttp"] = aiohttp


def _load_api_module() -> ModuleType:
    """Load api.py without importing the Home Assistant integration package."""
    package = sys.modules.get("custom_components.whisker_ting")
    if package is None:
        package = ModuleType("custom_components.whisker_ting")
        package.__path__ = [str(PACKAGE_PATH)]
        sys.modules["custom_components.whisker_ting"] = package
    return importlib.import_module("custom_components.whisker_ting.api")


api = _load_api_module()
auth = importlib.import_module("custom_components.whisker_ting.auth")


class FakeAuth:
    """Controllable Cognito authentication double."""

    def __init__(
        self,
        *,
        authenticate_result: dict[str, Any] | None = None,
        refresh_result: dict[str, Any] | None = None,
        refresh_error: Exception | None = None,
    ) -> None:
        self.authenticate_result = authenticate_result
        self.refresh_result = refresh_result
        self.refresh_error = refresh_error
        self.authenticate_calls = 0

    async def authenticate(self, username: str, password: str) -> dict[str, Any]:
        """Return the configured full-login result."""
        self.authenticate_calls += 1
        assert self.authenticate_result is not None
        return self.authenticate_result

    async def refresh_tokens(self, refresh_token: str) -> dict[str, Any]:
        """Return or raise the configured refresh outcome."""
        if self.refresh_error is not None:
            raise self.refresh_error
        assert self.refresh_result is not None
        return self.refresh_result


def _login_result(expires_in: int) -> dict[str, Any]:
    """Build a normalized full-login response."""
    return {
        "access_token": "access",
        "refresh_token": "refresh-new",
        "id_token": "id",
        "expires_in": expires_in,
        "user_attributes": [
            {"Name": "custom:user_id", "Value": "42"},
            {"Name": "custom:api_key", "Value": "api-key"},
        ],
    }


@pytest.mark.parametrize("expires_in", [600, 1800, 7200])
def test_full_auth_uses_cognito_expiry_and_utc(expires_in: int) -> None:
    """Full login honors varying ExpiresIn values with an aware UTC deadline."""

    async def scenario() -> None:
        client = api.WhiskerApiClient(object(), "user", "password")
        client._auth = FakeAuth(authenticate_result=_login_result(expires_in))
        before = datetime.now(UTC)

        await client._authenticate()

        assert client._token_expiry is not None
        assert client._token_expiry.tzinfo is UTC
        assert before + timedelta(seconds=expires_in) <= client._token_expiry
        assert client._token_expiry <= datetime.now(UTC) + timedelta(
            seconds=expires_in
        )

    asyncio.run(scenario())


def test_refresh_uses_expiry_and_five_minute_buffer() -> None:
    """A token is reused outside the buffer and refreshed inside it."""

    async def scenario() -> None:
        client = api.WhiskerApiClient(
            object(), "user", refresh_token="refresh", user_id=42, api_key="key"
        )
        fake_auth = FakeAuth(
            refresh_result={"AccessToken": "new-access", "ExpiresIn": 900}
        )
        client._auth = fake_auth
        client._access_token = "old-access"
        client._token_expiry = datetime.now(UTC) + timedelta(minutes=6)
        assert await client._ensure_token() == "old-access"

        client._token_expiry = datetime.now(UTC) + timedelta(minutes=5)
        assert await client._ensure_token() == "new-access"
        assert client._token_expiry is not None
        assert client._token_expiry.tzinfo is UTC

    asyncio.run(scenario())


def test_failed_refresh_falls_back_to_password_login() -> None:
    """A refresh error reaches full login when a legacy password is available."""

    async def scenario() -> None:
        client = api.WhiskerApiClient(
            object(), "user", "password", refresh_token="expired"
        )
        fake_auth = FakeAuth(
            authenticate_result=_login_result(1200),
            refresh_error=auth.AuthenticationError("refresh rejected"),
        )
        client._auth = fake_auth

        assert await client._ensure_token() == "access"
        assert fake_auth.authenticate_calls == 1

    asyncio.run(scenario())


def test_failed_refresh_without_password_requires_reauthentication() -> None:
    """Migrated entries fail predictably when their refresh token is rejected."""

    async def scenario() -> None:
        client = api.WhiskerApiClient(object(), "user", refresh_token="expired")
        client._auth = FakeAuth(
            refresh_error=auth.AuthenticationError("refresh rejected")
        )

        with pytest.raises(api.WhiskerAuthError, match="refresh rejected"):
            await client._ensure_token()

    asyncio.run(scenario())


def test_cognito_error_code_rejects_sensitive_response_text() -> None:
    """Only a short structured error type can enter an exception message."""

    class FakeResponse:
        async def json(self, *, content_type: None) -> dict[str, str]:
            return {"__type": "token=secret value from response"}

    assert asyncio.run(auth._cognito_error_code(FakeResponse())) is None
