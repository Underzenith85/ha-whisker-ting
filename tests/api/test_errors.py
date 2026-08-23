"""Offline tests for REST client error classification and retry behavior."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from custom_components.whisker_ting import api
from custom_components.whisker_ting.auth import AuthenticationError


class FakeResponse:
    """Minimal asynchronous HTTP response double."""

    def __init__(
        self,
        status: int,
        payload: Any = None,
        *,
        json_error: Exception | None = None,
    ) -> None:
        self.status = status
        self.payload = payload
        self.json_error = json_error

    async def json(self) -> Any:
        """Return or reject the configured JSON payload."""
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class FakeRequestContext:
    """Minimal aiohttp request context manager double."""

    def __init__(self, outcome: FakeResponse | Exception) -> None:
        self.outcome = outcome

    async def __aenter__(self) -> FakeResponse:
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome

    async def __aexit__(self, *args: Any) -> None:
        return None


class FakeSession:
    """Record requests and return configured outcomes in order."""

    def __init__(self, *outcomes: FakeResponse | Exception) -> None:
        self.outcomes = list(outcomes)
        self.headers: list[dict[str, str]] = []

    def request(self, *args: Any, **kwargs: Any) -> FakeRequestContext:
        """Return the next request context."""
        self.headers.append(kwargs["headers"])
        return FakeRequestContext(self.outcomes.pop(0))


def _client(session: FakeSession) -> api.WhiskerApiClient:
    """Build a client with a valid cached access token."""
    client = api.WhiskerApiClient(session, "user", refresh_token="refresh")
    client._access_token = "access"
    client._token_expiry = datetime.now(UTC) + timedelta(hours=1)
    return client


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (403, api.WhiskerAuthorizationError),
        (404, api.WhiskerNotFoundError),
        (429, api.WhiskerRateLimitError),
        (503, api.WhiskerServiceError),
        (400, api.WhiskerApiError),
    ],
)
def test_request_classifies_http_errors(
    status: int, error_type: type[api.WhiskerApiError]
) -> None:
    """REST status codes map to stable integration exceptions."""

    async def scenario() -> None:
        client = _client(FakeSession(FakeResponse(status)))
        with pytest.raises(error_type):
            await client._request("GET", "/resource")

    asyncio.run(scenario())


def test_request_retries_unauthorized_once_with_new_token() -> None:
    """A rejected access token is renewed and retried exactly once."""

    async def scenario() -> None:
        session = FakeSession(FakeResponse(401), FakeResponse(200, {"ok": True}))
        client = _client(session)
        client._renew_after_unauthorized = AsyncMock(return_value="renewed")

        assert await client._request("GET", "/resource") == {"ok": True}
        client._renew_after_unauthorized.assert_awaited_once_with("access")
        assert [headers["Authorization"] for headers in session.headers] == [
            "Bearer access",
            "Bearer renewed",
        ]

    asyncio.run(scenario())


def test_request_repeated_unauthorized_requires_reauthentication() -> None:
    """A second 401 is surfaced as an authentication failure."""

    async def scenario() -> None:
        session = FakeSession(FakeResponse(401), FakeResponse(401))
        client = _client(session)
        client._renew_after_unauthorized = AsyncMock(return_value="renewed")

        with pytest.raises(api.WhiskerAuthError):
            await client._request("GET", "/resource")

        assert len(session.headers) == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", [TimeoutError(), aiohttp.ClientConnectionError()])
def test_request_classifies_connection_failures(failure: Exception) -> None:
    """Transport and timeout failures become retryable connection errors."""

    async def scenario() -> None:
        client = _client(FakeSession(failure))
        with pytest.raises(api.WhiskerConnectionError):
            await client._request("GET", "/resource")

    asyncio.run(scenario())


def test_request_rejects_malformed_json_without_response_contents() -> None:
    """Malformed success bodies have a bounded, non-sensitive error message."""

    async def scenario() -> None:
        client = _client(
            FakeSession(
                FakeResponse(
                    200,
                    json_error=ValueError("secret response token=do-not-log"),
                )
            )
        )
        with pytest.raises(api.WhiskerInvalidResponseError) as exc_info:
            await client._request("GET", "/resource")

        assert str(exc_info.value) == "API returned an invalid JSON response"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("failure", "returns_none"),
    [
        (api.WhiskerAuthError("expired"), False),
        (api.WhiskerNotFoundError("missing"), True),
        (api.WhiskerServiceError("unavailable"), True),
    ],
)
def test_optional_endpoint_preserves_account_auth_failures(
    failure: api.WhiskerApiError, returns_none: bool
) -> None:
    """Optional feature degradation cannot hide invalid account credentials."""

    async def scenario() -> None:
        client = _client(FakeSession())
        client._request = AsyncMock(side_effect=failure)
        if returns_none:
            assert await client._get_optional_data("/optional") is None
        else:
            with pytest.raises(api.WhiskerAuthError):
                await client._get_optional_data("/optional")

    asyncio.run(scenario())


def test_optional_capability_tracks_only_explicit_authorization_failures() -> None:
    """Unsupported and temporary failures remain distinct from removed access."""

    async def scenario() -> None:
        client = _client(FakeSession())
        client._request = AsyncMock(side_effect=api.WhiskerAuthorizationError("denied"))
        assert (
            await client._get_optional_data("/optional", capability="event_history")
            is None
        )
        assert client.unauthorized_capabilities == {"event_history"}

        client._request = AsyncMock(side_effect=api.WhiskerServiceError("temporary"))
        await client._get_optional_data("/optional", capability="event_history")
        assert client.unauthorized_capabilities == {"event_history"}

        client._request = AsyncMock(side_effect=api.WhiskerNotFoundError("unsupported"))
        await client._get_optional_data("/optional", capability="event_history")
        assert not client.unauthorized_capabilities

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        (api.WhiskerRateLimitError("limited"), "rate limited"),
        (api.WhiskerServiceError("down"), "Ting service unavailable"),
        (api.WhiskerConnectionError("offline"), "connection failed"),
        (api.WhiskerInvalidResponseError("bad body"), "invalid response"),
        (api.WhiskerApiError("other"), "API request failed"),
    ],
)
def test_optional_capability_tracks_temporary_failure_reason(
    failure: api.WhiskerApiError, reason: str
) -> None:
    """Temporary optional failures expose a stable non-sensitive reason."""

    async def scenario() -> None:
        client = _client(FakeSession())
        client._request = AsyncMock(side_effect=failure)

        assert (
            await client._get_optional_data("/optional", capability="conditions")
            is None
        )
        assert client.optional_capability_failures == {"conditions": reason}

    asyncio.run(scenario())


def test_optional_capability_recovery_clears_temporary_failure(caplog: Any) -> None:
    """A successful optional response clears degradation without log spam."""

    async def scenario() -> None:
        client = _client(FakeSession())
        client._request = AsyncMock(side_effect=api.WhiskerServiceError("secret"))

        await client._get_optional_data("/optional", capability="conditions")
        await client._get_optional_data("/optional", capability="conditions")
        assert caplog.text.count("temporarily unavailable") == 1

        client._request = AsyncMock(return_value={"ok": True})
        assert await client._get_optional_data(
            "/optional", capability="conditions"
        ) == {"ok": True}
        assert not client.optional_capability_failures

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "failure_factory",
    [api.WhiskerAuthError, api.WhiskerConnectionError],
)
def test_connection_probe_preserves_actionable_failures(
    failure_factory: Callable[[str], api.WhiskerApiError],
) -> None:
    """Setup can distinguish reauthentication from a retryable outage."""

    async def scenario() -> None:
        client = _client(FakeSession())
        client.get_user_data = AsyncMock(side_effect=failure_factory("failure"))
        with pytest.raises(failure_factory):
            await client.test_connection()

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_client_properties_authentication_and_refresh_failures() -> None:
    """Credential properties and authentication failures are fully bounded."""
    client = api.WhiskerApiClient(
        MagicMock(), "user", refresh_token="refresh", user_id=42, api_key="key"
    )
    assert client.user_id == 42
    assert client.api_key == "key"
    assert client.refresh_token == "refresh"
    assert client.sites == {}

    client._password = None
    with pytest.raises(api.WhiskerAuthError, match="Reauthentication"):
        await client._authenticate()

    client._password = "password"
    client._auth.authenticate = AsyncMock(side_effect=AuthenticationError("bad"))
    with pytest.raises(api.WhiskerAuthError, match="bad"):
        await client._authenticate()

    client._auth.refresh_tokens = AsyncMock(side_effect=AuthenticationError("old"))
    with pytest.raises(api.WhiskerAuthError, match="old"):
        await client._refresh_access_token()


@pytest.mark.asyncio
async def test_token_renewal_covers_concurrent_and_full_auth_paths() -> None:
    """A concurrent token, refresh fallback, and absent result are handled."""
    client = api.WhiskerApiClient(MagicMock(), "user", "password")
    client._access_token = "newer"
    assert await client._renew_after_unauthorized("rejected") == "newer"

    client._access_token = "rejected"
    client._refresh_token = "refresh"
    client._refresh_access_token = AsyncMock(side_effect=api.WhiskerAuthError("old"))

    async def authenticate() -> None:
        client._access_token = "full-auth"

    client._authenticate = AsyncMock(side_effect=authenticate)
    assert await client._renew_after_unauthorized("rejected") == "full-auth"

    client._refresh_token = None
    client._access_token = None
    client._authenticate = AsyncMock()
    with pytest.raises(api.WhiskerAuthError, match="Authentication failed"):
        await client._renew_after_unauthorized("rejected")


@pytest.mark.asyncio
async def test_optional_success_and_getters_authenticate_when_identity_missing() -> (
    None
):
    """Optional capability recovery and identity-dependent getters renew first."""
    client = api.WhiskerApiClient(MagicMock(), "user")
    client._unauthorized_capabilities.add("conditions")
    client._request = AsyncMock(return_value={})
    assert await client._get_optional_data("/optional", capability="conditions") == {}
    assert not client.unauthorized_capabilities

    client._user_id = None

    async def ensure() -> str:
        client._user_id = 42
        return "token"

    client._ensure_token = AsyncMock(side_effect=ensure)
    client._request = AsyncMock(return_value={"id": 42})
    assert (await client.get_user_data()).user_id == 42

    client._user_id = None
    client._get_optional_data = AsyncMock(return_value={})
    await client.get_user_conditions()
    client._ensure_token.assert_awaited()

    client._user_id = None
    client._get_optional_data = AsyncMock(return_value=[])
    assert await client.get_event_history() == []


@pytest.mark.asyncio
async def test_all_device_states_without_conditions_and_probe_false() -> None:
    """Missing optional conditions retain devices and generic API errors probe false."""
    device = api.DeviceState("SERIAL", "Device", "Type", 1)
    user = api.UserData(42, "", "", "", devices=[device], sites=[])
    client = api.WhiskerApiClient(MagicMock(), "user")
    client.get_user_data = AsyncMock(return_value=user)
    client.get_user_conditions = AsyncMock(return_value=None)
    assert await client.get_all_device_states() == {"SERIAL": device}

    client.get_user_data = AsyncMock(side_effect=api.WhiskerApiError("bad"))
    assert not await client.test_connection()
