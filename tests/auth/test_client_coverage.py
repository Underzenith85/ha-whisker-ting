"""Complete boundary coverage for Cognito network orchestration."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from custom_components.whisker_ting.auth.client import (
    AuthenticationError,
    WhiskerAuth,
    _authentication_tokens,
    _cognito_error_code,
    _mapping,
    _required_string,
    _user_attributes,
)


class Response:
    """Minimal asynchronous aiohttp response context manager."""

    def __init__(self, status: int, payload: object) -> None:
        self.status = status
        self._payload = payload

    async def __aenter__(self) -> Response:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def json(self, **kwargs: object) -> object:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def session_with(*responses: Response) -> MagicMock:
    """Return a session yielding responses in order."""
    session = MagicMock()
    session.post.side_effect = responses
    return session


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"__type": "prefix#NotAuthorizedException"}, "NotAuthorizedException"),
        ({"__type": "bad value!"}, None),
        ({"other": "value"}, None),
        ([], None),
        (ValueError("bad json"), None),
        (aiohttp.ClientError(), None),
    ],
)
async def test_cognito_error_codes_are_bounded(
    payload: object, expected: str | None
) -> None:
    """Only safe Cognito error identifiers escape the response boundary."""
    assert await _cognito_error_code(Response(400, payload)) == expected


def test_auth_response_validators_cover_success_and_failure() -> None:
    """Cognito boundary helpers reject malformed values and retain known fields."""
    assert _mapping({"ok": True}, "test") == {"ok": True}
    with pytest.raises(AuthenticationError):
        _mapping([], "test")
    assert _required_string({"key": "value"}, "key", "test") == "value"
    for value in (None, ""):
        with pytest.raises(AuthenticationError):
            _required_string({"key": value}, "key", "test")

    full = _authentication_tokens(
        {
            "AccessToken": "access",
            "IdToken": "id",
            "RefreshToken": "refresh",
            "ExpiresIn": 600,
            "ignored": "secret",
        },
        refresh=False,
    )
    assert full == {
        "AccessToken": "access",
        "IdToken": "id",
        "RefreshToken": "refresh",
        "ExpiresIn": 600,
    }
    assert _authentication_tokens(
        {"AccessToken": "access", "ExpiresIn": 600}, refresh=True
    ) == {"AccessToken": "access", "ExpiresIn": 600}
    for invalid in (None, 0, -1, True, "600"):
        with pytest.raises(AuthenticationError):
            _authentication_tokens(
                {"AccessToken": "access", "ExpiresIn": invalid}, refresh=True
            )
    with pytest.raises(AuthenticationError):
        _authentication_tokens(
            {"AccessToken": "access", "RefreshToken": "refresh", "ExpiresIn": 1},
            refresh=False,
        )
    assert _user_attributes([{"Name": "email", "Value": "x@example.invalid"}]) == [
        {"Name": "email", "Value": "x@example.invalid"}
    ]
    with pytest.raises(AuthenticationError):
        _user_attributes({})


@pytest.mark.asyncio
async def test_authenticate_success_and_unexpected_challenge() -> None:
    """The SRP flow validates its challenge and returns a bounded session."""
    auth = WhiskerAuth(MagicMock())
    auth._initiate_auth = AsyncMock(
        return_value={
            "ChallengeName": "PASSWORD_VERIFIER",
            "ChallengeParameters": {
                "USER_ID_FOR_SRP": "user",
                "SALT": "salt",
                "SRP_B": "server",
                "SECRET_BLOCK": "block",
                "USERNAME": "canonical",
            },
        }
    )
    auth._respond_to_challenge = AsyncMock(
        return_value={
            "AuthenticationResult": {
                "AccessToken": "access",
                "IdToken": "id",
                "RefreshToken": "refresh",
                "ExpiresIn": 600,
            }
        }
    )
    auth._get_user = AsyncMock(
        return_value=[{"Name": "email", "Value": "x@example.invalid"}]
    )
    srp = MagicMock()
    srp.get_auth_params.return_value = {"USERNAME": "user", "SRP_A": "a"}
    srp.process_challenge.return_value = {"ANSWER": "proof"}
    with patch(
        "custom_components.whisker_ting.auth.client.CognitoSRP", return_value=srp
    ):
        result = await auth.authenticate("user", "password")
    assert result["access_token"] == "access"
    assert result["user_attributes"][0]["Name"] == "email"
    srp.process_challenge.assert_called_once()

    auth._initiate_auth.return_value = {"ChallengeName": "MFA"}
    with (
        patch(
            "custom_components.whisker_ting.auth.client.CognitoSRP", return_value=srp
        ),
        pytest.raises(AuthenticationError, match="Unexpected"),
    ):
        await auth.authenticate("user", "password")


@pytest.mark.asyncio
async def test_cognito_http_methods_cover_success_and_errors() -> None:
    """Every Cognito request maps success and sanitized HTTP failures."""
    auth = WhiskerAuth(
        session_with(
            Response(
                200,
                {"AuthenticationResult": {"AccessToken": "a", "ExpiresIn": 60}},
            ),
            Response(200, {"ChallengeName": "PASSWORD_VERIFIER"}),
            Response(200, {"AuthenticationResult": {}}),
            Response(200, {"UserAttributes": [{"Name": "sub", "Value": "42"}]}),
        )
    )
    assert (await auth.refresh_tokens("refresh"))["AccessToken"] == "a"
    assert (await auth._initiate_auth({"USERNAME": "u"}))["ChallengeName"]
    assert "AuthenticationResult" in await auth._respond_to_challenge({"A": "b"})
    assert await auth._get_user("access") == [{"Name": "sub", "Value": "42"}]

    cases = [
        ("refresh_tokens", ("r",), "Token refresh failed", "OtherException"),
        (
            "_initiate_auth",
            ({},),
            "Invalid username or password",
            "UserNotFoundException",
        ),
        ("_initiate_auth", ({},), "Auth initiation failed", "OtherException"),
        (
            "_respond_to_challenge",
            ({},),
            "Invalid username or password",
            "NotAuthorizedException",
        ),
        ("_respond_to_challenge", ({},), "Challenge response failed", "OtherException"),
        ("_get_user", ("a",), "Failed to get user info", "OtherException"),
    ]
    for method, args, message, code in cases:
        failing = WhiskerAuth(session_with(Response(400, {"__type": code})))
        with pytest.raises(AuthenticationError, match=message):
            await getattr(failing, method)(*args)
