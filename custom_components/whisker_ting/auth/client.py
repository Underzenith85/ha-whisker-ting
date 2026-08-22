"""Validated AWS Cognito network orchestration."""

from __future__ import annotations

import re
from typing import Any

import aiohttp

from ..const import COGNITO_CLIENT_ID, COGNITO_REGION
from .models import AuthenticationSession, RefreshTokens, UserAttribute
from .srp import CognitoSRP

COGNITO_IDP_URL = f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/"


class AuthenticationError(Exception):
    """Authentication error with a bounded, non-sensitive message."""


async def _cognito_error_code(response: aiohttp.ClientResponse) -> str | None:
    """Return Cognito's non-sensitive error code without retaining its body."""
    try:
        payload = await response.json(content_type=None)
    except (aiohttp.ClientError, ValueError):
        return None
    error_type = payload.get("__type") if isinstance(payload, dict) else None
    if not isinstance(error_type, str):
        return None
    error_code = error_type.rsplit("#", maxsplit=1)[-1]
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9.]{0,63}", error_code):
        return None
    return error_code


def _mapping(value: Any, context: str) -> dict[str, Any]:
    """Validate that a Cognito response boundary is an object."""
    if not isinstance(value, dict):
        raise AuthenticationError(f"Invalid Cognito {context} response")
    return value


def _required_string(data: dict[str, Any], key: str, context: str) -> str:
    """Read a required non-empty response string."""
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise AuthenticationError(f"Invalid Cognito {context} response")
    return value


def _authentication_tokens(value: Any, *, refresh: bool) -> RefreshTokens:
    """Validate Cognito's AuthenticationResult without retaining unknown fields."""
    data = _mapping(value, "authentication")
    expires_in = data.get("ExpiresIn")
    if (
        not isinstance(expires_in, int)
        or isinstance(expires_in, bool)
        or expires_in <= 0
    ):
        raise AuthenticationError("Invalid Cognito authentication response")
    result: RefreshTokens = {
        "AccessToken": _required_string(data, "AccessToken", "authentication"),
        "ExpiresIn": expires_in,
    }
    if isinstance(data.get("IdToken"), str) and data["IdToken"]:
        result["IdToken"] = data["IdToken"]
    elif not refresh:
        raise AuthenticationError("Invalid Cognito authentication response")
    if not refresh:
        result["RefreshToken"] = _required_string(
            data, "RefreshToken", "authentication"
        )
    return result


def _user_attributes(value: Any) -> list[UserAttribute]:
    """Validate and copy Cognito user attributes."""
    if not isinstance(value, list):
        raise AuthenticationError("Invalid Cognito user response")
    attributes: list[UserAttribute] = []
    for item in value:
        data = _mapping(item, "user")
        attributes.append(
            {
                "Name": _required_string(data, "Name", "user"),
                "Value": _required_string(data, "Value", "user"),
            }
        )
    return attributes


class WhiskerAuth:
    """Handle Whisker/Cognito authentication flow."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        """Initialize the auth handler."""
        self._session = session

    async def authenticate(self, username: str, password: str) -> AuthenticationSession:
        """Authenticate with username and password, return tokens and user info."""
        srp = CognitoSRP(username, password)

        # Step 1: Initiate auth
        auth_params = srp.get_auth_params()
        init_response = await self._initiate_auth(auth_params)

        if init_response.get("ChallengeName") != "PASSWORD_VERIFIER":
            raise AuthenticationError("Unexpected Cognito authentication challenge")

        # Step 2: Respond to challenge
        challenge_params_raw = _mapping(
            init_response.get("ChallengeParameters"), "challenge"
        )
        challenge_params = {
            key: _required_string(challenge_params_raw, key, "challenge")
            for key in ("USER_ID_FOR_SRP", "SALT", "SRP_B", "SECRET_BLOCK")
        }
        if isinstance(challenge_params_raw.get("USERNAME"), str):
            challenge_params["USERNAME"] = challenge_params_raw["USERNAME"]
        challenge_response = srp.process_challenge(challenge_params, auth_params)

        auth_result = await self._respond_to_challenge(challenge_response)

        tokens = _authentication_tokens(
            auth_result.get("AuthenticationResult"), refresh=False
        )

        # Step 3: Get user attributes
        user_info = await self._get_user(tokens["AccessToken"])

        return {
            "access_token": tokens["AccessToken"],
            "id_token": tokens["IdToken"],
            "refresh_token": tokens["RefreshToken"],
            "expires_in": tokens["ExpiresIn"],
            "user_attributes": user_info,
        }

    async def refresh_tokens(self, refresh_token: str) -> RefreshTokens:
        """Refresh access tokens using refresh token."""
        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
        }

        payload = {
            "AuthFlow": "REFRESH_TOKEN_AUTH",
            "AuthParameters": {
                "REFRESH_TOKEN": refresh_token,
            },
            "ClientId": COGNITO_CLIENT_ID,
        }

        async with self._session.post(
            COGNITO_IDP_URL, json=payload, headers=headers
        ) as resp:
            if resp.status != 200:
                error_code = await _cognito_error_code(resp)
                suffix = f" ({error_code})" if error_code else ""
                raise AuthenticationError(f"Token refresh failed{suffix}")

            result = _mapping(await resp.json(content_type=None), "refresh")
            return _authentication_tokens(
                result.get("AuthenticationResult"), refresh=True
            )

    async def _initiate_auth(self, auth_params: dict[str, str]) -> dict[str, Any]:
        """Initiate SRP authentication."""
        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
        }

        payload = {
            "AuthFlow": "USER_SRP_AUTH",
            "AuthParameters": auth_params,
            "ClientId": COGNITO_CLIENT_ID,
        }

        async with self._session.post(
            COGNITO_IDP_URL, json=payload, headers=headers
        ) as resp:
            if resp.status != 200:
                error_code = await _cognito_error_code(resp)
                if error_code in {"UserNotFoundException", "NotAuthorizedException"}:
                    raise AuthenticationError("Invalid username or password")
                suffix = f" ({error_code})" if error_code else ""
                raise AuthenticationError(f"Auth initiation failed{suffix}")

            return _mapping(await resp.json(content_type=None), "initiation")

    async def _respond_to_challenge(
        self, challenge_response: dict[str, str]
    ) -> dict[str, Any]:
        """Respond to password verifier challenge."""
        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.RespondToAuthChallenge",
        }

        payload = {
            "ChallengeName": "PASSWORD_VERIFIER",
            "ChallengeResponses": challenge_response,
            "ClientId": COGNITO_CLIENT_ID,
        }

        async with self._session.post(
            COGNITO_IDP_URL, json=payload, headers=headers
        ) as resp:
            if resp.status != 200:
                error_code = await _cognito_error_code(resp)
                if error_code == "NotAuthorizedException":
                    raise AuthenticationError("Invalid username or password")
                suffix = f" ({error_code})" if error_code else ""
                raise AuthenticationError(f"Challenge response failed{suffix}")

            return _mapping(await resp.json(content_type=None), "challenge")

    async def _get_user(self, access_token: str) -> list[UserAttribute]:
        """Get user attributes from Cognito."""
        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.GetUser",
        }

        payload = {
            "AccessToken": access_token,
        }

        async with self._session.post(
            COGNITO_IDP_URL, json=payload, headers=headers
        ) as resp:
            if resp.status != 200:
                error_code = await _cognito_error_code(resp)
                suffix = f" ({error_code})" if error_code else ""
                raise AuthenticationError(f"Failed to get user info{suffix}")

            result = _mapping(await resp.json(content_type=None), "user")
            return _user_attributes(result.get("UserAttributes"))
