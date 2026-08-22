"""Validated response types for Cognito authentication."""

from typing import NotRequired, TypedDict


class UserAttribute(TypedDict):
    """One validated Cognito user attribute."""

    Name: str
    Value: str


class AuthenticationSession(TypedDict):
    """Normalized full-authentication result consumed by the REST client."""

    access_token: str
    id_token: str
    refresh_token: str
    expires_in: int
    user_attributes: list[UserAttribute]


class RefreshTokens(TypedDict):
    """Validated Cognito refresh result."""

    AccessToken: str
    ExpiresIn: int
    IdToken: NotRequired[str]
    RefreshToken: NotRequired[str]
