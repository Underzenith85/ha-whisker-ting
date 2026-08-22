"""Cognito authentication support for the Whisker Ting integration."""

from .client import AuthenticationError, WhiskerAuth, _cognito_error_code
from .srp import CognitoSRP

__all__ = [
    "AuthenticationError",
    "CognitoSRP",
    "WhiskerAuth",
    "_cognito_error_code",
]
